///|
/// Durable covers this write and all earlier Accepted writes on this destination.
/// Accepted transfers ownership to the destination but never authorizes an ack.
pub(all) enum WriteStatus {
  Accepted
  Durable
} derive(Eq, Debug)

///|
/// Identity within a fresh snapshot attempt; retries must use the same batch ID.
/// A new snapshot generates a new attempt and resets each destination table first.
pub struct TableCopyBatchId {
  attempt : String
  relation_id : UInt
  sequence : UInt64
} derive(Eq, Debug, ToJson)

///|
/// Ordered destination contract. Implementations must tolerate replay and make
/// flush a cumulative durability barrier. Owned tasks require cancellation cleanup.
pub(open) trait Destination {
  async fn startup(Self) -> Unit = _
  async fn shutdown(Self) -> Unit = _
  // Fence all earlier copy writes before clearing table data and replay markers.
  async fn reset_table(Self, Relation, attempt~ : String) -> Unit
  // Empty rows carry no ID and let the destination create an empty table.
  async fn write_rows(
    Self,
    Relation,
    ReadOnlyArray[ReadOnlyArray[Value]],
    TableCopyBatchId?,
  ) -> WriteStatus
  async fn write_transaction(Self, Transaction) -> WriteStatus
  async fn flush(Self) -> Unit
}

///|
impl Destination with fn startup(_self) {
  ()
}

///|
impl Destination with fn shutdown(_self) {
  ()
}

///|
/// Run startup, then user work, then shutdown on success. On failure, propagate
/// immediately; destination owners must release/cancel their resources themselves.
pub async fn[T] with_destination(
  destination : &Destination,
  run : async () -> T,
) -> T {
  destination.startup()
  let result = run()
  destination.shutdown()
  result
}

///|
/// Copy a new snapshot through a destination, fencing prior copy attempts and
/// requiring a durable barrier before returning the WAL handoff position.
pub async fn copy_snapshot_to(
  connection : ConnectionConfig,
  destination : &Destination,
  slot~ : String,
  publication~ : String,
  batch_rows? : Int = 256,
  max_frame_bytes? : Int = 16777216,
  max_batch_bytes? : Int = 67108864,
) -> SnapshotPosition {
  copy_snapshot_to_impl(
    connection,
    destination,
    slot~,
    publication~,
    batch_rows~,
    max_frame_bytes~,
    max_batch_bytes~,
    temporary=false,
    table=None,
  )
}

///|
async fn copy_snapshot_to_impl(
  connection : ConnectionConfig,
  destination : &Destination,
  slot~ : String,
  publication~ : String,
  batch_rows~ : Int,
  max_frame_bytes~ : Int,
  max_batch_bytes~ : Int,
  temporary~ : Bool,
  table~ : UInt?,
  schema_store? : &SchemaStore,
  after_copy? : async (Connection, SnapshotPosition) -> Unit,
) -> SnapshotPosition {
  guard @env.rand(24) is Some(random) else {
    raise Authentication("secure entropy unavailable")
  }
  let attempt = @base64.encode(random)
  let mut sequence = 0UL
  let mut accepted = false
  let point = copy_snapshot_impl(
    connection,
    slot~,
    publication~,
    batch_rows~,
    max_frame_bytes~,
    max_batch_bytes~,
    temporary~,
    table~,
    schema_store?,
    after_copy?,
    event => {
      match event {
        TableBegin(relation) => {
          destination.reset_table(relation, attempt~)
          accepted = destination.write_rows(relation, [], None) == Accepted
        }
        TableRows(relation, rows) => {
          if sequence == 0xffffffffffffffffUL {
            raise LimitExceeded("snapshot batch sequence")
          }
          sequence += 1
          let id : TableCopyBatchId = {
            attempt,
            relation_id: relation.oid,
            sequence,
          }
          accepted = destination.write_rows(relation, rows, Some(id)) ==
            Accepted
        }
        TableEnd(_) =>
          if accepted {
            destination.flush()
            accepted = false
          }
      }
    },
  )
  point
}

///|
/// One-shot graceful stop signal. Stopping drains accepted destination writes;
/// cancellation/failure instead leaves them unacknowledged for replay.
pub struct ApplyControl {
  priv mut stopped : Bool
  priv changed : @async.CondVar
  priv mut table_retry : (async (UInt) -> Bool)?
}

///|
pub fn ApplyControl::ApplyControl() -> ApplyControl {
  { stopped: false, changed: @async.CondVar::Cond(), table_retry: None, }
}

///|
pub fn ApplyControl::stop(self : ApplyControl) -> Unit {
  self.stopped = true
  self.changed.broadcast()
}

///|
async fn ApplyControl::wait(self : ApplyControl) -> Unit {
  while !self.stopped {
    self.changed.wait()
  }
}

///|
async fn Subscription::ack_applied(
  self : Subscription,
  pending : Array[Transaction],
) -> Unit {
  // Mark later transactions first so the earliest ack saves the whole prefix
  // with one fsync sequence. Any failure leaves the checkpoint conservative.
  for tx in pending.rev_iter() {
    self.ack(tx)
  }
  pending.clear()
}

///|
/// Consume complete transactions in order. Call inside subscribe/with_destination;
/// do not call next or ack concurrently. Stop drains accepted work before returning.
/// Idle flushes prevent Accepted writes waiting indefinitely for another transaction.
pub async fn Subscription::apply_to(
  self : Subscription,
  destination : &Destination,
  control~ : ApplyControl,
  flush_every? : Int = 8,
  flush_interval_ms? : Int = 1000,
) -> Unit {
  if flush_every <= 0 ||
    flush_every > self.max_pending ||
    flush_interval_ms <= 0 {
    raise InvalidConfig("invalid destination flush limit or interval")
  }
  let pending : Array[Transaction] = []
  while !control.stopped {
    let waits : Array[async () -> Transaction?] = [
      () => Some(self.next()),
      () => {
        control.wait()
        None
      },
    ]
    if !pending.is_empty() {
      waits.push(() => {
        @async.sleep(flush_interval_ms)
        None
      })
    }
    match @async.any(waits, allow_failure=false) {
      Some(tx) => {
        let status = destination.write_transaction(tx)
        pending.push(tx)
        if status == Durable {
          self.ack_applied(pending)
        } else if pending.length() >= flush_every {
          destination.flush()
          self.ack_applied(pending)
        }
      }
      None =>
        if !pending.is_empty() {
          destination.flush()
          self.ack_applied(pending)
        }
    }
  }
  if !pending.is_empty() {
    destination.flush()
    self.ack_applied(pending)
  }
}
