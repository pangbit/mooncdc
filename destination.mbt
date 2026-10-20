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
  // Opt in only when partial WAL writes are replay-safe and cumulatively ordered.
  fn supports_wal_batches(Self) -> Bool = _
  async fn write_wal_batch(Self, WalBatch) -> WriteStatus = _
  async fn bind_state(Self, StateSession) -> Unit = _
  // Called once at the pipeline StateSession boundary, after source workers join
  // and before releasing the lease. Scoped task owners can finish cancellation
  // here even when startup/apply fails. Defaults to no owned state resources.
  async fn release_state(Self) -> Unit = _
  // Exact schema from the imported snapshot, before reset_table. A global
  // SchemaStore lookup can already contain newer DDL from concurrent apply.
  async fn bind_snapshot_schema(Self, Relation, SchemaSnapshot) -> Unit = _
  async fn startup(Self) -> Unit = _
  async fn shutdown(Self) -> Unit = _
  // Earliest schema still needed by durable target metadata, including both
  // endpoints of pending DDL. None disables automatic pruning for this table.
  async fn schema_retention(Self, UInt) -> SchemaId? = _
  // Fence all earlier copy writes before clearing table data and replay markers.
  async fn reset_table(Self, Relation, attempt~ : String) -> Unit
  // Empty rows carry no ID and let the destination create an empty table.
  async fn write_rows(
    Self,
    Relation,
    ReadOnlyArray[ReadOnlyArray[Value]],
    TableCopyBatchId?,
  ) -> WriteStatus
  // Called at the logical TableEnd, including empty tables and writes already
  // drained by a pressure flush. Complete per-table COPY lifetime barriers here;
  // flush remains the cumulative durability operation and may run mid-COPY.
  // Failure/cancellation must prevent publishing the snapshot handoff position.
  async fn finish_table_copy(Self, Relation) -> Unit = _
  async fn write_transaction(Self, Transaction) -> WriteStatus
  async fn flush(Self) -> Unit
}

///|
impl Destination with fn supports_wal_batches(_self) {
  false
}

///|
impl Destination with fn write_wal_batch(_self, _batch) {
  raise InvalidConfig("destination does not support WAL event batches")
}

///|
impl Destination with fn bind_state(_self, _session) {
  ()
}

///|
impl Destination with fn release_state(_self) {
  ()
}

///|
impl Destination with fn bind_snapshot_schema(_self, _relation, _schema) {
  ()
}

///|
// Existing destinations have no separate COPY lifetime barrier. Preserve their
// current flush contract while allowing a destination to require explicit end.
impl Destination with fn finish_table_copy(_self, _relation) {
  ()
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
impl Destination with fn schema_retention(_self, _oid) {
  None
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
  copy_connections? : Int = 1,
  max_frame_bytes? : Int = 16777216,
  max_batch_bytes? : Int = 67108864,
  schema_store? : &SchemaStore,
  slot_failover? : Bool = false,
) -> SnapshotPosition {
  copy_snapshot_to_impl(
    connection,
    destination,
    slot~,
    publication~,
    batch_rows~,
    copy_connections~,
    max_frame_bytes~,
    max_batch_bytes~,
    temporary=false,
    table=None,
    schema_store?,
    slot_failover~,
  )
}

///|
async fn copy_snapshot_to_impl(
  connection : ConnectionConfig,
  destination : &Destination,
  slot~ : String,
  publication~ : String,
  batch_rows~ : Int,
  copy_connections? : Int = 1,
  max_frame_bytes~ : Int,
  max_batch_bytes~ : Int,
  temporary~ : Bool,
  table~ : UInt?,
  schema_store? : &SchemaStore,
  after_copy? : async (Connection, SnapshotPosition) -> Unit,
  on_snapshot? : (SnapshotPosition) -> Unit,
  before_snapshot_schema? : async (SnapshotPosition) -> Unit,
  activity_control? : ApplyControl,
  copy_rows? : Bool = true,
  slot_failover? : Bool = false,
  worker? : SessionWorker,
  memory? : @memory.Monitor,
  batch? : BatchRuntime,
) -> SnapshotPosition {
  guard @env.rand(24) is Some(random) else {
    raise Authentication("secure entropy unavailable")
  }
  let attempt = @base64.encode(random)
  let mut sequence = 0UL
  let mut accepted = false
  let metrics = activity_control.map(control => control.metrics_data)
  let mut copied_rows = 0UL
  let mut copy_started = @bench.monotonic_clock_start()
  let mutex = @async.Mutex()
  let point = copy_snapshot_impl(
    connection,
    slot~,
    publication~,
    batch_rows~,
    copy_connections~,
    max_frame_bytes~,
    max_batch_bytes~,
    temporary~,
    table~,
    schema_store?,
    activity_control?,
    copy_rows~,
    slot_failover~,
    after_copy?,
    on_snapshot?,
    before_snapshot_schema?,
    worker?,
    batch?,
    before_read=activity => {
      if memory is Some(monitor) && monitor.is_paused() {
        // Accepted rows must drain before waiting for memory that they may own.
        {
          mutex.acquire()
          defer mutex.release()
          if accepted {
            destination.flush()
            accepted = false
          }
        }
        // Release the write lock before waiting: another copy reader may own
        // a batch that must flush to relieve this pressure.
        wait_source_capacity(memory, activity)
      }
    },
    on_schema=(relation, schema) => {
      destination.bind_snapshot_schema(relation, schema)
    },
    event => {
      mutex.acquire()
      defer mutex.release()
      match event {
        TableBegin(relation) => {
          copied_rows = 0UL
          copy_started = @bench.monotonic_clock_start()
          destination.reset_table(relation, attempt~)
          accepted = destination.write_rows(relation, [], None) == Accepted
          if accepted && batch is Some(_) {
            // The empty-table barrier must not remain owned when a later
            // pressure signal pauses the streaming FETCH between frames.
            destination.flush()
            accepted = false
          }
        }
        TableRows(relation, rows) => {
          if metrics is Some(metrics) {
            metrics.increment(
              "mooncdc_copy_rows_received_total",
              amount=rows.length().to_uint64(),
            )
          }
          if sequence == 0xffffffffffffffffUL {
            raise LimitExceeded("snapshot batch sequence")
          }
          sequence += 1
          let id : TableCopyBatchId = {
            attempt,
            relation_id: relation.oid,
            sequence,
          }
          let write_started = @bench.monotonic_clock_start()
          let status = destination.write_rows(relation, rows, Some(id))
          accepted = status == Accepted
          copied_rows = batch_add(copied_rows, rows.length().to_uint64())
          if metrics is Some(metrics) {
            metrics.increment(
              "mooncdc_copy_rows_written_total",
              amount=rows.length().to_uint64(),
            )
            let status = if accepted { "accepted" } else { "durable" }
            metrics.duration(
              "mooncdc_destination_write_seconds{worker_type=\"table_sync\",replication_path=\"copy\",status=\"\{status}\"}",
              write_started,
            )
          }
          if accepted && batch is Some(_) {
            destination.flush()
            accepted = false
          }
        }
        TableEnd(relation) => {
          if accepted {
            destination.flush()
            accepted = false
          }
          destination.finish_table_copy(relation)
          if metrics is Some(metrics) {
            metrics.increment(
              "mooncdc_copy_rows_completed_total",
              amount=copied_rows,
            )
            metrics.increment("mooncdc_table_copies_completed_total")
            metrics.duration("mooncdc_table_copy_seconds", copy_started)
          }
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
  priv mut lifecycle : PipelineLifecycle
  priv mut pipeline_active : Bool
  priv mut activity_sequence : UInt64
  priv activity_registry : Map[UInt64, ActivityState]
  priv metrics_data : RuntimeMetrics
}

///|
pub fn ApplyControl::ApplyControl() -> ApplyControl {
  {
    stopped: false,
    changed: @async.CondVar::Cond(),
    table_retry: None,
    lifecycle: Initializing,
    pipeline_active: false,
    activity_sequence: 0,
    activity_registry: Map([]),
    metrics_data: RuntimeMetrics::new(),
  }
}

///|
pub fn ApplyControl::stop(self : ApplyControl) -> Unit {
  self.stopped = true
  self.lifecycle = Stopping
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
  on_checkpoint? : async () -> Unit,
) -> Unit {
  // Mark later transactions first so the earliest ack saves the whole prefix
  // with one fsync sequence. Any failure leaves the checkpoint conservative.
  for tx in pending.rev_iter() {
    self.ack(tx)
  }
  pending.clear()
  if on_checkpoint is Some(run) {
    run()
  }
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
  self.apply_to_impl(destination, control~, flush_every~, flush_interval_ms~)
}

///|
async fn Subscription::apply_to_impl(
  self : Subscription,
  destination : &Destination,
  control~ : ApplyControl,
  flush_every? : Int = 8,
  flush_interval_ms? : Int = 1000,
  on_checkpoint? : async () -> Unit,
  batch? : BatchRuntime,
) -> Unit {
  if flush_every <= 0 ||
    flush_every > self.max_pending ||
    flush_interval_ms <= 0 {
    raise InvalidConfig("invalid destination flush limit or interval")
  }
  let pending : Array[Transaction] = []
  let labels = "worker_type=\"" +
    self.activity.map_or("apply", activity => activity.worker_type) +
    "\",replication_path=\"cdc\""
  let mut pending_bytes = 0UL
  let mut started : @bench.Timestamp? = None
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
        let wait = match (batch, started) {
          (Some(batch), Some(started)) =>
            (batch.max_fill_ms.to_double() -
            @bench.monotonic_clock_end(started) / 1000.0)
            .max(0.0)
            .to_int()
          _ => flush_interval_ms
        }
        @async.sleep(wait)
        None
      })
      if self.memory is Some(memory) {
        waits.insert(0, () => {
          memory.wait_for_pressure()
          None
        })
      }
    }
    let expired = match (batch, started) {
      (Some(batch), Some(started)) =>
        @bench.monotonic_clock_end(started) / 1000.0 >=
        batch.max_fill_ms.to_double()
      _ => false
    }
    let next = if !pending.is_empty() &&
      (expired || self.memory.map_or(false, memory => memory.is_paused())) {
      // Pressure/deadline wins even when another retained transaction is ready.
      None
    } else {
      @async.any(waits, allow_failure=false)
    }
    self.activity_busy = true
    if self.activity is Some(activity) {
      activity.destination_busy(true)
    }
    defer {
      self.activity_busy = false
      if self.activity is Some(activity) {
        activity.destination_busy(false)
      }
    }
    match next {
      Some(tx) => {
        if pending.is_empty() {
          started = Some(@bench.monotonic_clock_start())
        }
        let write_started = @bench.monotonic_clock_start()
        let status = destination.write_transaction(tx)
        let write_status = if status == Durable {
          "durable"
        } else {
          "accepted"
        }
        control.metrics_data.increment(
          "mooncdc_transactions_written_total{" + labels + "}",
        )
        control.metrics_data.increment(
          "mooncdc_changes_written_total{" + labels + "}",
          amount=tx.changes.length().to_uint64(),
        )
        control.metrics_data.duration(
          "mooncdc_destination_write_seconds{" +
          labels +
          ",status=\"\{write_status}\"}",
          write_started,
        )
        pending.push(tx)
        if batch is Some(_) {
          pending_bytes = batch_add(pending_bytes, transaction_size_hint(tx))
        }
        if status == Durable {
          self.ack_applied(pending, on_checkpoint?)
        } else if pending.length() >= flush_every ||
          batch.map_or(false, batch => {
            pending_bytes >= batch.governor.target_bytes() ||
            batch.max_fill_ms == 0
          }) {
          destination.flush()
          self.ack_applied(pending, on_checkpoint?)
        }
      }
      None =>
        if !pending.is_empty() {
          destination.flush()
          self.ack_applied(pending, on_checkpoint?)
        }
    }
    if pending.is_empty() {
      started = None
      pending_bytes = 0UL
    }
    if self.activity is Some(activity) {
      activity.ping()
    }
  }
  if !pending.is_empty() {
    destination.flush()
    self.ack_applied(pending, on_checkpoint?)
  }
}
