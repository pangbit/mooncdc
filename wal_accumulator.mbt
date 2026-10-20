///|
priv struct WalAccumulator {
  mut header : WalBatch?
  mut changes : Array[Change]
  mut ids : Array[String?]
  mut bytes : UInt64
  mut started : @bench.Timestamp?
}

///|
fn WalAccumulator::new() -> WalAccumulator {
  { header: None, changes: [], ids: [], bytes: 0, started: None, }
}

///|
fn WalAccumulator::push(self : WalAccumulator, batch : WalBatch) -> Unit raise {
  if self.header is Some(header) && header.id != batch.id {
    raise Protocol("WAL fragment crossed a transaction boundary")
  }
  self.header = Some(batch)
  for index, change in batch.changes {
    if self.started is None {
      self.started = Some(@bench.monotonic_clock_start())
    }
    self.changes.push(change)
    self.ids.push(batch.event_id(index))
  }
  if !batch.changes.is_empty() {
    self.bytes = batch_add(
      self.bytes,
      transaction_size_hint(batch.internal_events()),
    )
  }
}

///|
fn WalAccumulator::take(self : WalAccumulator) -> WalBatch? {
  guard self.header is Some(header) else { return None }
  if self.changes.is_empty() && header.end_lsn is None {
    return None
  }
  let batch = {
    ..header,
    changes: ReadOnlyArray::from_array(self.changes),
    ids: ReadOnlyArray::from_array(self.ids),
    decoded_schemas: None,
  }
  self.changes = []
  self.ids = []
  self.bytes = 0
  self.started = None
  self.header = if header.end_lsn is Some(_) {
    None
  } else {
    Some({ ..header, changes: [], ids: [], decoded_schemas: None, })
  }
  Some(batch)
}

///|
fn WalAccumulator::remaining(
  self : WalAccumulator,
  batch : BatchRuntime,
) -> Int {
  self.started.map_or(batch.max_fill_ms, started => {
    (batch.max_fill_ms.to_double() -
    @bench.monotonic_clock_end(started) / 1000.0)
    .max(0.0)
    .to_int()
  })
}

///|
async fn Subscription::apply_events(
  self : Subscription,
  destination : &Destination,
  control~ : ApplyControl,
  batch~ : BatchRuntime,
  on_checkpoint? : async () -> Unit,
  on_commit? : async (WalBatch) -> Bool,
) -> Unit {
  let accumulator = WalAccumulator::new()
  let mut owned = false
  let mut finished = false
  let labels = "worker_type=\"" +
    self.activity.map_or("apply", activity => activity.worker_type) +
    "\",replication_path=\"cdc\""
  let drain = async fn() -> Unit {
    self.activity_busy = true
    if self.activity is Some(activity) {
      activity.destination_busy(true)
    }
    defer {
      self.activity_busy = false
      if self.activity is Some(activity) {
        activity.destination_busy(false)
        activity.ping()
      }
    }
    if accumulator.take() is Some(fragment) {
      let started = @bench.monotonic_clock_start()
      let status = destination.write_wal_batch(fragment)
      owned = status == Accepted
      control.metrics_data.increment(
        "mooncdc_wal_batches_written_total{" + labels + "}",
      )
      control.metrics_data.increment(
        "mooncdc_changes_written_total{" + labels + "}",
        amount=fragment.changes.length().to_uint64(),
      )
      let status_label = if owned { "accepted" } else { "durable" }
      control.metrics_data.duration(
        "mooncdc_destination_write_seconds{" +
        labels +
        ",status=\"" +
        status_label +
        "\"}",
        started,
      )
      if fragment.end_lsn is Some(_) {
        control.metrics_data.increment(
          "mooncdc_transactions_written_total{" + labels + "}",
        )
        if owned {
          destination.flush()
          owned = false
        }
        match on_commit {
          Some(run) => finished = run(fragment)
          None => {
            self.ack(fragment.internal_events())
            if on_checkpoint is Some(run) {
              run()
            }
          }
        }
      }
    }
    // Includes writes from a previous fragment; pressure, timeout and stop must
    // drain their ownership even when the source has not sent Commit yet.
    if owned {
      destination.flush()
      owned = false
    }
  }
  while !control.stopped && !finished {
    let waits : Array[async () -> WalBatch?] = [
      () => Some(self.next_wal_event()),
      () => {
        control.wait()
        None
      },
    ]
    if !accumulator.changes.is_empty() {
      waits.push(() => {
        @async.sleep(accumulator.remaining(batch))
        None
      })
      if self.memory is Some(memory) {
        waits.insert(0, () => {
          memory.wait_for_pressure()
          None
        })
      }
    }
    let force = !accumulator.changes.is_empty() &&
      (
        accumulator.remaining(batch) == 0 ||
        self.memory.map_or(false, memory => memory.is_paused())
      )
    let next = if force { None } else { @async.any(waits, allow_failure=false) }
    match next {
      Some(event) => {
        accumulator.push(event)
        if event.end_lsn is Some(_) ||
          accumulator.bytes >= batch.governor.target_bytes() ||
          batch.max_fill_ms == 0 {
          drain()
        }
      }
      None => drain()
    }
  }
  drain()
}
