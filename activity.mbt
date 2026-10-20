///|
pub(all) enum PipelineLifecycle {
  Initializing
  Running
  Stopping
} derive(Eq, Debug)

///|
pub(all) enum ActivityKind {
  InitialTableCopy
  WalApply(wal_sender_timeout_ms~ : UInt64)
} derive(Eq, Debug)

///|
/// Activity is elapsed monotonic time, not durable destination progress.
pub struct ActivitySnapshot {
  kind : ActivityKind
  inactive_ms : UInt64
} derive(Eq, Debug)

///|
pub struct PipelineHealth {
  lifecycle : PipelineLifecycle
  live : Bool
  ready : Bool
  // Readiness reason: ok, initializing, inactive, or stopping.
  status : String
} derive(Eq, Debug)

///|
priv struct ActivityState {
  kind : ActivityKind
  mut observed : @bench.Timestamp
  mut source_paused : Bool
  mut destination_busy : Bool
}

///|
priv struct ActivityHandle {
  id : UInt64
  registry : Map[UInt64, ActivityState]
  state : ActivityState
  metrics : RuntimeMetrics
  worker_type : String
}

///|
fn ApplyControl::observe(
  self : ApplyControl,
  kind : ActivityKind,
  worker? : SessionWorker,
) -> ActivityHandle {
  let id = self.activity_sequence
  self.activity_sequence += 1
  let state = {
    kind,
    observed: @bench.monotonic_clock_start(),
    source_paused: false,
    destination_busy: false,
  }
  self.activity_registry[id] = state
  let worker_type = if kind is InitialTableCopy ||
    worker is Some(TableWorker(_, _)) {
    "table_sync"
  } else {
    "apply"
  }
  {
    id,
    registry: self.activity_registry,
    state,
    metrics: self.metrics_data,
    worker_type,
  }
}

///|
fn ActivityHandle::ping(self : ActivityHandle) -> Unit {
  self.state.observed = @bench.monotonic_clock_start()
}

///|
fn ActivityHandle::close(self : ActivityHandle) -> Unit {
  self.registry.remove(self.id)
  self.metrics.subscriptions.remove(self.id)
  self.metrics.source_monitors.remove(self.id)
}

///|
fn ActivityHandle::source_paused(self : ActivityHandle, paused : Bool) -> Unit {
  self.state.source_paused = paused
  if !paused && !self.state.destination_busy {
    self.ping()
  }
}

///|
fn ActivityHandle::destination_busy(self : ActivityHandle, busy : Bool) -> Unit {
  self.state.destination_busy = busy
  self.ping()
}

///|
/// Approximate observations for this control's run_pipeline workers. Finished
/// workers are absent; intentional source pauses do not accrue inactivity.
/// A concurrent destination write/flush still accrues inactivity.
pub fn ApplyControl::activities(
  self : ApplyControl,
) -> ReadOnlyArray[ActivitySnapshot] {
  let snapshots : Array[ActivitySnapshot] = []
  for _, state in self.activity_registry {
    snapshots.push({
      kind: state.kind,
      inactive_ms: if state.source_paused && !state.destination_busy {
        0UL
      } else {
        (@bench.monotonic_clock_end(state.observed).max(0.0) / 1000.0).to_uint64()
      },
    })
  }
  ReadOnlyArray::from_array(snapshots)
}

///|
fn activity_health(
  lifecycle : PipelineLifecycle,
  observations : ReadOnlyArray[ActivitySnapshot],
  stall_timeout_ms : UInt64,
) -> PipelineHealth {
  let inactive = observations.any(observation => {
    let allowance = match observation.kind {
      InitialTableCopy => stall_timeout_ms
      WalApply(wal_sender_timeout_ms~) =>
        if stall_timeout_ms > wal_sender_timeout_ms {
          stall_timeout_ms
        } else {
          wal_sender_timeout_ms
        }
    }
    observation.inactive_ms >= allowance
  })
  let status = if lifecycle == Stopping {
    "stopping"
  } else if inactive {
    "inactive"
  } else if lifecycle == Initializing || observations.is_empty() {
    "initializing"
  } else {
    "ok"
  }
  {
    lifecycle,
    live: lifecycle == Stopping || !inactive,
    ready: status == "ok",
    status,
  }
}

///|
/// Read-only liveness/readiness policy matching the pinned ETL replicator.
/// Initial copy can be ready; ready never means caught up or fully durable.
pub fn ApplyControl::health(
  self : ApplyControl,
  stall_timeout_ms? : Int = 300000,
) -> PipelineHealth raise {
  if stall_timeout_ms <= 0 {
    raise InvalidConfig("health stall timeout must be positive")
  }
  activity_health(
    self.lifecycle,
    self.activities(),
    stall_timeout_ms.to_uint64(),
  )
}

///|
async fn Connection::wal_activity_timeout(
  self : Connection,
  timeout : Int,
) -> UInt64 {
  // Failure/disabled setting uses the reference's one-minute allowance. This
  // observation does not replace replication startup's own connection checks.
  try {
    let rows = self.snapshot_query(
      "SELECT setting, unit FROM pg_settings WHERE name='wal_sender_timeout'", timeout,
      1048576,
    )
    let value = @string.parse_uint64(cell(rows, 0, 0))
    if cell(rows, 0, 1) == "ms" && value > 0 {
      value
    } else {
      60000UL
    }
  } catch {
    error => if @async.is_being_cancelled() { raise error } else { 60000UL }
  }
}

///|
pub extend PipelineLifecycle with Eq::{equal, not_equal}

///|
pub extend PipelineLifecycle with Debug::{to_repr}

///|
pub extend ActivityKind with Eq::{equal, not_equal}

///|
pub extend ActivityKind with Debug::{to_repr}

///|
pub extend ActivitySnapshot with Eq::{equal, not_equal}

///|
pub extend ActivitySnapshot with Debug::{to_repr}

///|
pub extend PipelineHealth with Eq::{equal, not_equal}

///|
pub extend PipelineHealth with Debug::{to_repr}
