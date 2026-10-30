///|
// A durable intent bridges two separately atomic state records. Recovery accepts
// either checkpoint binding while the intent is present, never an arbitrary pair.
priv struct TimelinePromotion {
  from : Source
  to : Source
  position : String
  fork : String
  horizon : String
} derive(ToJson, @json.FromJson)

///|
// An outer pipeline owns the proof and record rebinding. Subscription itself
// never accepts a checkpoint from a different timeline.
priv suberror PipelineSourceChanged {
  PipelineSourceChanged
}

///|
priv suberror PipelinePromotionRequested {
  PipelinePromotionRequested(Bool)
}

///|
fn timeline_number(text : String) -> UInt32 raise {
  if text.is_empty() || text.length() > 10 || text.any(c => c < '0' || c > '9') {
    raise UnsafeResume("invalid timeline number")
  }
  let number = @string.parse_uint(text)
  if number == 0U || number.to_string() != text {
    raise UnsafeResume("noncanonical timeline number")
  }
  number
}

///|
fn timeline_filename(number : UInt32) -> String {
  let hex = number.to_string(radix=16).to_upper()
  "0".repeat(8 - hex.length()) + hex + ".history"
}

///|
fn timeline_fork(
  history : String,
  ancestor : UInt32,
  current : UInt32,
) -> Lsn raise {
  if history.is_empty() ||
    @utf8.encode(history).length() > 1048576 ||
    !history.has_suffix("\n") ||
    ancestor >= current ||
    ancestor == 0U {
    raise UnsafeResume("invalid timeline history bounds")
  }
  let mut previous = 0U
  let mut previous_fork : Lsn = { value: 0, }
  let mut found : Lsn? = None
  let mut count = 0
  for raw in history.split("\n") {
    let line = raw.trim().to_owned()
    if line.is_empty() || line.has_prefix("#") {
      continue
    }
    count += 1
    let fields = line
      .replace_all(old="\t", new=" ")
      .split(" ")
      .filter(s => !s.is_empty())
      .collect()
    if count > 4096 || fields.length() < 3 {
      raise UnsafeResume("invalid timeline history row")
    }
    let timeline = timeline_number(fields[0].to_owned())
    let fork = Lsn::parse(fields[1].to_owned())
    if (previous == 0U && timeline != 1U) ||
      timeline <= previous ||
      timeline >= current ||
      fork.value == 0 ||
      fork < previous_fork {
      raise UnsafeResume("invalid timeline history ancestry")
    }
    if timeline == ancestor {
      found = Some(fork)
    }
    previous = timeline
    previous_fork = fork
  }
  guard found is Some(fork) else {
    raise UnsafeResume("stored timeline is not an ancestor")
  }
  fork
}

///|
fn wal_write_horizon(tx : Transaction, batch : WalBatch?) -> Lsn raise {
  match batch {
    Some(fragment) =>
      match fragment.end_lsn {
        Some(end) => end
        None => {
          if tx.commit_lsn.value == 0xffffffffffffffffUL {
            raise Protocol("WAL write horizon overflow")
          }
          // A fork at the start of this Commit record must reject its partial rows.
          { value: tx.commit_lsn.value + 1UL, }
        }
      }
    None => tx.end_lsn
  }
}

///|
async fn PipelineStore::record_write_horizon(
  self : PipelineStore,
  position : Lsn,
) -> Unit {
  // Never upgrade unknown legacy target history merely by seeing a new write.
  if self.state.write_horizon is None {
    return
  }
  if self.state.write_horizon is Some(old) && Lsn::parse(old) >= position {
    return
  }
  self.update(state => {
    guard state.write_horizon is Some(old) else { return state }
    if Lsn::parse(old) >= position {
      state
    } else {
      { ..state, write_horizon: Some(position.to_string()), }
    }
  })
}

///|
fn required_ancestor_position(
  state : PipelineState,
  checkpoint : Lsn,
) -> Lsn raise {
  guard state.write_horizon is Some(horizon) else {
    raise UnsafeResume("legacy state lacks a durable target write horizon")
  }
  let mut required = Lsn::parse(horizon)
  if checkpoint > required {
    required = checkpoint
  }
  if state.initial_lsn is Some(initial) && Lsn::parse(initial) > required {
    required = Lsn::parse(initial)
  }
  for table in state.tables {
    match table.phase {
      Catchup(position) | Ready(position) => {
        let cutoff = Lsn::parse(position)
        if cutoff > required {
          required = cutoff
        }
      }
      _ => ()
    }
  }
  required
}

///|
async fn PipelineStore::recover_promotion(
  self : PipelineStore,
  conn : Connection,
  actual : Source,
  boundary~ : async (String) -> Unit = _ => (),
) -> Unit {
  let state = self.state
  if state.version != 1 ||
    !state.checkpoint_initialized ||
    { ..actual, timeline: state.source.timeline, } != state.source ||
    timeline_number(actual.timeline) <= timeline_number(state.source.timeline) {
    raise UnsafeResume("incompatible promoted source")
  }
  let path = self.path + ".checkpoint"
  let lock = record_lock(path, self.session)
  defer (if lock is Some(lock) { lock.close() })
  guard load_record(path, ReplicationCheckpoint, self.session) is Some(bytes) else {
    raise CheckpointError("pipeline checkpoint is missing")
  }
  let cp = parse_checkpoint_record(bytes)
  let position = Lsn::parse(cp.lsn)
  if state.promotion is Some(intent) {
    if intent.to != actual ||
      intent.position != cp.lsn ||
      (cp.source != state.source && cp.source != actual) {
      raise UnsafeResume("promotion intent/checkpoint/source mismatch")
    }
  } else if cp.source != state.source {
    raise CheckpointError("checkpoint source changed without promotion intent")
  }
  let current = timeline_number(actual.timeline)
  let history = conn.query(
    "TIMELINE_HISTORY \{current}",
    max_result_bytes=1048576,
  )
  if history.length() != 1 ||
    history[0].length() != 2 ||
    cell(history, 0, 0) != timeline_filename(current) {
    raise UnsafeResume("invalid timeline history response")
  }
  let fork = timeline_fork(
    cell(history, 0, 1),
    timeline_number(state.source.timeline),
    current,
  )
  if required_ancestor_position(state, position) > fork {
    raise UnsafeResume("promoted timeline diverges before target write horizon")
  }
  if state.promotion is Some(intent) && intent.fork != fork.to_string() {
    raise UnsafeResume("timeline history changed during promotion recovery")
  }
  // Validate the existing synchronized slot before changing either binding.
  let slots = conn.query(
    "SELECT failover::text,temporary::text FROM pg_replication_slots WHERE slot_name='\{actual.slot}'",
  )
  if slots != [[Some("true"), Some("false")]] {
    raise UnsafeResume(
      "promotion requires an existing persistent failover slot",
    )
  }
  validate_replication_position(conn, actual, position, position)
  let intent : TimelinePromotion = {
    from: state.source,
    to: actual,
    position: position.to_string(),
    fork: fork.to_string(),
    horizon: state.write_horizon.unwrap(),
  }
  self.commit_promotion(intent, boundary~)
}

///|
async fn PipelineStore::commit_promotion(
  self : PipelineStore,
  intent : TimelinePromotion,
  boundary~ : async (String) -> Unit = _ => (),
) -> Unit {
  self.update(state => { ..state, promotion: Some(intent), })
  boundary("after_intent")
  @async.protect_from_cancel(() => {
    let bytes = checkpoint_bytes(intent.to, Lsn::parse(intent.position))
    match self.session {
      Some(session) => session.save(ReplicationCheckpoint, bytes)
      None => save_atomic_file(self.path + ".checkpoint", bytes)
    }
  })
  boundary("after_checkpoint")
  self.update(state => { ..state, source: intent.to, promotion: None, })
  boundary("after_complete")
}
