///|
async fn record_lock(path : String, session : StateSession?) -> @fs.File? {
  if session is Some(_) {
    return None
  }
  let lock = @fs.open(
    path + ".lock",
    mode=ReadWrite,
    create_mode=OpenOrCreate,
    permission=0o600,
  )
  errdefer lock.close()
  if !lock.try_lock(Exclusive) {
    raise CheckpointError("state/checkpoint already in use")
  }
  Some(lock)
}

///|
async fn load_record(
  path : String,
  key : StateRecord,
  session : StateSession?,
) -> Bytes? {
  if session is Some(store) {
    return store.load(key)
  }
  if !@fs.exists(path) {
    return None
  }
  let file = @fs.open(path, mode=ReadOnly)
  defer file.close()
  let size = file.size()
  if size < 0 || size > 16777216 {
    raise CheckpointError("state record size")
  }
  Some(file.read_exactly(size.to_int()))
}

///|
async fn stored_checkpoint(
  path : String,
  source : Source,
  session : StateSession?,
) -> Lsn {
  match session {
    Some(store) => store.checkpoint(source)
    None => load_checkpoint(path, source)
  }
}

///|
async fn store_checkpoint(
  path : String,
  source : Source,
  lsn : Lsn,
  session : StateSession?,
) -> Unit {
  match session {
    Some(store) => store.save_checkpoint(source, lsn)
    None => save_checkpoint(path, source, lsn)
  }
}

///|
/// Persist table synchronization using legacy files or an exclusive StateStore.
/// Provide exactly one of state_path and state_store; keep store identity stable.
pub async fn run_pipeline(
  connection : ConnectionConfig,
  destination : &Destination,
  publication~ : String,
  destination_id~ : String,
  control~ : ApplyControl,
  state_path? : String = "",
  state_store? : &StateStore,
  slot_prefix? : String = "mooncdc",
  copy_concurrency? : Int = 2,
  batch_rows? : Int = 256,
  schema_store? : &SchemaStore,
  table_retry_delay_ms? : Int = 10000,
  max_table_retries? : Int = 5,
) -> Unit {
  validate_connection(connection)
  identifier(publication)
  identifier(slot_prefix)
  if slot_prefix.length() > 32 ||
    (state_path.is_empty() && state_store is None) ||
    destination_id.is_empty() ||
    copy_concurrency <= 0 ||
    copy_concurrency > 16 ||
    batch_rows <= 0 ||
    batch_rows > 1024 ||
    table_retry_delay_ms <= 0 ||
    max_table_retries <= 0 {
    raise InvalidConfig("pipeline identity or copy limits")
  }
  if state_store is Some(_) && !state_path.is_empty() {
    raise InvalidConfig("choose state_path or state_store")
  }
  if control.stopped {
    return
  }
  if control.table_retry is Some(_) {
    raise InvalidConfig("control already belongs to a running pipeline")
  }
  let run = async fn(session : StateSession?) -> Unit {
    run_pipeline_impl(
      connection,
      destination,
      publication~,
      destination_id~,
      state_path~,
      control~,
      slot_prefix~,
      copy_concurrency~,
      batch_rows~,
      schema_store?,
      table_retry_delay_ms~,
      max_table_retries~,
      session?,
    )
  }
  match state_store {
    Some(store) => with_state_store(store, session => run(Some(session)))
    None => run(None)
  }
}

///|
/// Bounded CDC subscription with a legacy checkpoint file or exclusive StateStore.
/// Returning/raising cancels owned tasks, closes the socket and releases ownership.
pub async fn[T] subscribe(
  connection : ConnectionConfig,
  slot~ : String,
  publication~ : String,
  start~ : Start,
  consume : async (Subscription) -> T,
  checkpoint_path? : String = "",
  checkpoint_store? : &StateStore,
  max_frame_bytes? : Int = 16777216,
  max_transaction_bytes? : Int = 67108864,
  max_transaction_events? : Int = 100000,
  max_pending_transactions? : Int = 16,
  max_metadata_entries? : Int = 4096,
  heartbeat_ms? : Int = 1000,
  max_reconnects? : Int = 3,
  reconnect_delay_ms? : Int = 250,
  binary? : Bool = false,
  messages? : Bool = false,
  nontransactional_message? : async (LogicalMessage) -> Unit,
  backpressure? : Bool = false,
) -> T {
  validate_connection(connection)
  identifier(slot)
  identifier(publication)
  if max_frame_bytes < 64 ||
    max_frame_bytes > 1073741824 ||
    max_transaction_bytes < max_frame_bytes ||
    max_transaction_events <= 0 ||
    max_pending_transactions <= 0 ||
    max_metadata_entries <= 0 ||
    heartbeat_ms <= 0 ||
    max_reconnects < 0 ||
    reconnect_delay_ms < 0 ||
    (checkpoint_path.is_empty() && checkpoint_store is None) {
    raise InvalidConfig("invalid resource limit, interval or checkpoint path")
  }
  if checkpoint_store is Some(_) && !checkpoint_path.is_empty() {
    raise InvalidConfig("choose checkpoint_path or checkpoint_store")
  }
  let run = async fn(state_session : StateSession?) -> T {
    subscribe_impl(
      connection,
      slot~,
      publication~,
      start~,
      checkpoint_path~,
      consume,
      max_frame_bytes~,
      max_transaction_bytes~,
      max_transaction_events~,
      max_pending_transactions~,
      max_metadata_entries~,
      heartbeat_ms~,
      max_reconnects~,
      reconnect_delay_ms~,
      binary~,
      messages~,
      nontransactional_message?,
      backpressure~,
      state_session?,
    )
  }
  match checkpoint_store {
    Some(store) => with_state_store(store, session => run(Some(session)))
    None => run(None)
  }
}
