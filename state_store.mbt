///|
pub(all) enum StateRecord {
  Pipeline
  ReplicationCheckpoint
  TableMetadata(UInt)
} derive(Eq, Debug)

///|
/// One store instance/namespace owns one pipeline. acquire must exclude every
/// other owner until release (including other processes). save must verify that
/// owner, atomically persist all bytes, and return only after durability. A lost
/// ownership/session must fail closed, never silently reconnect as the old owner.
/// load returns an atomic snapshot and is also available to read-only observers.
pub(open) trait StateStore {
  async fn acquire(Self, owner : String) -> Unit
  async fn release(Self, owner : String) -> Unit
  async fn load(Self, StateRecord) -> Bytes?
  async fn save(Self, owner : String, StateRecord, Bytes) -> Unit
}

///|
/// Valid only inside with_state_store or a pipeline's bind_state/startup lifetime.
pub struct StateSession {
  priv store : &StateStore
  priv owner : String
  priv mut active : Bool
  priv mutex : @async.Mutex
}

///|
pub async fn[T] with_state_store(
  store : &StateStore,
  run : async (StateSession) -> T,
) -> T {
  let owner = pipeline_nonce()
  let session : StateSession = {
    store,
    owner,
    active: true,
    mutex: @async.Mutex(),
  }
  store.acquire(owner)
  defer @async.protect_from_cancel(() => {
    session.active = false
    store.release(owner)
  })
  run(session)
}

///|
fn StateSession::check(self : StateSession) -> Unit raise {
  if !self.active {
    raise CheckpointError("state session is closed")
  }
}

///|
async fn StateSession::load(self : StateSession, key : StateRecord) -> Bytes? {
  self.check()
  let result = self.store.load(key)
  let limit = if key == ReplicationCheckpoint { 65536 } else { 16777216 }
  if result is Some(bytes) && bytes.length() > limit {
    raise CheckpointError("state record size")
  }
  result
}

///|
async fn StateSession::save(
  self : StateSession,
  key : StateRecord,
  bytes : Bytes,
) -> Unit {
  self.check()
  if bytes.length() > 16777216 {
    raise CheckpointError("state record size")
  }
  self.store.save(self.owner, key, bytes)
}

///|
async fn StateSession::checkpoint(self : StateSession, source : Source) -> Lsn {
  guard self.load(ReplicationCheckpoint) is Some(bytes) else {
    raise CheckpointError("replication checkpoint is missing")
  }
  parse_checkpoint(bytes, source)
}

///|
async fn StateSession::save_checkpoint(
  self : StateSession,
  source : Source,
  lsn : Lsn,
) -> Unit {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.load(ReplicationCheckpoint) is Some(bytes) {
    if parse_checkpoint(bytes, source) > lsn {
      raise CheckpointError("checkpoint would move backwards")
    }
  }
  self.save(ReplicationCheckpoint, checkpoint_bytes(source, lsn))
}

///|
/// Destination-owned metadata is scoped by this store's pipeline and source OID.
/// Persist it only after the corresponding destination operation is durable.
pub async fn StateSession::put_metadata(
  self : StateSession,
  table : UInt,
  value : Json,
) -> Unit {
  if table == 0U {
    raise InvalidConfig("zero table OID")
  }
  let document : Json = {
    "version": 1,
    "table": table.to_json(),
    "value": value,
  }
  let body = @utf8.encode(document.stringify())
  ignore(pg_json(document.stringify()))
  let bytes = @utf8.encode(@crypto.bytes_to_hex_string(sha256(body)) + "\n") +
    body
  @async.protect_from_cancel(() => self.save(TableMetadata(table), bytes))
}

///|
pub async fn StateSession::get_metadata(
  self : StateSession,
  table : UInt,
) -> Json? {
  if table == 0U {
    raise InvalidConfig("zero table OID")
  }
  guard self.load(TableMetadata(table)) is Some(bytes) else { return None }
  if bytes.length() < 66 || bytes[64] != b'\n' {
    raise CheckpointError("destination metadata envelope")
  }
  let body = bytes[65:].to_owned()
  if @utf8.decode(bytes[:64]) != @crypto.bytes_to_hex_string(sha256(body)) {
    raise CheckpointError("destination metadata checksum")
  }
  let value = pg_json(@utf8.decode(body))
  guard value is Object(fields) &&
    fields.get("version") == Some(Json::number(1)) &&
    fields.get("table") == Some(table.to_json()) else {
    raise CheckpointError("destination metadata identity")
  }
  guard fields.get("value") is Some(value) else {
    raise CheckpointError("destination metadata payload")
  }
  Some(value)
}

///|
pub async fn SnapshotPosition::save_checkpoint_in(
  self : SnapshotPosition,
  store : &StateStore,
) -> Unit {
  with_state_store(store, session => {
    if session.load(ReplicationCheckpoint) is Some(_) {
      raise CheckpointError("checkpoint exists; use Resume")
    }
    @async.protect_from_cancel(() => {
      session.save_checkpoint(self.source, self.lsn)
    })
  })
}

///|
/// Observe a store without acquiring pipeline ownership or changing records.
pub async fn state_store_status(store : &StateStore) -> PipelineStatus? {
  guard store.load(Pipeline) is Some(bytes) else { return None }
  if bytes.length() > 16777216 {
    raise CheckpointError("pipeline state size")
  }
  Some(pipeline_state_status(parse_pipeline_state(bytes)))
}
