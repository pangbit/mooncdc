///|
/// Persisted schemas must be visible in the cache only after durable storage succeeds.
pub(open) trait SchemaStore {
  async fn get(Self, UInt, SchemaId) -> SchemaSnapshot?
  async fn all(Self) -> ReadOnlyArray[SchemaSnapshot]
  async fn put(Self, SchemaSnapshot) -> Unit
  // Keep the newest version at/before each bound, and all versions after it.
  async fn prune(Self, ReadOnlyArray[(UInt, SchemaId)]) -> Int
}

///|
priv struct StoredSchema {
  commit_lsn : String
  message_lsn : String
  payload : Json
} derive(ToJson, @json.FromJson)

///|
/// Canonical versioned representation for external SchemaStore implementations.
pub impl ToJson for SchemaSnapshot with fn to_json(self) {
  ToJson::to_json(StoredSchema::{
    commit_lsn: self.id.commit_lsn.to_string(),
    message_lsn: self.id.message_lsn.to_string(),
    payload: self.payload,
  })
}

///|
/// Reconstruct validated schema metadata and identity from the canonical envelope.
pub fn SchemaSnapshot::from_json(json : Json) -> SchemaSnapshot raise {
  try {
    let stored : StoredSchema = @json.from_json(json)
    parse_schema(stored.payload, {
      commit_lsn: Lsn::parse(stored.commit_lsn),
      message_lsn: Lsn::parse(stored.message_lsn),
    })
  } catch {
    e => raise Protocol("invalid serialized schema: \{e}")
  }
}

///|
priv struct SchemaFile {
  version : Int
  identity : String
  schemas : Array[StoredSchema]
} derive(ToJson, @json.FromJson)

///|
priv struct FileSchemaStore {
  path : String
  identity : String
  mut schemas : Array[SchemaSnapshot]
  mutex : @async.Mutex
  mut closed : Bool
}

///|
fn schema_file_bytes(
  identity : String,
  schemas : Array[SchemaSnapshot],
) -> Bytes raise {
  let data : SchemaFile = {
    version: 1,
    identity,
    schemas: schemas.map(schema => {
      commit_lsn: schema.id.commit_lsn.to_string(),
      message_lsn: schema.id.message_lsn.to_string(),
      payload: schema.payload,
    }),
  }
  let body = @utf8.encode(ToJson::to_json(data).stringify())
  if body.length() > 16777151 {
    raise LimitExceeded("schema store size; prune obsolete versions")
  }
  @utf8.encode(@crypto.bytes_to_hex_string(sha256(body)) + "\n") + body
}

///|
fn read_schema_bytes(
  bytes : Bytes,
  identity : String,
) -> Array[SchemaSnapshot] raise {
  try {
    if bytes.length() < 66 || bytes[64] != b'\n' {
      raise CheckpointError("schema envelope")
    }
    let body = bytes[65:].to_owned()
    if @utf8.decode(bytes[:64]) != @crypto.bytes_to_hex_string(sha256(body)) {
      raise CheckpointError("schema checksum")
    }
    let data : SchemaFile = @json.from_json(@json.parse(@utf8.decode(body)))
    if data.version != 1 || data.identity != identity {
      raise CheckpointError("schema store identity or version")
    }
    let seen : Map[String, Bool] = Map([])
    data.schemas.map(stored => {
      let schema = parse_schema(stored.payload, {
        commit_lsn: Lsn::parse(stored.commit_lsn),
        message_lsn: Lsn::parse(stored.message_lsn),
      })
      let key = "\{schema.oid}/\{schema.id.commit_lsn.to_string()}/\{schema.id.message_lsn.to_string()}"
      if seen.contains(key) {
        raise CheckpointError("duplicate schema version")
      }
      seen[key] = true
      schema
    })
  } catch {
    e => raise CheckpointError("invalid schema store: \{e}")
  }
}

///|
async fn FileSchemaStore::save(
  self : FileSchemaStore,
  schemas : Array[SchemaSnapshot],
) -> Unit {
  let bytes = schema_file_bytes(self.identity, schemas)
  @async.protect_from_cancel(() => {
    save_atomic_file(self.path, bytes)
    self.schemas = schemas
  })
}

///|
impl SchemaStore for FileSchemaStore with fn get(self, oid, bound) {
  if self.closed {
    raise Closed
  }
  let mut found : SchemaSnapshot? = None
  for schema in self.schemas {
    if schema.oid == oid && schema.id <= bound {
      if found is None || (found is Some(old) && old.id < schema.id) {
        found = Some(schema)
      }
    }
  }
  found
}

///|
impl SchemaStore for FileSchemaStore with fn all(self) {
  if self.closed {
    raise Closed
  }
  ReadOnlyArray::from_array(self.schemas.copy())
}

///|
impl SchemaStore for FileSchemaStore with fn put(self, schema) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.closed {
    raise Closed
  }
  for old in self.schemas {
    if old.oid == schema.oid && old.id == schema.id {
      if old != schema {
        raise UnsafeResume("schema changed at the same snapshot version")
      }
      return
    }
  }
  let next = self.schemas.copy()
  next.push(schema)
  self.save(next)
}

///|
impl SchemaStore for FileSchemaStore with fn prune(self, bounds) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.closed {
    raise Closed
  }
  let keep : Map[UInt, SchemaId] = Map([])
  let seen : Map[UInt, Bool] = Map([])
  for (oid, bound) in bounds {
    if seen.contains(oid) {
      raise InvalidConfig("duplicate schema retention table")
    }
    seen[oid] = true
    if SchemaStore::get(self, oid, bound) is Some(schema) {
      keep[oid] = schema.id
    }
  }
  let next = self.schemas.filter(schema => {
    match keep.get(schema.oid) {
      Some(id) => schema.id >= id
      None => true
    }
  })
  let removed = self.schemas.length() - next.length()
  if removed > 0 {
    self.save(next)
  }
  removed
}

///|
/// Exclusive checksummed file store with bounded atomic writes. The identity binds
/// source/publication/destination; keep it stable and never reuse a path for another source.
pub async fn[T] with_file_schema_store(
  path : String,
  identity~ : String,
  run : async (&SchemaStore) -> T,
) -> T {
  if path.is_empty() || identity.is_empty() {
    raise InvalidConfig("schema store identity or path")
  }
  let lock = @fs.open(
    path + ".lock",
    mode=ReadWrite,
    create_mode=OpenOrCreate,
    permission=0o600,
  )
  defer lock.close()
  if !lock.try_lock(Exclusive) {
    raise CheckpointError("schema store already in use")
  }
  let schemas = if @fs.exists(path) {
    let file = @fs.open(path, mode=ReadOnly)
    defer file.close()
    let size = file.size()
    if size < 0 || size > 16777216 {
      raise CheckpointError("schema store size")
    }
    read_schema_bytes(file.read_exactly(size.to_int()), identity)
  } else {
    []
  }
  let store : FileSchemaStore = {
    path,
    identity,
    schemas,
    mutex: @async.Mutex(),
    closed: false,
  }
  defer {
    store.closed = true
  }
  if !@fs.exists(path) {
    store.save([])
  }
  run(store)
}
