///|
/// Atomic records in an existing directory, protected by a process-wide file lock.
pub struct FileStateStore {
  priv directory : String
  priv mut lock : @fs.File?
  priv mut owner : String?
  priv mutex : @async.Mutex
}

///|
pub fn FileStateStore::new(directory : String) -> FileStateStore raise {
  if directory.is_empty() {
    raise InvalidConfig("empty state directory")
  }
  { directory, lock: None, owner: None, mutex: @async.Mutex(), }
}

///|
fn state_record_name(key : StateRecord) -> String {
  match key {
    Pipeline => "pipeline"
    ReplicationCheckpoint => "checkpoint"
    TableMetadata(oid) => "table-" + oid.to_string()
  }
}

///|
pub impl StateStore for FileStateStore with fn acquire(self, owner) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.owner is Some(_) {
    raise CheckpointError("state store already in use")
  }
  let lock = @fs.open(
    self.directory + "/.lock",
    mode=ReadWrite,
    create_mode=OpenOrCreate,
    permission=0o600,
  )
  errdefer lock.close()
  if !lock.try_lock(Exclusive) {
    raise CheckpointError("state store already in use")
  }
  self.lock = Some(lock)
  self.owner = Some(owner)
}

///|
pub impl StateStore for FileStateStore with fn release(self, owner) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.owner != Some(owner) {
    raise CheckpointError("stale state owner")
  }
  self.owner = None
  if self.lock is Some(lock) {
    lock.close()
  }
  self.lock = None
}

///|
pub impl StateStore for FileStateStore with fn load(self, key) {
  let path = self.directory + "/" + state_record_name(key)
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
pub impl StateStore for FileStateStore with fn save(self, owner, key, bytes) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.owner != Some(owner) || self.lock is None {
    raise CheckpointError("stale state owner")
  }
  if bytes.length() > 16777216 {
    raise CheckpointError("state record size")
  }
  @async.protect_from_cancel(() => {
    save_atomic_file(self.directory + "/" + state_record_name(key), bytes)
  })
}
