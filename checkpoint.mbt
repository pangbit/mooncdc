///|
/// A fresh explicit WAL position, or strict recovery from an existing checkpoint.
pub(all) enum Start {
  At(Lsn)
  AfterSnapshot(SnapshotPosition)
  Resume
}

///|
/// Persist a completed, business-durable snapshot handoff for a later Resume.
/// The parent directory must exist; existing checkpoints are never overwritten.
pub async fn SnapshotPosition::save_checkpoint(
  self : SnapshotPosition,
  path : String,
) -> Unit {
  if path.is_empty() {
    raise InvalidConfig("empty snapshot checkpoint path")
  }
  let lock = @fs.open(
    path + ".lock",
    mode=ReadWrite,
    create_mode=OpenOrCreate,
    permission=0o600,
  )
  defer lock.close()
  if !lock.try_lock(Exclusive) {
    raise CheckpointError("checkpoint already in use")
  }
  if @fs.exists(path) {
    raise CheckpointError("checkpoint exists; use Resume")
  }
  @async.protect_from_cancel(() => save_checkpoint(path, self.source, self.lsn))
}

///|
priv struct Source {
  system_id : String
  timeline : String
  database : String
  slot : String
  publication : String
} derive(Eq, ToJson, @json.FromJson)

///|
priv struct Checkpoint {
  version : Int
  source : Source
  lsn : String
} derive(ToJson, @json.FromJson)

///|
fn checkpoint_bytes(source : Source, lsn : Lsn) -> Bytes {
  let value : Checkpoint = { version: 1, source, lsn: lsn.to_string(), }
  let body = @utf8.encode(ToJson::to_json(value).stringify())
  @utf8.encode(@crypto.bytes_to_hex_string(sha256(body)) + "\n") + body
}

///|
fn parse_checkpoint(bytes : Bytes, source : Source) -> Lsn raise {
  try {
    if bytes.length() < 66 || bytes[64] != b'\n' {
      raise CheckpointError("invalid envelope")
    }
    let body = bytes[65:].to_owned()
    if @utf8.decode(bytes[:64]) != @crypto.bytes_to_hex_string(sha256(body)) {
      raise CheckpointError("checksum mismatch")
    }
    let cp : Checkpoint = @json.from_json(@json.parse(@utf8.decode(body)))
    if cp.version != 1 || cp.source != source {
      raise CheckpointError("version or source/slot/publication mismatch")
    }
    Lsn::parse(cp.lsn)
  } catch {
    e => raise CheckpointError("invalid checkpoint: \{e}")
  }
}

///|
async fn save_checkpoint(
  path : String,
  source : Source,
  lsn : Lsn,
  boundary? : async (String) -> Unit = _ => (),
) -> Unit {
  save_atomic_file(path, checkpoint_bytes(source, lsn), boundary~)
}

///|
async fn save_atomic_file(
  path : String,
  bytes : Bytes,
  boundary? : async (String) -> Unit = _ => (),
) -> Unit {
  let temp = path + ".tmp"
  boundary("before_write")
  let file = @fs.open(
    temp,
    mode=WriteOnly,
    create_mode=CreateOrTruncate,
    permission=0o600,
  )
  defer file.close()
  file.write(bytes)
  boundary("after_write")
  file.sync()
  boundary("after_file_sync")
  @fs.rename(temp, path)
  boundary("after_rename")
  let parent = match path.rev_find("/") {
    Some(i) => if i == 0 { "/" } else { path[:i].to_owned() }
    None => "."
  }
  let dir = @fs.open(parent, mode=ReadOnly)
  defer dir.close()
  dir.sync()
  boundary("after_directory_sync")
}

///|
async fn load_checkpoint(path : String, source : Source) -> Lsn {
  let file = @fs.open(path, mode=ReadOnly)
  defer file.close()
  let size = file.size()
  if size < 0 || size > 65536 {
    raise CheckpointError("file size")
  }
  parse_checkpoint(file.read_exactly(size.to_int()), source)
}
