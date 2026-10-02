///|
priv enum StoredPhase {
  Pending
  Copying
  Catchup(String)
  Ready(String)
  Errored(String, TableRetryPolicy, Int)
} derive(ToJson, @json.FromJson)

///|
priv struct StoredTable {
  oid : UInt
  phase : StoredPhase
} derive(ToJson, @json.FromJson)

///|
priv struct PipelineState {
  version : Int
  source : Source
  destination_id : String
  initial_lsn : String?
  checkpoint_initialized : Bool
  tables : Array[StoredTable]
} derive(ToJson, @json.FromJson)

///|
priv struct PipelineStore {
  path : String
  mut state : PipelineState
  mutex : @async.Mutex
}

///|
fn state_bytes(state : PipelineState) -> Bytes {
  let body = @utf8.encode(ToJson::to_json(state).stringify())
  @utf8.encode(@crypto.bytes_to_hex_string(sha256(body)) + "\n") + body
}

///|
fn parse_pipeline_state(bytes : Bytes) -> PipelineState raise {
  try {
    if bytes.length() < 66 || bytes[64] != b'\n' {
      raise CheckpointError("pipeline state envelope")
    }
    let body = bytes[65:].to_owned()
    if @utf8.decode(bytes[:64]) != @crypto.bytes_to_hex_string(sha256(body)) {
      raise CheckpointError("pipeline state checksum")
    }
    let state : PipelineState = @json.from_json(@json.parse(@utf8.decode(body)))
    if state.version != 1 ||
      state.destination_id.is_empty() ||
      state.tables.length() > 1024 {
      raise CheckpointError("pipeline state version or bounds")
    }
    identifier(state.source.slot)
    identifier(state.source.publication)
    let seen : Map[UInt, Bool] = Map([])
    for table in state.tables {
      if seen.contains(table.oid) || table.oid == 0U {
        raise CheckpointError("duplicate or invalid table OID")
      }
      seen[table.oid] = true
      match table.phase {
        Errored(reason, policy, attempts) =>
          if reason.is_empty() ||
            attempts < 0 ||
            (policy is TimedRetry(at) && at == 0UL) {
            raise CheckpointError("invalid table error state")
          }
        Catchup(lsn) | Ready(lsn) =>
          if Lsn::parse(lsn).value == 0 ||
            (
              state.initial_lsn is Some(initial) &&
              Lsn::parse(lsn) < Lsn::parse(initial)
            ) {
            raise CheckpointError("table snapshot precedes initial position")
          }
        _ => ()
      }
    }
    if state.initial_lsn is Some(lsn) {
      if Lsn::parse(lsn).value == 0 {
        raise CheckpointError("zero initial position")
      }
    } else if state.checkpoint_initialized || !state.tables.is_empty() {
      raise CheckpointError("uninitialized pipeline contains progress")
    }
    state
  } catch {
    e => raise CheckpointError("invalid pipeline state: \{e}")
  }
}

///|
async fn read_pipeline_state(path : String) -> PipelineState {
  let file = @fs.open(path, mode=ReadOnly)
  defer file.close()
  let size = file.size()
  if size < 0 || size > 16777216 {
    raise CheckpointError("pipeline state size")
  }
  parse_pipeline_state(file.read_exactly(size.to_int()))
}

///|
async fn PipelineStore::update(
  self : PipelineStore,
  change : (PipelineState) -> PipelineState raise,
) -> Unit {
  self.mutex.acquire()
  defer self.mutex.release()
  let next = change(self.state)
  let bytes = state_bytes(next)
  ignore(parse_pipeline_state(bytes))
  @async.protect_from_cancel(() => {
    save_atomic_file(self.path, bytes)
    self.state = next
  })
}

///|
async fn PipelineStore::set_phase(
  self : PipelineStore,
  oid : UInt,
  phase : StoredPhase,
) -> Unit {
  self.update(state => {
    let mut found = false
    let tables = state.tables.map(table => {
      if table.oid == oid {
        found = true
        { ..table, phase, }
      } else {
        table
      }
    })
    if !found {
      raise CheckpointError("table removed during copy")
    }
    { ..state, tables, }
  })
}

///|
/// Persisted table progress. Copying includes private-slot replay; Catchup/Ready
/// retain the durable handover cutoff (initial snapshot or last private commit end).
pub(all) enum TableSyncPhase {
  Pending
  Copying
  Catchup(Lsn)
  Ready(Lsn)
  Errored(reason~ : String, retry~ : TableRetryPolicy, attempts~ : Int)
} derive(Eq, Debug)

///|
pub struct TableSyncStatus {
  oid : UInt
  phase : TableSyncPhase
} derive(Debug)

///|
pub struct PipelineStatus {
  slot : String
  destination_id : String
  tables : ReadOnlyArray[TableSyncStatus]
} derive(Debug)

///|
/// Read an atomic state snapshot without changing a running pipeline.
pub async fn pipeline_status(path : String) -> PipelineStatus {
  let state = read_pipeline_state(path)
  {
    slot: state.source.slot,
    destination_id: state.destination_id,
    tables: ReadOnlyArray::from_array(
      state.tables.map(table => {
        oid: table.oid,
        phase: match table.phase {
          Pending => Pending
          Copying => Copying
          Catchup(lsn) => Catchup(Lsn::parse(lsn))
          Ready(lsn) => Ready(Lsn::parse(lsn))
          Errored(reason, retry, attempts) =>
            Errored(reason~, retry~, attempts~)
        },
      }),
    ),
  }
}
