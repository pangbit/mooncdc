///|
priv suberror PipelineStopped {
  PipelineStopped
}

///|
fn pipeline_nonce() -> String raise {
  guard @env.rand(12) is Some(bytes) else {
    raise Authentication("secure entropy unavailable")
  }
  @crypto.bytes_to_hex_string(bytes)
}

///|
fn copy_slot_prefix(main_slot : String) -> String {
  let digest = @crypto.bytes_to_hex_string(sha256(@utf8.encode(main_slot)))
  "mooncdc_copy_" + digest[:24].to_owned() + "_"
}

///|
async fn publication_tables(
  conn : Connection,
  publication : String,
  timeout : Int,
) -> Array[UInt] {
  let rows = conn.snapshot_query(
    "SELECT DISTINCT c.oid::text FROM pg_catalog.pg_publication_tables p JOIN pg_catalog.pg_namespace n ON n.nspname=p.schemaname JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename WHERE p.pubname='\{publication}' ORDER BY 1",
    timeout,
    1048576,
  )
  if rows.is_empty() {
    raise InvalidConfig("publication has no tables")
  }
  rows.map(row => @string.parse_uint(cell([row], 0, 0)))
}

///|
priv struct PipelineDestination {
  destination : &Destination
  mutex : @async.Mutex
  store : PipelineStore
  connection : ConnectionConfig
  control : ApplyControl
  batch_rows : Int
  schema_store : &SchemaStore?
  mut frontier : Lsn?
  mut main_lsn : Lsn
  copy_jobs : Array[UInt]
  copy_ready : @async.CondVar
}

///|
impl Destination for PipelineDestination with fn reset_table(
  self,
  relation,
  attempt~,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.control.stopped {
    raise PipelineStopped
  }
  self.destination.reset_table(relation, attempt~)
}

///|
impl Destination for PipelineDestination with fn write_rows(
  self,
  relation,
  rows,
  id,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.control.stopped {
    raise PipelineStopped
  }
  self.destination.write_rows(relation, rows, id)
}

///|
async fn PipelineStore::advance_tables(
  self : PipelineStore,
  position : Lsn,
) -> Unit {
  if !self.state.tables.any(table => {
      match table.phase {
        Catchup(lsn) => Lsn::parse(lsn) <= position
        _ => false
      }
    }) {
    return
  }
  self.update(state => {
    ..state,
    tables: state.tables.map(table => {
      match table.phase {
        Catchup(lsn) if Lsn::parse(lsn) <= position =>
          { ..table, phase: Ready(lsn), }
        _ => table
      }
    }),
  })
}

///|
async fn PipelineDestination::copy_table(
  self : PipelineDestination,
  oid : UInt,
) -> Unit {
  if self.control.stopped {
    raise PipelineStopped
  }
  self.store.set_phase(oid, Copying)
  let slot = copy_slot_prefix(self.store.state.source.slot) + pipeline_nonce()
  ignore(
    copy_snapshot_to_impl(
      self.connection,
      self,
      slot~,
      publication=self.store.state.source.publication,
      batch_rows=self.batch_rows,
      max_frame_bytes=16777216,
      max_batch_bytes=67108864,
      temporary=true,
      table=Some(oid),
      schema_store?=self.schema_store,
      after_copy=(exporter, point) => self.catch_up_table(exporter, point, oid),
    ),
  )
}

///|
fn change_relations(change : Change) -> ReadOnlyArray[Relation] {
  match change {
    RelationChanged(r) | Insert(r, _) | Update(r, _, _) | Delete(r, _) => [r]
    Truncate(rs, ..) => rs
    _ => []
  }
}

///|
fn PipelineStore::project(
  self : PipelineStore,
  tx : Transaction,
) -> Transaction raise {
  let cutoffs : Map[UInt, Lsn] = Map([])
  for table in self.state.tables {
    match table.phase {
      Catchup(lsn) | Ready(lsn) => cutoffs[table.oid] = Lsn::parse(lsn)
      _ => ()
    }
  }
  project_tables(tx, cutoffs, self.state.source.publication)
}

///|
fn project_tables(
  tx : Transaction,
  cutoffs : Map[UInt, Lsn],
  publication : String,
  table_only? : Bool = false,
) -> Transaction raise {
  let keep = fn(oid : UInt) -> Bool {
    cutoffs.get(oid) is Some(cutoff) && tx.commit_lsn >= cutoff
  }
  let changes : Array[Change] = []
  let ids : Array[String?] = []
  let mut ordinal = 0
  for index, change in tx.changes {
    let original_id = if tx.projected_ids is Some(previous) {
      previous[index]
    } else {
      match change {
        Message(message) => Some(tx.id + "/message/" + message.lsn.to_string())
        Insert(_, _) | Update(_, _, _) | Delete(_, _) | Truncate(_, ..) => {
          let id = tx.id + "/" + ordinal.to_string()
          ordinal += 1
          Some(id)
        }
        _ => None
      }
    }
    // A multi-table truncate may be delivered by different workers. Scope each
    // fragment's replay ID to its table so destination dedup cannot erase one.
    if change is Truncate(relations, cascade~, restart_identity~) {
      for relation in relations {
        if keep(relation.oid) {
          changes.push(Truncate([relation], cascade~, restart_identity~))
          let suffix = "/table/" + relation.oid.to_string()
          ids.push(
            original_id.map(id => {
              if id.has_suffix(suffix) {
                id
              } else {
                id + suffix
              }
            }),
          )
        }
      }
      continue
    }
    let selected = match change {
      RelationChanged(r) | Insert(r, _) | Update(r, _, _) | Delete(r, _) =>
        if keep(r.oid) {
          Some(change)
        } else {
          None
        }
      Message(message) if message.prefix == "supabase_etl_ddl" =>
        match message.schema_snapshot(commit_lsn=tx.commit_lsn, publication~) {
          Some(schema) if cutoffs.contains(schema.oid) && keep(schema.oid) =>
            Some(change)
          _ => None
        }
      _ => if table_only { None } else { Some(change) }
    }
    if selected is Some(value) {
      changes.push(value)
      ids.push(original_id)
    }
  }
  {
    ..tx,
    changes: ReadOnlyArray::from_array(changes),
    projected_ids: Some(ReadOnlyArray::from_array(ids)),
  }
}

///|
impl Destination for PipelineDestination with fn write_transaction(self, tx) {
  // A table added while running joins the same bounded copy-worker queue.
  // Its private snapshot/slot covers changes skipped by the main worker.
  let missing : Map[UInt, Bool] = Map([])
  let known : Map[UInt, Bool] = Map([])
  for table in self.store.state.tables {
    known[table.oid] = true
  }
  for change in tx.changes {
    for relation in change_relations(change) {
      if !known.contains(relation.oid) {
        missing[relation.oid] = true
      }
    }
    if change is Message(message) &&
      message.schema_snapshot(
        commit_lsn=tx.commit_lsn,
        publication=self.store.state.source.publication,
      )
      is Some(schema) &&
      !known.contains(schema.oid) {
      let conn = connect(self.connection, 16777216, false)
      defer conn.close()
      if publication_tables(
          conn,
          self.store.state.source.publication,
          self.connection.timeout_ms,
        ).contains(schema.oid) {
        missing[schema.oid] = true
      }
    }
  }
  if !missing.is_empty() {
    let conn = connect(self.connection, 16777216, false)
    defer conn.close()
    let current = publication_tables(
      conn,
      self.store.state.source.publication,
      self.connection.timeout_ms,
    )
    for oid, _ in missing {
      if !current.contains(oid) {
        raise UnsafeResume("publication changed during table discovery")
      }
      self.store.update(state => {
        ..state,
        tables: state.tables + [{ oid, phase: Pending, }],
      })
      self.copy_jobs.push(oid)
      self.copy_ready.broadcast()
    }
  }
  self.mutex.acquire()
  defer self.mutex.release()
  let projected = self.store.project(tx)
  if self.schema_store is Some(store) {
    for change in projected.changes {
      if change is Message(message) &&
        message.schema_snapshot(
          commit_lsn=tx.commit_lsn,
          publication=self.store.state.source.publication,
        )
        is Some(schema) {
        store.put(schema)
      }
    }
  }
  let status = self.destination.write_transaction(projected)
  self.main_lsn = tx.end_lsn
  self.frontier = Some(tx.end_lsn)
  if status == Durable {
    self.store.advance_tables(tx.end_lsn)
    self.frontier = None
  }
  status
}

///|
impl Destination for PipelineDestination with fn flush(self) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.destination.flush()
  if self.frontier is Some(lsn) {
    self.store.advance_tables(lsn)
    self.frontier = None
  }
}

///|
/// Persist table synchronization across restarts. Owns a randomly named main slot
/// under slot_prefix and temporary per-table copy slots. Keep state and checkpoint
/// files together; source/destination identity changes require a new state path.
pub async fn run_pipeline(
  connection : ConnectionConfig,
  destination : &Destination,
  publication~ : String,
  destination_id~ : String,
  state_path~ : String,
  control~ : ApplyControl,
  slot_prefix? : String = "mooncdc",
  copy_concurrency? : Int = 2,
  batch_rows? : Int = 256,
  schema_store? : &SchemaStore,
) -> Unit {
  identifier(publication)
  identifier(slot_prefix)
  if slot_prefix.length() > 32 ||
    state_path.is_empty() ||
    destination_id.is_empty() ||
    copy_concurrency <= 0 ||
    copy_concurrency > 16 ||
    batch_rows <= 0 ||
    batch_rows > 1024 {
    raise InvalidConfig("pipeline identity or copy limits")
  }
  if control.stopped {
    return
  }
  let lock = @fs.open(
    state_path + ".lock",
    mode=ReadWrite,
    create_mode=OpenOrCreate,
    permission=0o600,
  )
  defer lock.close()
  if !lock.try_lock(Exclusive) {
    raise CheckpointError("pipeline already in use")
  }
  let checkpoint_path = state_path + ".checkpoint"
  let conn = connect(connection, 16777216, true)
  defer conn.close()
  let exists = @fs.exists(state_path)
  let state = if exists {
    read_pipeline_state(state_path)
  } else {
    if @fs.exists(checkpoint_path) {
      raise CheckpointError("checkpoint without pipeline state")
    }
    let slot = slot_prefix + "_" + pipeline_nonce()
    let source = @async.with_timeout(connection.timeout_ms, () => {
      identify(conn, connection, slot, publication)
    })
    PipelineState::{
      version: 1,
      source,
      destination_id,
      initial_lsn: None,
      checkpoint_initialized: false,
      tables: [],
    }
  }
  let store : PipelineStore = {
    path: state_path,
    state,
    mutex: @async.Mutex(),
  }
  let actual = @async.with_timeout(connection.timeout_ms, () => {
    identify(conn, connection, state.source.slot, publication)
  })
  if actual != state.source ||
    destination_id != state.destination_id ||
    !state.source.slot.has_prefix(slot_prefix + "_") {
    raise UnsafeResume("pipeline source, destination or slot prefix changed")
  }
  let tables = publication_tables(conn, publication, connection.timeout_ms)
  if !exists {
    store.update(state => state)
  }
  if store.state.initial_lsn is None {
    if @fs.exists(checkpoint_path) {
      raise CheckpointError("uninitialized state has a checkpoint")
    }
    // Only this durable creation intent can own this random slot. No table writes
    // occur before its initial position and checkpoint have both been persisted.
    @async.with_timeout(connection.timeout_ms, () => {
      let slot = state.source.slot
      let query = "SELECT plugin, database, active::text FROM pg_replication_slots WHERE slot_name='\{slot}'"
      let mut previous = conn.query(query)
      while previous.length() == 1 && cell(previous, 0, 2) == "true" {
        @async.sleep(20)
        previous = conn.query(query)
      }
      if !previous.is_empty() {
        if cell(previous, 0, 0) != "pgoutput" ||
          cell(previous, 0, 1) != connection.database {
          raise UnsafeResume("incompatible owned pipeline slot")
        }
        ignore(conn.query("SELECT pg_drop_replication_slot('\{slot}')"))
      }
      let created = conn.query(
        "CREATE_REPLICATION_SLOT \{slot} LOGICAL pgoutput NOEXPORT_SNAPSHOT",
      )
      if cell(created, 0, 0) != slot || cell(created, 0, 3) != "pgoutput" {
        raise Protocol("pipeline slot creation metadata")
      }
      let lsn = Lsn::parse(cell(created, 0, 1))
      store.update(state => { ..state, initial_lsn: Some(lsn.to_string()), })
    })
  }
  if !store.state.checkpoint_initialized {
    let checkpoint_lock = @fs.open(
      checkpoint_path + ".lock",
      mode=ReadWrite,
      create_mode=OpenOrCreate,
      permission=0o600,
    )
    defer checkpoint_lock.close()
    if !checkpoint_lock.try_lock(Exclusive) {
      raise CheckpointError("pipeline checkpoint already in use")
    }
    let position = Lsn::parse(store.state.initial_lsn.unwrap())
    if @fs.exists(checkpoint_path) {
      if load_checkpoint(checkpoint_path, state.source) != position {
        raise CheckpointError("unexpected initial pipeline checkpoint")
      }
    } else {
      save_checkpoint(checkpoint_path, state.source, position)
    }
    store.update(state => { ..state, checkpoint_initialized: true, })
  } else if !@fs.exists(checkpoint_path) {
    raise CheckpointError("pipeline checkpoint is missing")
  }
  store.update(state => {
    let previous : Map[UInt, StoredTable] = Map([])
    for table in state.tables {
      previous[table.oid] = table
    }
    {
      ..state,
      tables: tables.map(oid => {
        previous.get(oid).unwrap_or(StoredTable::{ oid, phase: Pending, })
      }),
    }
  })
  if schema_store is Some(schemas) {
    let checkpoint = load_checkpoint(checkpoint_path, store.state.source)
    for table in store.state.tables {
      match table.phase {
        Catchup(cutoff) | Ready(cutoff) => {
          let cutoff = Lsn::parse(cutoff)
          let bound : SchemaId = if checkpoint > cutoff {
            SchemaId::before_lsn(checkpoint)
          } else {
            { commit_lsn: cutoff, message_lsn: { value: 0, }, }
          }
          if schemas.get(table.oid, bound) is None {
            raise UnsafeResume("completed table has no retained schema")
          }
        }
        _ => ()
      }
    }
  }
  conn.close()
  subscribe(
    connection,
    slot=state.source.slot,
    publication~,
    start=Resume,
    checkpoint_path~,
    backpressure=true,
    messages=true,
    nontransactional_message=message => {
      if message.prefix == "supabase_etl_ddl" {
        raise Protocol("DDL message must be transactional")
      }
    },
    sub => {
      let sink : PipelineDestination = {
        destination,
        mutex: @async.Mutex(),
        store,
        connection,
        control,
        batch_rows,
        schema_store,
        frontier: None,
        main_lsn: load_checkpoint(checkpoint_path, store.state.source),
        copy_jobs: store.state.tables.filter_map(table => {
          match table.phase {
            Pending | Copying => Some(table.oid)
            _ => None
          }
        }),
        copy_ready: @async.CondVar::Cond(),
      }
      with_destination(destination, () => {
        try {
          @async.with_task_group(group => {
            for _ in 0..<copy_concurrency {
              group.spawn_bg(() => sink.copy_worker())
            }
            sub.apply_to(sink, control~)
            group.return_immediately(())
          })
          Destination::flush(sink)
        } catch {
          PipelineStopped => Destination::flush(sink)
          e => raise e
        }
      })
    },
  )
}
