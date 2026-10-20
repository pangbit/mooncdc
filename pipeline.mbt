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
  memory : @memory.Monitor?
  batch : BatchRuntime?
  batch_rows : Int
  copy_connections_per_table : Int
  copy_monitor_interval_ms : Int
  table_sync_copy : TableSyncCopy
  slot_failover : Bool
  schema_store : &SchemaStore?
  mut frontier : Lsn?
  mut main_lsn : Lsn
  copy_jobs : Array[UInt]
  copy_ready : @async.CondVar
  retry_delay_ms : Int
  max_table_retries : Int
  retry_attempts : Map[UInt, Int]
  cleanup_candidates : Map[UInt, Bool]
  mut decoding : PipelineDecoding?
  copy_decoding : Map[UInt, PipelineDecoding]
}

///|
impl Destination for PipelineDestination with fn bind_snapshot_schema(
  self,
  relation,
  schema,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.control.stopped {
    raise PipelineStopped
  }
  self.store.record_write_horizon(schema.id.commit_lsn)
  self.destination.bind_snapshot_schema(relation, schema)
  if self.schema_store is Some(schemas) {
    let decoder = PipelineDecoding::new(schemas, schema.id.commit_lsn)
    decoder.tables[relation.oid] = Complete(schema.project(relation))
    self.copy_decoding[relation.oid] = decoder
  }
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
// Forward the TableEnd lifetime barrier before snapshot/private-WAL
// handover. Classify its failure like flush: the copy worker cannot retry it as
// a table-local success while the main worker continues past unknown durability.
impl Destination for PipelineDestination with fn finish_table_copy(
  self,
  relation,
) {
  self.mutex.acquire()
  defer self.mutex.release()
  if self.control.stopped {
    raise PipelineStopped
  }
  self.destination.finish_table_copy(relation) catch {
    error => raise PipelineFlushFailed(error)
  }
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
  if self.batch is Some(batch) {
    batch.governor.with_slots(1UL, () => self.copy_table_with_slot(oid))
  } else {
    self.copy_table_with_slot(oid)
  }
}

///|
async fn PipelineDestination::copy_table_with_slot(
  self : PipelineDestination,
  oid : UInt,
) -> Unit {
  if self.control.stopped {
    raise PipelineStopped
  }
  let source = self.store.state.source
  let slot = if self.slot_failover {
    cleanup_copy_slot(self.connection, source, oid, pipeline_source_change=true)
    failover_copy_slot(source, oid)
  } else {
    copy_slot_prefix(source.slot) + pipeline_nonce()
  }
  defer (if self.slot_failover {
    @async.protect_from_cancel(() => {
      cleanup_copy_slot_on_exit(self.connection, source, oid)
    })
  })
  with_copy_monitor(
    self.connection,
    slot,
    self.copy_monitor_interval_ms,
    self.control.metrics_data,
    on_snapshot => {
      ignore(
        copy_snapshot_to_impl(
          self.connection,
          self,
          slot~,
          publication=self.store.state.source.publication,
          batch_rows=self.batch_rows,
          copy_connections=self.copy_connections_per_table,
          max_frame_bytes=16777216,
          max_batch_bytes=67108864,
          temporary=!self.slot_failover,
          slot_failover=self.slot_failover,
          worker=TableWorker(source.slot, oid),
          table=Some(oid),
          schema_store?=self.schema_store,
          activity_control=self.control,
          memory?=self.memory,
          batch?=self.batch,
          copy_rows=self.table_sync_copy.should_copy_table(oid),
          after_copy=(exporter, point) => {
            self.catch_up_table(exporter, point, oid)
          },
          on_snapshot~,
          before_snapshot_schema=point => {
            self.mutex.acquire()
            defer self.mutex.release()
            if { ..point.source, slot: source.slot, } != source {
              raise UnsafeResume("source changed during table copy")
            }
            self.store.record_write_horizon(point.lsn)
          },
        ),
      )
    },
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
    decoded_schemas: None,
  }
}

///|
impl Destination for PipelineDestination with fn write_transaction(self, tx) {
  self.write_events(tx, None)
}

///|
impl Destination for PipelineDestination with fn supports_wal_batches(self) {
  self.destination.supports_wal_batches()
}

///|
impl Destination for PipelineDestination with fn write_wal_batch(self, batch) {
  self.write_events(batch.internal_events(), Some(batch))
}

///|
async fn PipelineDestination::write_events(
  self : PipelineDestination,
  tx : Transaction,
  wal_batch : WalBatch?,
) -> WriteStatus {
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
      let conn = source_operation(() => {
        connect(
          self.connection,
          16777216,
          false,
          metrics=self.control.metrics_data,
        )
      })
      defer conn.close()
      if source_operation(() => {
          publication_tables(
            conn,
            self.store.state.source.publication,
            self.connection.timeout_ms,
          )
        }).contains(schema.oid) {
        missing[schema.oid] = true
      }
    }
  }
  if !missing.is_empty() {
    let conn = source_operation(() => {
      connect(
        self.connection,
        16777216,
        false,
        metrics=self.control.metrics_data,
      )
    })
    defer conn.close()
    let current = source_operation(() => {
      publication_tables(
        conn,
        self.store.state.source.publication,
        self.connection.timeout_ms,
      )
    })
    for oid, _ in missing {
      if !current.contains(oid) {
        raise UnsafeResume("publication changed during table discovery")
      }
      self.store.update(state => {
        ..state,
        tables: state.tables + [{ oid, phase: Pending, decoding: None, }],
      })
      self.copy_jobs.push(oid)
      self.copy_ready.broadcast()
    }
  }
  self.mutex.acquire()
  defer self.mutex.release()
  self.store.record_write_horizon(wal_write_horizon(tx, wal_batch))
  let projected = self.store.project(tx)
  let projected = match self.decoding {
    Some(decoding) =>
      decoding.transaction(
        projected,
        self.store.state.tables,
        self.store.state.source.publication,
      )
    None => projected
  }
  for change in projected.changes {
    for relation in change_relations(change) {
      self.cleanup_candidates[relation.oid] = true
    }
  }
  if self.schema_store is Some(_) {
    for change in projected.changes {
      if change is Message(message) &&
        message.schema_snapshot(
          commit_lsn=tx.commit_lsn,
          publication=self.store.state.source.publication,
        )
        is Some(schema) {
        self.cleanup_candidates[schema.oid] = true
      }
    }
  }
  let status = try {
    match wal_batch {
      Some(batch) =>
        self.destination.write_wal_batch(batch.with_events(projected))
      None => self.destination.write_transaction(projected)
    }
  } catch {
    error => raise ApplyWriteFailed(error)
  }
  let boundary = match wal_batch {
    Some(batch) => batch.end_lsn
    None => Some(tx.end_lsn)
  }
  if boundary is Some(end_lsn) {
    self.main_lsn = end_lsn
    self.frontier = Some(end_lsn)
  }
  if status == Durable {
    if self.frontier is Some(frontier) {
      self.store.advance_tables(frontier)
      self.frontier = None
    }
  }
  status
}

///|
impl Destination for PipelineDestination with fn flush(self) {
  self.mutex.acquire()
  defer self.mutex.release()
  self.destination.flush() catch {
    e => raise PipelineFlushFailed(e)
  }
  if self.frontier is Some(lsn) {
    self.store.advance_tables(lsn) catch {
      error => raise PipelineCritical(error)
    }
    self.frontier = None
  }
}

///|
/// Persist table synchronization across restarts. Owns a randomly named main slot
/// under slot_prefix and temporary per-table copy slots. Keep state and checkpoint
/// files together; source/destination identity changes require a new state path.
async fn run_pipeline_impl(
  connection : ConnectionConfig,
  destination : &Destination,
  publication~ : String,
  destination_id~ : String,
  state_path~ : String,
  control~ : ApplyControl,
  slot_prefix? : String = "mooncdc",
  copy_concurrency? : Int = 2,
  copy_connections_per_table? : Int = 1,
  table_sync_monitor_refresh_interval_ms? : Int = 10000,
  batch_rows? : Int = 256,
  schema_store? : &SchemaStore,
  table_retry_delay_ms? : Int = 10000,
  max_table_retries? : Int = 5,
  table_sync_copy? : TableSyncCopy = IncludeAllTables,
  slot_failover? : Bool = false,
  invalidated_slot_behavior? : InvalidatedSlotBehavior = RejectInvalidatedSlot,
  reuse_destination? : Bool = false,
  session? : StateSession,
  memory? : @memory.Monitor,
  batch? : BatchRuntime,
) -> Unit {
  let lock = record_lock(state_path, session)
  defer (if lock is Some(lock) { lock.close() })
  if control.pipeline_active {
    raise InvalidConfig("control already belongs to a running pipeline")
  }
  control.pipeline_active = true
  control.lifecycle = if control.stopped { Stopping } else { Initializing }
  control.metrics_data.memory = memory
  control.metrics_data.batch = batch
  defer {
    control.metrics_data.pipeline = None
    control.metrics_data.memory = None
    control.metrics_data.batch = None
    control.pipeline_active = false
    control.lifecycle = Stopping
  }
  let checkpoint_path = state_path + ".checkpoint"
  let retry = ApplyRetry::new(control, table_retry_delay_ms, max_table_retries)
  guard retry.run(() => {
      let conn = source_operation(() => {
        connect(connection, 16777216, true, metrics=control.metrics_data)
      })
      defer conn.close()
      let stored = load_record(state_path, Pipeline, session)
      let exists = stored is Some(_)
      let state = if stored is Some(bytes) {
        parse_pipeline_state(bytes)
      } else {
        if load_record(checkpoint_path, ReplicationCheckpoint, session)
          is Some(_) {
          raise CheckpointError("checkpoint without pipeline state")
        }
        let slot = slot_prefix + "_" + pipeline_nonce()
        let source = source_operation(() => {
          @async.with_timeout(connection.timeout_ms, () => {
            identify(conn, connection, slot, publication)
          })
        })
        PipelineState::{
          version: 1,
          source,
          destination_id,
          initial_lsn: None,
          checkpoint_initialized: false,
          write_horizon: None,
          promotion: None,
          tables: [],
        }
      }
      let store : PipelineStore = {
        path: state_path,
        session,
        state,
        mutex: @async.Mutex(),
      }
      let actual = source_operation(() => {
        @async.with_timeout(connection.timeout_ms, () => {
          identify(conn, connection, state.source.slot, publication)
        })
      })
      if destination_id != state.destination_id ||
        !state.source.slot.has_prefix(slot_prefix + "_") {
        raise UnsafeResume(
          "pipeline source, destination or slot prefix changed",
        )
      }
      if actual != state.source || store.state.promotion is Some(_) {
        if !slot_failover {
          raise UnsafeResume(
            "pipeline timeline changed; failover recovery is disabled",
          )
        }
        source_operation(() => {
          @async.with_timeout(connection.timeout_ms, () => {
            store.recover_promotion(conn, actual)
          })
        })
      }
      let state = store.state
      if store.state.version == 2 &&
        invalidated_slot_behavior != RecreateInvalidatedSlot {
        raise UnsafeResume(
          "pending slot recreation requires explicit recreate behavior",
        )
      }
      let needs_recreation = store.state.version == 2 ||
        (
          invalidated_slot_behavior == RecreateInvalidatedSlot &&
          store.state.initial_lsn is Some(_) &&
          source_operation(() => {
            @async.with_timeout(connection.timeout_ms, () => {
              pipeline_slot_invalidated(conn, state.source)
            })
          })
        )
      let tables = source_operation(() => {
        @async.with_timeout(connection.timeout_ms, () => {
          if slot_failover {
            require_slot_failover(conn)
          }
          cleanup_copy_slots(
            conn,
            state.source,
            state.tables.map(table => table.oid),
          )
        })
        publication_tables(conn, publication, connection.timeout_ms)
      })
      if needs_recreation {
        control.metrics_data.increment("mooncdc_slot_recreation_attempts_total")
        source_operation(() => {
          @async.with_timeout(connection.timeout_ms, () => {
            store.recreate_slot(conn, slot_failover)
          })
        })
      }
      if !exists {
        store.update(state => state)
      }
      if store.state.initial_lsn is None {
        if load_record(checkpoint_path, ReplicationCheckpoint, session)
          is Some(_) {
          raise CheckpointError("uninitialized state has a checkpoint")
        }
        // Only this durable creation intent can own this random slot. No table writes
        // occur before its initial position and checkpoint have both been persisted.
        let lsn = source_operation(() => {
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
            ignore(conn.query("SET lock_timeout=0"))
            let created = conn.query(
              create_slot_sql(
                slot,
                temporary=false,
                export_snapshot=false,
                failover=slot_failover,
              ),
            )
            ignore(conn.query("SET lock_timeout=30000"))
            if cell(created, 0, 0) != slot || cell(created, 0, 3) != "pgoutput" {
              raise Protocol("pipeline slot creation metadata")
            }
            Lsn::parse(cell(created, 0, 1))
          })
        })
        store.update(state => {
          ..state,
          initial_lsn: Some(lsn.to_string()),
          write_horizon: Some(lsn.to_string()),
        })
      }
      if slot_failover {
        source_operation(() => {
          @async.with_timeout(connection.timeout_ms, () => {
            ensure_slot_failover(conn, state.source)
          })
        })
      }
      if !store.state.checkpoint_initialized {
        let checkpoint_lock = record_lock(checkpoint_path, session)
        defer (if checkpoint_lock is Some(lock) { lock.close() })
        let position = Lsn::parse(store.state.initial_lsn.unwrap())
        if load_record(checkpoint_path, ReplicationCheckpoint, session)
          is Some(_) {
          if stored_checkpoint(checkpoint_path, state.source, session) !=
            position {
            raise CheckpointError("unexpected initial pipeline checkpoint")
          }
        } else {
          store_checkpoint(checkpoint_path, state.source, position, session)
        }
        store.update(state => { ..state, checkpoint_initialized: true, })
      } else if load_record(checkpoint_path, ReplicationCheckpoint, session)
        is None {
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
            previous
            .get(oid)
            .unwrap_or(StoredTable::{ oid, phase: Pending, decoding: None, })
          }),
        }
      })
      if schema_store is Some(schemas) {
        let checkpoint = stored_checkpoint(
          checkpoint_path,
          store.state.source,
          session,
        )
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
              if table.decoding is Some(decoding) {
                ignore(decoding.restore(schemas, table.oid, cutoff))
              }
            }
            _ => ()
          }
        }
      } else if store.state.tables.any(table => table.decoding is Some(_)) {
        raise UnsafeResume(
          "handover recovery requires the original SchemaStore",
        )
      }
      conn.close()
      store
    })
    is Some(store) else {
    return
  }
  let state = store.state
  control.metrics_data.pipeline = Some(store)
  let sink : PipelineDestination = {
    destination,
    copy_monitor_interval_ms: table_sync_monitor_refresh_interval_ms,
    mutex: @async.Mutex(),
    store,
    connection,
    control,
    memory,
    batch,
    batch_rows,
    copy_connections_per_table,
    table_sync_copy,
    slot_failover,
    schema_store,
    frontier: None,
    main_lsn: stored_checkpoint(checkpoint_path, store.state.source, session),
    copy_jobs: store.state.tables.filter_map(table => {
      match table.phase {
        Pending | Copying => Some(table.oid)
        _ => None
      }
    }),
    copy_ready: @async.CondVar::Cond(),
    retry_delay_ms: table_retry_delay_ms,
    max_table_retries,
    retry_attempts: Map([]),
    cleanup_candidates: Map([]),
    decoding: None,
    copy_decoding: Map([]),
  }
  if control.table_retry is Some(_) {
    raise InvalidConfig("control already belongs to a running pipeline")
  }
  control.table_retry = Some(oid => sink.retry_table(oid))
  defer {
    control.table_retry = None
  }
  if !reuse_destination && session is Some(session) {
    destination.bind_state(session)
  }
  let mut workers_started = false
  // Startup is deferred until the first subscription has validated the slot.
  // Retry attempts reuse this destination lifetime and the existing copy pool.
  @async.with_task_group(group => {
    ignore(
      retry.run(() => {
        subscribe_impl(
          connection,
          slot=state.source.slot,
          publication~,
          start=Resume,
          checkpoint_path~,
          state_session?=session,
          worker_retry=true,
          pipeline_source?=if slot_failover { Some(state.source) } else { None },
          activity_control=control,
          source_monitor_interval_ms=table_sync_monitor_refresh_interval_ms,
          memory?,
          batch_governor?=batch.map(batch => batch.governor),
          wal_events=batch is Some(_) && destination.supports_wal_batches(),
          backpressure=true,
          messages=true,
          nontransactional_message=message => {
            if message.prefix == "supabase_etl_ddl" {
              raise Protocol("DDL message must be transactional")
            }
          },
          sub => {
            {
              sink.mutex.acquire()
              defer sink.mutex.release()
              sink.decoding = schema_store.map(schemas => {
                PipelineDecoding::new(schemas, sub.diagnostics().durable)
              })
            }
            // Validate the main slot before copy workers can mutate the target.
            // They stay alive across subsequent main apply attempts.
            if !workers_started {
              if !reuse_destination {
                destination.startup()
              }
              control.lifecycle = if control.stopped {
                Stopping
              } else {
                Running
              }
              for _ in 0..<copy_concurrency {
                group.spawn_bg(() => sink.copy_worker())
              }
              group.spawn_bg(() => sink.schedule_retries())
              workers_started = true
            }
            if sub.wal_events {
              sub.apply_events(sink, control~, batch=batch.unwrap(), on_checkpoint=() => {
                sink.cleanup_schemas()
              })
            } else {
              sub.apply_to_impl(sink, control~, batch?, on_checkpoint=() => {
                sink.cleanup_schemas()
              })
            }
          },
        )
      }),
    )
    group.return_immediately(())
  }) catch {
    PipelineStopped => ()
    PipelineSourceChanged if slot_failover => {
      // Every old source worker has joined. Preserve accepted target work before
      // startup checks its durable horizon against the new timeline's fork.
      if workers_started || reuse_destination {
        destination.flush()
      }
      raise PipelinePromotionRequested(workers_started || reuse_destination)
    }
    UnsafeResume("replication slot invalidated") if invalidated_slot_behavior ==
      RecreateInvalidatedSlot => {
      // The group has closed every source worker. Drain target ownership before
      // re-entering startup and resetting tables from a fresh snapshot.
      if workers_started || reuse_destination {
        destination.flush()
      }
      raise PipelineRecreateRequested(workers_started || reuse_destination)
    }
    PipelineCritical(error) | PipelineFlushFailed(error) => raise error
    e => raise e
  }
  if workers_started || reuse_destination {
    Destination::flush(sink) catch {
      PipelineCritical(error) | PipelineFlushFailed(error) => raise error
      error => raise error
    }
    destination.shutdown()
  }
}
