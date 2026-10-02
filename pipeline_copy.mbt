///|
async fn PipelineDestination::copy_worker(self : PipelineDestination) -> Unit {
  for ;; {
    while self.copy_jobs.is_empty() {
      self.copy_ready.wait()
    }
    let oid = self.copy_jobs.remove(0)
    self.copy_table(oid)
  }
}

///|
// The destination mutex also serializes handover with main-worker projection.
// Copying stays durable until both snapshot and private WAL catchup are durable.
// A crash before this write resets the table from a new snapshot; afterwards the
// main worker excludes every transaction already covered by the saved cutoff.
async fn PipelineDestination::handover(
  self : PipelineDestination,
  oid : UInt,
  position : Lsn,
) -> Bool {
  if position < self.main_lsn {
    return false
  }
  self.store.set_phase(
    oid,
    if position == self.main_lsn {
      Ready(position.to_string())
    } else {
      Catchup(position.to_string())
    },
  )
  true
}

///|
async fn PipelineDestination::catch_up_table(
  self : PipelineDestination,
  exporter : Connection,
  point : SnapshotPosition,
  oid : UInt,
) -> Unit {
  let source = self.store.state.source
  if { ..point.source, slot: source.slot, } != source {
    raise UnsafeResume("source changed during table copy")
  }
  if point.lsn < Lsn::parse(self.store.state.initial_lsn.unwrap()) {
    raise UnsafeResume("table snapshot precedes pipeline slot")
  }
  let done = {
    self.mutex.acquire()
    defer self.mutex.release()
    self.handover(oid, point.lsn)
  }
  if done {
    return
  }
  @async.with_timeout(self.connection.timeout_ms, () => {
    begin_replication(
      exporter,
      point.source,
      point.lsn,
      binary=false,
      messages=true,
    )
  })
  let decoder = Decoder::new(67108864, 100000, 4096)
  let send = @async.Mutex()
  let mut durable = point.lsn
  let feedback = async fn() -> Unit {
    send.acquire()
    defer send.release()
    exporter.feedback(durable)
  }
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      for ;; {
        @async.sleep(1000)
        feedback()
      }
    })
    for ;; {
      if self.control.stopped {
        raise PipelineStopped
      }
      let (tag, body) = exporter.read()
      if tag == b'E' {
        raise server_error(body)
      }
      if tag != b'd' {
        raise Protocol("unexpected table catchup frame")
      }
      let cursor = Cursor::new(body)
      match cursor.byte() {
        b'k' => {
          ignore(cursor.u64())
          ignore(cursor.u64())
          let reply = cursor.byte()
          if reply != b'\x00' && reply != b'\x01' {
            raise Protocol("table catchup keepalive flag")
          }
          cursor.done()
          if reply == b'\x01' {
            feedback()
          }
        }
        b'w' => {
          ignore(cursor.u64())
          ignore(cursor.u64())
          ignore(cursor.u64())
          let data = body[cursor.pos:].to_owned()
          if data.get(0) == Some(b'M') && data.get(1) == Some(b'\x00') {
            let message_cursor = Cursor::new(data)
            ignore(message_cursor.byte())
            let message = message_cursor.logical_message()
            message_cursor.done()
            if message.prefix == "supabase_etl_ddl" {
              raise Protocol("DDL message must be transactional")
            }
            continue
          }
          if decoder.decode(data)
            is Some((xid, commit_lsn, end_lsn, time, changes)) {
            if end_lsn <= durable {
              continue
            }
            let tx : Transaction = {
              id: "\{source.system_id}/\{source.database}/\{source.slot}/\{commit_lsn.to_string()}",
              xid,
              commit_lsn,
              end_lsn,
              commit_time_us: time,
              changes,
              session: point.source.slot,
              sequence: 0,
              projected_ids: None,
            }
            let selected = project_tables(
              tx,
              Map([(oid, point.lsn)]),
              source.publication,
              table_only=true,
            )
            // A private worker delivers a different transaction projection from
            // the main worker. Event IDs still refer to the original WAL events.
            let projected = {
              ..selected,
              id: tx.id + "/copy/" + point.source.slot,
            }
            let done = {
              self.mutex.acquire()
              defer self.mutex.release()
              if self.schema_store is Some(store) {
                for change in projected.changes {
                  if change is Message(message) &&
                    message.schema_snapshot(
                      commit_lsn~,
                      publication=source.publication,
                    )
                    is Some(schema) {
                    store.put(schema)
                  }
                }
              }
              if !projected.changes.is_empty() {
                if self.destination.write_transaction(projected) == Accepted {
                  self.destination.flush()
                }
              }
              self.handover(oid, end_lsn)
            }
            durable = end_lsn
            if done {
              break
            }
          }
        }
        _ => raise Protocol("unknown table catchup CopyData message")
      }
    }
    group.return_immediately(())
  })
}
