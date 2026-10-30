///|
async fn PipelineDestination::copy_worker(self : PipelineDestination) -> Unit {
  for ;; {
    while self.copy_jobs.is_empty() {
      self.copy_ready.wait()
    }
    let oid = self.copy_jobs.remove(0)
    {
      self.mutex.acquire()
      defer self.mutex.release()
      self.store.set_phase(oid, Copying)
      if self.decoding is Some(decoder) {
        decoder.tables.remove(oid)
      }
      self.copy_decoding.remove(oid)
    }
    self.copy_table(oid) catch {
      PipelineStopped => raise PipelineStopped
      PipelineSourceChanged => raise PipelineSourceChanged
      PipelineCritical(error) => raise PipelineCritical(error)
      PipelineFlushFailed(error) => raise PipelineCritical(error)
      CheckpointError(reason) => raise CheckpointError(reason)
      UnsafeResume(reason) => raise UnsafeResume(reason)
      error => {
        if @async.is_being_cancelled() || self.control.stopped {
          raise error
        }
        let attempts = self.retry_attempts.get(oid).unwrap_or(0)
        self.control.metrics_data.increment(
          "mooncdc_worker_errors_total{worker_type=\"table_sync\"}",
        )
        let retry = table_retry_policy(
          error,
          attempts,
          self.max_table_retries,
          self.retry_delay_ms,
        )
        self.mutex.acquire()
        defer self.mutex.release()
        self.store.set_phase(oid, Errored(error.to_string(), retry, attempts))
        self.copy_ready.broadcast()
      }
    }
  }
}

///|
// The destination mutex also serializes handover with main-worker projection.
// Copying stays durable until both snapshot and private WAL catchup are durable.
// A crash before this write resets the table from a new snapshot; afterwards the
// main worker excludes every transaction already covered by the saved cutoff.
async fn PipelineDestination::handover(
  self : PipelineDestination,
  oid : UInt32,
  position : Lsn,
) -> Bool {
  if position < self.main_lsn {
    return false
  }
  let decoding = match self.copy_decoding.get(oid) {
    Some(decoder) => {
      guard decoder.tables.get(oid) is Some(Complete(schema)) else {
        raise UnsafeResume("handover requires a complete decoder")
      }
      Some(StoredDecoding::capture(schema))
    }
    None => None
  }
  let phase : StoredPhase = if position == self.main_lsn {
    Ready(position.to_string())
  } else {
    Catchup(position.to_string())
  }
  self.store.update(state => {
    ..state,
    tables: state.tables.map(table => {
      if table.oid == oid {
        { ..table, phase, decoding, }
      } else {
        table
      }
    }),
  }) catch {
    error => raise PipelineCritical(error)
  }
  self.retry_attempts.remove(oid)
  self.copy_decoding.remove(oid)
  true
}

///|
async fn PipelineDestination::catch_up_table(
  self : PipelineDestination,
  exporter : Connection,
  point : SnapshotPosition,
  oid : UInt32,
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
  let wal_timeout = exporter.wal_activity_timeout(self.connection.timeout_ms)
  @async.with_timeout(self.connection.timeout_ms, () => {
    begin_replication(
      exporter,
      point.source,
      point.lsn,
      binary=false,
      messages=true,
    )
  })
  let activity = self.control.observe(
    WalApply(wal_sender_timeout_ms=wal_timeout),
    worker=TableWorker(source.slot, oid),
  )
  defer activity.close()
  if self.batch is Some(batch) && self.destination.supports_wal_batches() {
    // copy_table already owns one batch slot; add the independent receiver slot.
    batch.governor.with_slots(1UL, () => {
      self.catch_up_events(exporter, point, oid, activity, batch)
    })
    return
  }
  let decoder = Decoder::new(67108864, 100000, 4096)
  let send = @async.Mutex()
  let mut durable = point.lsn
  let mut received = point.lsn
  let feedback = async fn(request_reply : Bool) -> Unit {
    send.acquire()
    defer send.release()
    exporter.feedback(durable, request_reply~)
  }
  with_source_monitor(
    self.connection,
    activity,
    self.copy_monitor_interval_ms,
    () => (received, durable),
    () => {
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          for ;; {
            @async.sleep(1000)
            feedback(true)
          }
        })
        for ;; {
          if self.control.stopped {
            raise PipelineStopped
          }
          wait_source_capacity(self.memory, Some(activity))
          let (tag, body) = exporter.read()
          if tag == b'E' {
            raise server_error(body)
          }
          if tag != b'd' {
            raise Protocol("unexpected table catchup frame")
          }
          let labels = activity.metric_worker()
          activity.metrics.increment(
            "mooncdc_replication_messages_total{" + labels + "}",
          )
          activity.metrics.increment(
            "mooncdc_replication_payload_bytes_total{" + labels + "}",
            amount=body.length().to_uint64(),
          )
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
                feedback(false)
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
                activity.ping()
                continue
              }
              if decoder.decode(data)
                is Some((xid, commit_lsn, end_lsn, time, changes)) {
                received = end_lsn
                activity.metrics.increment(
                  "mooncdc_transactions_received_total{" + labels + "}",
                )
                activity.metrics.increment(
                  "mooncdc_changes_received_total{" + labels + "}",
                  amount=changes.length().to_uint64(),
                )
                if end_lsn <= durable {
                  activity.ping()
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
                  decoded_schemas: None,
                }
                self.mutex.acquire()
                defer self.mutex.release()
                self.store.record_write_horizon(tx.end_lsn)
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
                let projected = match self.copy_decoding.get(oid) {
                  Some(decoding) =>
                    decoding.transaction(projected, [], source.publication)
                  None => projected
                }
                let done = {
                  if !projected.changes.is_empty() {
                    let write_started = @bench.monotonic_clock_start()
                    if self.destination.write_transaction(projected) == Accepted {
                      self.destination.flush() catch {
                        error => raise PipelineCritical(error)
                      }
                    }
                    let labels = "worker_type=\"table_sync\",replication_path=\"cdc\""
                    activity.metrics.increment(
                      "mooncdc_transactions_written_total{" + labels + "}",
                    )
                    activity.metrics.increment(
                      "mooncdc_changes_written_total{" + labels + "}",
                      amount=projected.changes.length().to_uint64(),
                    )
                    activity.metrics.duration(
                      "mooncdc_private_wal_write_and_flush_seconds", write_started,
                    )
                  }
                  self.handover(oid, end_lsn)
                }
                durable = end_lsn
                activity.metrics.increment(
                  "mooncdc_transactions_durable_total{" + labels + "}",
                )
                if done {
                  break
                }
              }
            }
            _ => raise Protocol("unknown table catchup CopyData message")
          }
          activity.ping()
        }
        group.return_immediately(())
      })
    },
  )
}
