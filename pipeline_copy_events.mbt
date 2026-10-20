///|
priv struct PrivateWalDestination {
  pipeline : PipelineDestination
  point : SnapshotPosition
  oid : UInt
}

///|
impl Destination for PrivateWalDestination with fn supports_wal_batches(_self) {
  true
}

///|
impl Destination for PrivateWalDestination with fn reset_table(
  _self,
  _relation,
  attempt~,
) {
  ignore(attempt)
  raise InvalidConfig("private WAL destination does not copy rows")
}

///|
impl Destination for PrivateWalDestination with fn write_rows(
  _self,
  _relation,
  _rows,
  _id,
) {
  raise InvalidConfig("private WAL destination does not copy rows")
}

///|
impl Destination for PrivateWalDestination with fn write_transaction(_self, _tx) {
  raise InvalidConfig("private WAL destination requires event batches")
}

///|
impl Destination for PrivateWalDestination with fn write_wal_batch(self, batch) {
  let pipeline = self.pipeline
  let source = pipeline.store.state.source
  let tx = batch.internal_events()
  pipeline.mutex.acquire()
  defer pipeline.mutex.release()
  pipeline.store.record_write_horizon(wal_write_horizon(tx, Some(batch)))
  let selected = project_tables(
    tx,
    Map([(self.oid, self.point.lsn)]),
    source.publication,
    table_only=true,
  )
  let projected = { ..selected, id: tx.id + "/copy/" + self.point.source.slot, }
  let projected = match pipeline.copy_decoding.get(self.oid) {
    Some(decoding) => decoding.transaction(projected, [], source.publication)
    None => projected
  }
  pipeline.destination.write_wal_batch(batch.with_events(projected))
}

///|
impl Destination for PrivateWalDestination with fn flush(self) {
  self.pipeline.mutex.acquire()
  defer self.pipeline.mutex.release()
  self.pipeline.destination.flush() catch {
    error => raise PipelineCritical(error)
  }
}

///|
async fn PipelineDestination::catch_up_events(
  self : PipelineDestination,
  exporter : Connection,
  point : SnapshotPosition,
  oid : UInt,
  activity : ActivityHandle,
  batch : BatchRuntime,
) -> Unit {
  let sub : Subscription = {
    config: self.connection,
    source: self.store.state.source,
    path: "",
    state_session: None,
    session: point.source.slot,
    connection: exporter,
    durable: point.lsn,
    received: point.lsn,
    server_end: point.lsn,
    queue: [],
    wal_queue: [],
    wal_events: true,
    pending: Map([]),
    ready: @async.CondVar::Cond(),
    ack_mutex: @async.Mutex(),
    send_mutex: @async.Mutex(),
    sequence: 0,
    acknowledged: 0,
    reconnects: 0,
    closed: false,
    max_pending: 16,
    binary: false,
    messages: true,
    protocol: V1,
    stream_decoder: None,
    nontransactional_message: Some(message => {
      if message.prefix == "supabase_etl_ddl" {
        raise Protocol("DDL message must be transactional")
      }
    }),
    message_failed: false,
    backpressure: true,
    activity: Some(activity),
    activity_busy: false,
    memory: self.memory,
  }
  defer sub.close()
  let sink : PrivateWalDestination = { pipeline: self, point, oid, }
  activity.metrics.subscriptions[activity.id] = (
    "table_sync",
    () => sub.diagnostics(),
  )
  with_source_monitor(
    self.connection,
    activity,
    self.copy_monitor_interval_ms,
    () => (sub.received, sub.durable),
    () => {
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          sub.read_stream(Decoder::new(67108864, 100000, 4096))
        })
        group.spawn_bg(() => {
          for ;; {
            @async.sleep(1000)
            sub.feedback(request_reply=true)
          }
        })
        sub.apply_events(sink, control=self.control, batch~, on_commit=fragment => {
          guard fragment.end_lsn is Some(end_lsn) else {
            raise Protocol("private WAL missing Commit")
          }
          let done = {
            self.mutex.acquire()
            defer self.mutex.release()
            self.handover(oid, end_lsn)
          }
          sub.durable = end_lsn
          sub.pending.remove(fragment.sequence)
          sub.ready.broadcast()
          activity.metrics.increment(
            "mooncdc_transactions_durable_total{" +
            activity.metric_worker() +
            "}",
          )
          done
        })
        if self.control.stopped {
          raise PipelineStopped
        }
        group.return_immediately(())
      })
    },
  )
}
