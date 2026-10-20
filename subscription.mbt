///|
priv struct Pending {
  end_lsn : Lsn
  mut delivered : Bool
  mut done : Bool
}

///|
/// A subscription lives only within `subscribe`'s callback. Do not use it after return.
pub struct Subscription {
  priv config : ConnectionConfig
  priv source : Source
  priv path : String
  priv state_session : StateSession?
  priv session : String
  priv mut connection : Connection
  priv mut durable : Lsn
  priv mut received : Lsn
  priv mut server_end : Lsn
  priv queue : Array[Transaction]
  priv wal_queue : Array[WalBatch]
  priv wal_events : Bool
  priv pending : Map[Int, Pending]
  priv ready : @async.CondVar
  priv ack_mutex : @async.Mutex
  priv send_mutex : @async.Mutex
  priv mut sequence : Int
  priv mut acknowledged : Int
  priv mut reconnects : Int
  priv mut closed : Bool
  priv max_pending : Int
  priv binary : Bool
  priv messages : Bool
  priv protocol : PgOutputProtocol
  priv mut stream_decoder : StreamDecoder?
  priv nontransactional_message : (async (LogicalMessage) -> Unit)?
  priv mut message_failed : Bool
  priv backpressure : Bool
  priv activity : ActivityHandle?
  priv mut activity_busy : Bool
  priv memory : @memory.Monitor?
}

///|
fn Subscription::observe_received(self : Subscription) -> Unit {
  // The receiver is a separate task. Its keepalives must not conceal a blocked
  // destination write or flush in the apply task.
  if !self.activity_busy {
    if self.activity is Some(activity) {
      activity.ping()
    }
  }
}

///|
/// Stop the stream and wake blocked consumers. Closing never acknowledges data.
pub fn Subscription::close(self : Subscription) -> Unit {
  self.closed = true
  self.connection.close()
  self.ready.broadcast()
}

///|
/// Wait for the next complete committed transaction. Only one task may call next at a time.
pub async fn Subscription::next(self : Subscription) -> Transaction {
  while self.queue.is_empty() && !self.closed {
    self.ready.wait()
  }
  if self.closed {
    raise Closed
  }
  let tx = self.queue.remove(0)
  self.pending[tx.sequence].delivered = true
  tx
}

///|
/// Persist a contiguous acknowledgement prefix. Call only after business persistence.
/// Later acknowledgements may be recorded without advancing the durable position.
pub async fn Subscription::ack(self : Subscription, tx : Transaction) -> Unit {
  self.ack_mutex.acquire()
  defer self.ack_mutex.release()
  if self.closed {
    raise Closed
  }
  if tx.session != self.session {
    raise InvalidConfig("transaction belongs to another subscription")
  }
  if tx.sequence <= self.acknowledged {
    return
  }
  guard self.pending.get(tx.sequence) is Some(p) && p.delivered else {
    raise InvalidConfig("transaction was not delivered")
  }
  p.done = true
  let mut count = self.acknowledged
  let mut position = self.durable
  while self.pending.get(count + 1) is Some(next) && next.done {
    count += 1
    position = next.end_lsn
  }
  if count == self.acknowledged {
    return
  }
  // No cancellation between disk commit and in-memory publication of the position.
  @async.protect_from_cancel(() => {
    store_checkpoint(self.path, self.source, position, self.state_session)
    if self.activity is Some(activity) {
      activity.metrics.increment(
        "mooncdc_transactions_durable_total{" + activity.metric_worker() + "}",
        amount=(count - self.acknowledged).to_uint64(),
      )
      activity.metrics.increment(
        "mooncdc_checkpoint_updates_total{" + activity.metric_worker() + "}",
      )
    }
    for i in (self.acknowledged + 1)..<=count {
      self.pending.remove(i)
    }
    self.acknowledged = count
    self.durable = position
    self.ready.broadcast()
  })
  // Background heartbeat is the sole periodic sender; ack need not wait for the server.
}

///|
pub fn Subscription::diagnostics(self : Subscription) -> Diagnostics {
  {
    reconnects: self.reconnects,
    received: self.received,
    durable: self.durable,
    server_end: self.server_end,
    outstanding_transactions: self.pending.length(),
    queued_transactions: self.queue.length() +
    self.wal_queue.filter(batch => batch.end_lsn is Some(_)).length(),
    wal_lag_bytes: if self.server_end > self.durable {
      self.server_end.value - self.durable.value
    } else {
      0
    },
  }
}

///|
async fn Subscription::feedback(
  self : Subscription,
  request_reply? : Bool = false,
) -> Unit {
  self.send_mutex.acquire()
  defer self.send_mutex.release()
  if !self.closed {
    @async.with_timeout(self.config.timeout_ms, () => {
      self.connection.feedback(self.durable, request_reply~)
    })
  }
}

///|
async fn Subscription::read_stream(
  self : Subscription,
  decoder : Decoder,
) -> Unit {
  self.stream_decoder = match self.protocol {
    V1 => None
    V2Streaming => Some(StreamDecoder::new(decoder, self.max_pending))
  }
  let streaming = self.stream_decoder
  for ;; {
    if self.wal_events {
      // Do not decode a second event while the single delivery slot is occupied.
      while !self.wal_queue.is_empty() {
        self.ready.wait()
        if self.closed {
          raise Closed
        }
      }
    }
    wait_source_capacity(self.memory, self.activity)
    let (tag, body) = self.connection.read()
    if tag == b'E' {
      raise server_error(body)
    }
    if tag == b'N' {
      // Notices are out-of-band protocol messages, not WAL or confirmations.
      ignore(server_error(body))
      continue
    }
    if tag == b'S' {
      let parameter = Cursor::new(body)
      ignore(parameter.cstring())
      ignore(parameter.cstring())
      parameter.done()
      continue
    }
    if tag == b'C' {
      // A caught-up walsender sends COPY 0 before exiting during shutdown.
      // It ends this stream, without acknowledging any pending transaction.
      let completed = Cursor::new(body)
      if completed.cstring() != "COPY 0" {
        raise Protocol("unexpected replication completion")
      }
      completed.done()
      if self.activity is Some(activity) {
        activity.metrics.increment(
          "mooncdc_replication_stream_ends_total{message_type=\"command_complete\"," +
          activity.metric_worker() +
          "}",
        )
      }
      raise @io.ReaderClosed
    }
    if tag == b'c' {
      // CopyDone carries no payload. Retry from the unchanged durable frontier.
      if !body.is_empty() {
        raise Protocol("invalid replication CopyDone")
      }
      if self.activity is Some(activity) {
        activity.metrics.increment(
          "mooncdc_replication_stream_ends_total{message_type=\"copy_done\"," +
          activity.metric_worker() +
          "}",
        )
      }
      raise @io.ReaderClosed
    }
    if tag != b'd' {
      raise Protocol("unexpected replication frame \{tag.to_int()}")
    }
    if self.activity is Some(activity) {
      let labels = activity.metric_worker()
      activity.metrics.increment(
        "mooncdc_replication_messages_total{" + labels + "}",
      )
      activity.metrics.increment(
        "mooncdc_replication_payload_bytes_total{" + labels + "}",
        amount=body.length().to_uint64(),
      )
    }
    let c = Cursor::new(body)
    match c.byte() {
      b'k' => {
        self.server_end = { value: c.u64(), }
        ignore(c.u64())
        let reply = c.byte()
        if reply != b'\x00' && reply != b'\x01' {
          raise Protocol("keepalive flag")
        }
        c.done()
        if reply == b'\x01' {
          self.feedback()
        }
      }
      b'w' => {
        ignore(c.u64())
        self.server_end = { value: c.u64(), }
        ignore(c.u64())
        let data = body[c.pos:].to_owned()
        if data.get(0) == Some(b'M') {
          if !self.messages {
            raise Protocol("logical message without negotiation")
          }
          if data.get(1) == Some(b'\x00') &&
            streaming.map(decoder => decoder.active is None).unwrap_or(true) {
            let message_cursor = Cursor::new(data)
            ignore(message_cursor.byte())
            let message = message_cursor.logical_message()
            message_cursor.done()
            guard self.nontransactional_message is Some(handle) else {
              raise InvalidConfig(
                "nontransactional logical message requires a handler",
              )
            }
            // Handler errors are business failures, never transport retries.
            self.message_failed = true
            handle(message)
            self.message_failed = false
            self.observe_received()
            continue
          }
        }
        if self.wal_events {
          self.receive_event(decoder.decode_event(data))
          self.observe_received()
          continue
        }
        let committed = match streaming {
          Some(stream) => stream.decode(data)
          None => decoder.decode(data)
        }
        if committed is Some((xid, commit_lsn, end_lsn, time, changes)) {
          if self.activity is Some(activity) {
            let labels = activity.metric_worker()
            activity.metrics.increment(
              "mooncdc_transactions_received_total{" + labels + "}",
            )
            activity.metrics.increment(
              "mooncdc_changes_received_total{" + labels + "}",
              amount=changes.length().to_uint64(),
            )
          }
          // A reconnect may repeat the last complete transaction already retained in memory.
          if end_lsn <= self.received {
            self.observe_received()
            continue
          }
          while self.pending.length() >= self.max_pending {
            if !self.backpressure {
              raise LimitExceeded("outstanding transaction queue")
            }
            self.ready.wait()
            if self.closed {
              raise Closed
            }
          }
          if self.sequence == 2147483647 {
            raise LimitExceeded("session sequence; reopen subscription")
          }
          self.sequence += 1
          let tx : Transaction = {
            id: "\{self.source.system_id}/\{self.source.database}/\{self.source.slot}/\{commit_lsn.to_string()}",
            xid,
            commit_lsn,
            end_lsn,
            commit_time_us: time,
            changes,
            session: self.session,
            sequence: self.sequence,
            projected_ids: None,
            decoded_schemas: None,
          }
          self.pending[self.sequence] = {
            end_lsn,
            delivered: false,
            done: false,
          }
          self.queue.push(tx)
          self.received = end_lsn
          self.ready.broadcast()
        }
      }
      _ => raise Protocol("unknown CopyData message")
    }
    self.observe_received()
  }
}

///|
async fn Subscription::receive(
  self : Subscription,
  max_bytes : Int,
  max_events : Int,
  max_metadata : Int,
  max_reconnects : Int,
  delay_ms : Int,
) -> Unit {
  for ;; {
    self.read_stream(Decoder::new(max_bytes, max_events, max_metadata)) catch {
      e => {
        if self.closed {
          raise e
        }
        if fatal(e) || self.message_failed {
          raise e
        }
        self.connection.close()
        let mut last_error = e
        for ;; {
          if self.reconnects >= max_reconnects {
            raise last_error
          }
          self.reconnects += 1
          @async.sleep(delay_ms)
          let next = try {
            let next = connect(
              self.config,
              self.connection.limit,
              true,
              metrics?=self.connection.queries.map(observer => observer.metrics),
            )
            errdefer next.close()
            @async.with_timeout(self.config.timeout_ms, () => {
              if identify(
                  next,
                  self.config,
                  self.source.slot,
                  self.source.publication,
                ) !=
                self.source {
                raise UnsafeResume("source changed during reconnect")
              }
              start_replication(
                next,
                self.source,
                self.durable,
                self.received,
                binary=self.binary,
                messages=self.messages,
                protocol=self.protocol,
              )
            })
            next
          } catch {
            e => {
              if fatal(e) {
                raise e
              }
              last_error = e
              continue
            }
          }
          self.connection = next
          break
        }
      }
    }
  }
}

///|
/// Run a bounded CDC subscription. Returning or raising from `consume` cancels all
/// background tasks and releases the socket and checkpoint lock. Queue overflow is fatal.
async fn[T] subscribe_impl(
  connection : ConnectionConfig,
  slot~ : String,
  publication~ : String,
  start~ : Start,
  checkpoint_path~ : String,
  consume : async (Subscription) -> T,
  max_frame_bytes? : Int = 16777216,
  max_transaction_bytes? : Int = 67108864,
  max_transaction_events? : Int = 100000,
  max_pending_transactions? : Int = 16,
  max_metadata_entries? : Int = 4096,
  heartbeat_ms? : Int = 1000,
  max_reconnects? : Int = 3,
  reconnect_delay_ms? : Int = 250,
  binary? : Bool = false,
  messages? : Bool = false,
  protocol? : PgOutputProtocol = V1,
  nontransactional_message? : async (LogicalMessage) -> Unit,
  backpressure? : Bool = false,
  state_session? : StateSession,
  worker_retry? : Bool = false,
  pipeline_source? : Source,
  activity_control? : ApplyControl,
  memory? : @memory.Monitor,
  batch_governor? : @memory.Governor,
  wal_events? : Bool = false,
  source_monitor_interval_ms? : Int,
) -> T {
  if wal_events && !worker_retry {
    raise InvalidConfig("WAL event delivery requires an outer worker retry")
  }
  if wal_events && protocol != V1 {
    raise InvalidConfig("WAL event delivery requires pgoutput v1")
  }
  if source_monitor_interval_ms is Some(interval) &&
    (interval <= 0 || activity_control is None) {
    raise InvalidConfig(
      "source monitor requires positive interval and activity control",
    )
  }
  let lock = record_lock(checkpoint_path, state_session)
  defer (if lock is Some(lock) { lock.close() })
  let worker = if worker_retry { Some(ApplyWorker(slot)) } else { None }
  let conn = source_operation(
    () => {
      connect(
        connection,
        max_frame_bytes,
        true,
        worker?,
        metrics?=activity_control.map(control => control.metrics_data),
      )
    },
    retry=worker_retry,
  )
  errdefer conn.close()
  let source = source_operation(
    () => {
      @async.with_timeout(connection.timeout_ms, () => {
        identify(conn, connection, slot, publication)
      })
    },
    retry=worker_retry,
  )
  if pipeline_source is Some(expected) &&
    source.timeline != expected.timeline &&
    { ..source, timeline: expected.timeline, } == expected {
    // Close this connection before slot validation, target delivery or feedback.
    // The outer pipeline must join every worker and prove the timeline transition.
    raise PipelineSourceChanged
  }
  let position = match start {
    At(lsn) => {
      if lsn.value == 0 {
        raise UnsafeResume("explicit nonzero start LSN required")
      }
      if load_record(checkpoint_path, ReplicationCheckpoint, state_session)
        is Some(_) {
        raise CheckpointError("checkpoint exists; use Resume")
      }
      lsn
    }
    AfterSnapshot(snapshot) => {
      if snapshot.source != source {
        raise UnsafeResume("snapshot source/slot/publication mismatch")
      }
      if load_record(checkpoint_path, ReplicationCheckpoint, state_session)
        is Some(_) {
        raise CheckpointError("checkpoint exists; use Resume")
      }
      snapshot.lsn
    }
    Resume => stored_checkpoint(checkpoint_path, source, state_session)
  }
  let wal_timeout = if activity_control is Some(_) {
    conn.wal_activity_timeout(connection.timeout_ms)
  } else {
    60000UL
  }
  source_operation(
    () => {
      @async.with_timeout(connection.timeout_ms, () => {
        start_replication(
          conn,
          source,
          position,
          position,
          binary~,
          messages~,
          protocol~,
        )
      })
    },
    retry=worker_retry,
  )
  if start is (At(_) | AfterSnapshot(_)) {
    store_checkpoint(checkpoint_path, source, position, state_session)
  }
  let activity = activity_control.map(control => {
    control.observe(WalApply(wal_sender_timeout_ms=wal_timeout), worker?)
  })
  defer (if activity is Some(activity) { activity.close() })
  guard @env.rand(24) is Some(random) else {
    raise Authentication("secure entropy unavailable")
  }
  let session = @base64.encode(random)
  let sub : Subscription = {
    config: connection,
    source,
    path: checkpoint_path,
    state_session,
    session,
    connection: conn,
    durable: position,
    received: position,
    server_end: position,
    queue: [],
    wal_queue: [],
    wal_events,
    pending: Map([]),
    ready: @async.CondVar::Cond(),
    ack_mutex: @async.Mutex(),
    send_mutex: @async.Mutex(),
    sequence: 0,
    acknowledged: 0,
    reconnects: 0,
    closed: false,
    max_pending: max_pending_transactions,
    binary,
    messages,
    protocol,
    stream_decoder: None,
    nontransactional_message,
    message_failed: false,
    backpressure,
    activity,
    activity_busy: false,
    memory,
  }
  if activity is Some(activity) {
    activity.metrics.subscriptions[activity.id] = (
      activity.worker_type,
      () => sub.diagnostics(),
    )
  }
  defer sub.close()
  let run = async fn() -> T {
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        sub.receive(
          max_transaction_bytes,
          max_transaction_events,
          max_metadata_entries,
          if worker_retry {
            0
          } else {
            max_reconnects
          },
          reconnect_delay_ms,
        ) catch {
          error =>
            if worker_retry && !sub.message_failed {
              raise ApplySourceFailed(error)
            } else {
              raise error
            }
        }
      })
      group.spawn_bg(() => {
        for ;; {
          @async.sleep(heartbeat_ms)
          sub.feedback(request_reply=activity is Some(_)) catch {
            _ =>
              // Closing wakes the reader, which owns the bounded reconnect policy.
              sub.connection.close()
          }
        }
      })
      let result = consume(sub)
      group.return_immediately(result)
      result
    })
  }
  let monitored = async fn() -> T {
    match source_monitor_interval_ms {
      Some(interval) =>
        with_source_monitor(
          connection,
          activity.unwrap(),
          interval,
          () => (sub.received, sub.durable),
          run,
        )
      None => run()
    }
  }
  match batch_governor {
    Some(governor) =>
      governor.with_slots(
        if wal_events {
          2UL
        } else {
          max_pending_transactions.to_uint64() + 1UL
        },
        monitored,
      )
    None => monitored()
  }
}

///|
fn fatal(error : Error) -> Bool {
  match error {
    Server("57P01", _) | Server("57P02", _) | Server("57P03", _) => false
    Protocol(_)
    | Authentication(_)
    | Server(_, _)
    | InvalidConfig(_)
    | LimitExceeded(_)
    | UnsafeResume(_)
    | CheckpointError(_)
    | Closed => true
    _ => false
  }
}
