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
  priv session : String
  priv mut connection : Connection
  priv mut durable : Lsn
  priv mut received : Lsn
  priv mut server_end : Lsn
  priv queue : Array[Transaction]
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
    save_checkpoint(self.path, self.source, position)
    for i in (self.acknowledged + 1)..<=count {
      self.pending.remove(i)
    }
    self.acknowledged = count
    self.durable = position
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
    queued_transactions: self.queue.length(),
    wal_lag_bytes: if self.server_end > self.durable {
      self.server_end.value - self.durable.value
    } else {
      0
    },
  }
}

///|
async fn Subscription::feedback(self : Subscription) -> Unit {
  self.send_mutex.acquire()
  defer self.send_mutex.release()
  if !self.closed {
    @async.with_timeout(self.config.timeout_ms, () => {
      self.connection.feedback(self.durable)
    })
  }
}

///|
async fn Subscription::read_stream(
  self : Subscription,
  decoder : Decoder,
) -> Unit {
  for ;; {
    let (tag, body) = self.connection.read()
    if tag == b'E' {
      raise server_error(body)
    }
    if tag != b'd' {
      raise Protocol("unexpected replication frame")
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
        if decoder.decode(body[c.pos:].to_owned())
          is Some((xid, commit_lsn, end_lsn, time, changes)) {
          // A reconnect may repeat the last complete transaction already retained in memory.
          if end_lsn <= self.received {
            continue
          }
          if self.pending.length() >= self.max_pending {
            raise LimitExceeded("outstanding transaction queue")
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
        if fatal(e) {
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
            let next = connect(self.config, self.connection.limit, true)
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
pub async fn[T] subscribe(
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
) -> T {
  identifier(slot)
  identifier(publication)
  if max_frame_bytes < 64 ||
    max_frame_bytes > 1073741824 ||
    max_transaction_bytes < max_frame_bytes ||
    max_transaction_events <= 0 ||
    max_pending_transactions <= 0 ||
    max_metadata_entries <= 0 ||
    heartbeat_ms <= 0 ||
    max_reconnects < 0 ||
    reconnect_delay_ms < 0 ||
    checkpoint_path.is_empty() {
    raise InvalidConfig("invalid resource limit, interval or checkpoint path")
  }
  let lock = @fs.open(
    checkpoint_path + ".lock",
    mode=ReadWrite,
    create_mode=OpenOrCreate,
    permission=0o600,
  )
  defer lock.close()
  if !lock.try_lock(Exclusive) {
    raise CheckpointError("checkpoint already in use")
  }
  let conn = connect(connection, max_frame_bytes, true)
  errdefer conn.close()
  let source = @async.with_timeout(connection.timeout_ms, () => {
    identify(conn, connection, slot, publication)
  })
  let position = match start {
    At(lsn) => {
      if lsn.value == 0 {
        raise UnsafeResume("explicit nonzero start LSN required")
      }
      if @fs.exists(checkpoint_path) {
        raise CheckpointError("checkpoint exists; use Resume")
      }
      lsn
    }
    Resume => load_checkpoint(checkpoint_path, source)
  }
  @async.with_timeout(connection.timeout_ms, () => {
    start_replication(conn, source, position, position, binary~)
  })
  if start is At(_) {
    save_checkpoint(checkpoint_path, source, position)
  }
  guard @env.rand(24) is Some(random) else {
    raise Authentication("secure entropy unavailable")
  }
  let session = @base64.encode(random)
  let sub : Subscription = {
    config: connection,
    source,
    path: checkpoint_path,
    session,
    connection: conn,
    durable: position,
    received: position,
    server_end: position,
    queue: [],
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
  }
  defer sub.close()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      sub.receive(
        max_transaction_bytes, max_transaction_events, max_metadata_entries, max_reconnects,
        reconnect_delay_ms,
      )
    })
    group.spawn_bg(() => {
      for ;; {
        @async.sleep(heartbeat_ms)
        sub.feedback() catch {
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
