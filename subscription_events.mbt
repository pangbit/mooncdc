///|
async fn Subscription::receive_event(
  self : Subscription,
  event : DecodedWalEvent,
) -> Unit {
  let header = match event {
    Begin(header) | Change(header, _, _) | Commit(header, _) => header
  }
  if self.activity is Some(activity) {
    let labels = activity.metric_worker()
    match event {
      Commit(_, _) =>
        activity.metrics.increment(
          "mooncdc_transactions_received_total{" + labels + "}",
        )
      Change(_, _, _) =>
        activity.metrics.increment(
          "mooncdc_changes_received_total{" + labels + "}",
        )
      _ => ()
    }
  }
  // Decode replayed metadata to establish this connection's relation cache, but
  // do not transfer rows already covered by the contiguous durable prefix.
  if header.commit_lsn <= self.received {
    return
  }
  if event is Begin(_) {
    while self.pending.length() >= self.max_pending {
      self.ready.wait()
      if self.closed {
        raise Closed
      }
    }
    if self.sequence == 2147483647 {
      raise LimitExceeded("session sequence; reopen subscription")
    }
    self.sequence += 1
  }
  while !self.wal_queue.is_empty() {
    self.ready.wait()
    if self.closed {
      raise Closed
    }
  }
  let batch = event.batch(self.source, self.session, self.sequence)
  if batch.end_lsn is Some(end_lsn) {
    self.pending[self.sequence] = { end_lsn, delivered: false, done: false, }
    self.received = end_lsn
  }
  self.wal_queue.push(batch)
  self.ready.broadcast()
}

///|
async fn Subscription::next_wal_event(self : Subscription) -> WalBatch {
  while self.wal_queue.is_empty() && !self.closed {
    self.ready.wait()
  }
  if self.closed {
    raise Closed
  }
  let batch = self.wal_queue.remove(0)
  if batch.end_lsn is Some(_) {
    self.pending[batch.sequence].delivered = true
  }
  self.ready.broadcast()
  batch
}
