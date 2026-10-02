///|
/// Persisted retry policy; TimedRetry uses Unix milliseconds.
pub(all) enum TableRetryPolicy {
  NoRetry
  ManualRetry
  TimedRetry(UInt64)
} derive(Eq, Debug, ToJson, @json.FromJson)

///|
/// A destination may classify a table-local failure explicitly. Shared flush
/// failures still stop the pipeline regardless of this classification.
pub(all) suberror TableReplicationError {
  Retryable(String)
  Manual(String)
  Permanent(String)
} derive(Debug)

///|
priv suberror PipelineCritical {
  PipelineCritical(Error)
}

///|
fn table_retry_policy(
  error : Error,
  attempts : Int,
  maximum : Int,
  delay : Int,
) -> TableRetryPolicy {
  let timed = match error {
    Retryable(_)
    | @async.TimeoutError
    | @io.PipeClosed
    | @io.ReaderClosed
    | @tls.ConnectionClosed
    | @socket.ResolveHostnameError(_) => true
    @os_error.OSError(code, context~) =>
      @os_error.OSError(code, context~).is_ECONNREFUSED()
    Server(code, _) =>
      code == "57P01" ||
      code == "57P02" ||
      code == "57P03" ||
      code == "55P03" ||
      (code.has_prefix("08") && code != "08P01")
    Permanent(_) => return NoRetry
    _ => false
  }
  if timed && attempts < maximum {
    TimedRetry(@env.now() + delay.to_uint64())
  } else {
    ManualRetry
  }
}

///|
/// Retry a failed table on an active run_pipeline, using a fresh snapshot/reset.
/// Returns false for unknown, healthy or NoRetry tables. A manual request resets
/// the automatic retry budget. The control is usable only while the pipeline runs.
pub async fn ApplyControl::retry_table(self : ApplyControl, oid : UInt) -> Bool {
  if self.stopped {
    return false
  }
  guard self.table_retry is Some(retry) else {
    raise InvalidConfig("table retry requires a running pipeline")
  }
  retry(oid)
}

///|
async fn PipelineDestination::retry_table(
  self : PipelineDestination,
  oid : UInt,
) -> Bool {
  self.mutex.acquire()
  defer self.mutex.release()
  let retryable = self.store.state.tables.any(table => {
    table.oid == oid &&
    (match table.phase {
      Errored(_, NoRetry, _) => false
      Errored(_, _, _) => true
      _ => false
    })
  })
  if !retryable || self.control.stopped {
    return false
  }
  self.store.set_phase(oid, Pending)
  self.retry_attempts.remove(oid)
  self.copy_jobs.push(oid)
  self.copy_ready.broadcast()
  true
}

///|
async fn PipelineDestination::schedule_retries(
  self : PipelineDestination,
) -> Unit {
  for ;; {
    let mut delay = 1000
    {
      self.mutex.acquire()
      defer self.mutex.release()
      let now = @env.now()
      // update replaces the array; iterate the captured immutable state.
      for table in self.store.state.tables {
        if table.phase is Errored(_, TimedRetry(at), attempts) {
          if at <= now {
            self.store.set_phase(table.oid, Pending)
            self.retry_attempts[table.oid] = attempts + 1
            self.copy_jobs.push(table.oid)
            self.copy_ready.broadcast()
          } else {
            let remaining = at - now
            if remaining < delay.to_uint64() {
              delay = remaining.to_int()
            }
          }
        }
      }
    }
    @async.any(
      [() => self.copy_ready.wait(), () => @async.sleep(delay)],
      allow_failure=false,
    )
  }
}
