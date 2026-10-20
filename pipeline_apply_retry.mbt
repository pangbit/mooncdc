///|
// Only source I/O and destination calls mark an attempt retryable. Persistence
// and user callback failures must not become retries because of their shape.
priv suberror ApplyWriteFailed {
  ApplyWriteFailed(Error)
}

///|
priv suberror PipelineFlushFailed {
  PipelineFlushFailed(Error)
}

///|
priv suberror ApplySourceFailed {
  ApplySourceFailed(Error)
}

///|
async fn[T] source_operation(run : async () -> T, retry? : Bool = true) -> T {
  run() catch {
    error => if retry { raise ApplySourceFailed(error) } else { raise error }
  }
}

///|
priv struct ApplyRetry {
  control : ApplyControl
  delay_ms : Int
  maximum : Int
  mut retries : Int
}

///|
fn ApplyRetry::new(
  control : ApplyControl,
  delay_ms : Int,
  maximum : Int,
) -> ApplyRetry {
  { control, delay_ms, maximum, retries: 0, }
}

///|
async fn[T] ApplyRetry::run(self : ApplyRetry, attempt : async () -> T) -> T? {
  while !self.control.stopped {
    try attempt() catch {
      ApplyWriteFailed(error)
      | PipelineFlushFailed(error)
      | ApplySourceFailed(error) as marked => {
        self.control.metrics_data.increment(
          "mooncdc_worker_errors_total{worker_type=\"apply\"}",
        )
        let transient = match (marked, error) {
          (ApplySourceFailed(_), @os_error.OSError(code, context~)) => {
            let os = @os_error.OSError(code, context~)
            !os.is_EACCES() && !os.is_ENOENT() && !os.is_ENOTDIR()
          }
          _ => table_retry_policy(error, 0, 1, self.delay_ms) is TimedRetry(_)
        }
        if @async.is_being_cancelled() ||
          self.control.stopped ||
          !transient ||
          self.retries >= self.maximum {
          raise error
        }
        self.retries += 1
        self.control.metrics_data.increment(
          "mooncdc_worker_retries_total{worker_type=\"apply\",trigger=\"timed\"}",
        )
        // Failed connection/subscription scopes have closed their sockets.
        // Streaming attempts discard delivery and reload the durable checkpoint.
        @async.any(
          [() => self.control.wait(), () => @async.sleep(self.delay_ms)],
          allow_failure=false,
        )
      }
      error => raise error
    } noraise {
      value => return Some(value)
    }
  }
  None
}
