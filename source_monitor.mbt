///|
priv struct SourceObservation {
  mut current : Lsn?
  mut sampled : @bench.Timestamp?
  worker_type : String
  progress : () -> (Lsn, Lsn)
}

///|
fn source_current_lsn(rows : Array[Array[String?]]) -> Lsn? raise {
  guard rows.length() == 1 && rows[0].length() == 1 else {
    raise Protocol("source LSN sample shape")
  }
  rows[0][0].map(value => Lsn::parse(value))
}

///|
async fn sample_source_lsn(
  config : ConnectionConfig,
  metrics? : RuntimeMetrics,
) -> Lsn? {
  @async.with_timeout(config.timeout_ms, () => {
    let conn = connect(config, 1048576, false, metrics?)
    defer conn.close()
    source_current_lsn(
      conn.snapshot_query(
        "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END::text",
        config.timeout_ms,
        1048576,
      ),
    )
  })
}

///|
// Observation only: this independent SQL connection must never authorize a
// checkpoint, feedback, write horizon or handover. It keeps running when the
// stream receiver is paused or its destination is blocked.
async fn[T] with_source_monitor(
  config : ConnectionConfig,
  activity : ActivityHandle,
  interval_ms : Int32,
  progress : () -> (Lsn, Lsn),
  run : async () -> T,
) -> T {
  let observation : SourceObservation = {
    current: None,
    sampled: None,
    worker_type: activity.worker_type,
    progress,
  }
  activity.metrics.source_monitors[activity.id] = observation
  defer activity.metrics.source_monitors.remove(activity.id)
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      for ;; {
        try sample_source_lsn(config, metrics=activity.metrics) catch {
          _ =>
            activity.metrics.increment(
              "mooncdc_source_monitor_failures_total{" +
              activity.metric_worker() +
              "}",
            )
        } noraise {
          Some(current) =>
            if observation.current.map(last => current >= last).unwrap_or(true) {
              observation.current = Some(current)
              observation.sampled = Some(@bench.monotonic_clock_start())
              activity.metrics.increment(
                "mooncdc_source_monitor_samples_total{" +
                activity.metric_worker() +
                "}",
              )
            } else {
              activity.metrics.increment(
                "mooncdc_source_monitor_failures_total{" +
                activity.metric_worker() +
                "}",
              )
            }
          None => ()
        }
        // Fixed delay: missed ticks do not create a connection/task backlog.
        @async.sleep(interval_ms)
      }
    })
    let result = run()
    group.return_immediately(result)
    result
  })
}
