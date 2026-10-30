///|
fn copy_slot_lag(
  rows : Array[Array[String?]],
  point : SnapshotPosition,
) -> UInt64? raise {
  if rows.is_empty() {
    raise Retryable("table copy replication slot disappeared")
  }
  if rows.length() != 1 ||
    cell(rows, 0, 0) != "pgoutput" ||
    cell(rows, 0, 1) != point.source.database {
    raise UnsafeResume("table copy replication slot incompatible")
  }
  if cell(rows, 0, 2) == "lost" || rows[0].get(3) != Some(None) {
    raise Retryable("table copy replication slot invalidated")
  }
  match rows[0].get(4) {
    Some(Some(current)) => {
      let current = Lsn::parse(current)
      Some(
        if current > point.lsn {
          current.value - point.lsn.value
        } else {
          0UL
        },
      )
    }
    Some(None) => None
    _ => raise Protocol("missing table copy source LSN")
  }
}

///|
async fn sample_copy_slot(
  config : ConnectionConfig,
  point : SnapshotPosition,
  metrics? : RuntimeMetrics,
) -> UInt64? {
  @async.with_timeout(config.timeout_ms, () => {
    let conn = connect(config, 1048576, false, metrics?)
    defer conn.close()
    let rows = conn.snapshot_query(
      "SELECT plugin,database,wal_status,invalidation_reason,CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END::text FROM pg_replication_slots WHERE slot_name='\{point.source.slot}'",
      config.timeout_ms,
      1048576,
    )
    copy_slot_lag(rows, point)
  })
}

///|
// Sampling owns independent connections and never queries the snapshot exporter.
// The scope cancels and joins pending handshakes/queries on every owner exit.
async fn[T] with_copy_monitor(
  config : ConnectionConfig,
  slot : String,
  interval_ms : Int32,
  metrics : RuntimeMetrics,
  run : async ((SnapshotPosition) -> Unit) -> T,
) -> T {
  let mut point : SnapshotPosition? = None
  let ready = @async.CondVar::Cond()
  defer metrics.copy_lags.remove(slot)
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      while point is None {
        ready.wait()
      }
      let point = point.unwrap()
      for ;; {
        try sample_copy_slot(config, point, metrics~) catch {
          Retryable(_) as error => {
            metrics.increment(
              "mooncdc_slot_invalidations_total{worker_type=\"table_sync\"}",
            )
            raise error
          }
          UnsafeResume(_) as error => raise error
          _ => metrics.increment("mooncdc_copy_monitor_failures_total")
        } noraise {
          Some(lag) => metrics.copy_lags[slot] = lag
          None => ()
        }
        // Fixed delay skips missed ticks and never accumulates sampler tasks.
        @async.sleep(interval_ms)
      }
    })
    let result = run(position => {
      point = Some(position)
      ready.broadcast()
    })
    group.return_immediately(result)
    result
  })
}
