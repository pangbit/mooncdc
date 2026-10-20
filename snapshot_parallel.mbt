///|
fn validate_copy_connections(value : Int) -> Unit raise {
  if value <= 0 || value > 256 {
    raise InvalidConfig("snapshot copy connections must be between 1 and 256")
  }
}

///|
priv enum CtidRange {
  OpenStart(UInt64)
  Bounded(UInt64, UInt64)
  OpenEnd(UInt64)
}

///|
// Storage size is observed at planning time, not at the imported MVCC snapshot.
// Keep the first and last ranges open, including the one-range case.
fn ctid_ranges(blocks : UInt64, partitions : Int) -> Array[CtidRange] raise {
  if partitions <= 0 || partitions > 1024 || blocks > 0xffffffffUL {
    raise InvalidConfig("invalid CTID planning limit")
  }
  if blocks == 0UL {
    return []
  }
  let count = if blocks < partitions.to_uint64() {
    blocks
  } else {
    partitions.to_uint64()
  }
  Array::makei(count.to_int(), i => {
    let start = i.to_uint64() * blocks / count
    let end = (i + 1).to_uint64() * blocks / count
    if count == 1UL || i == count.to_int() - 1 {
      OpenEnd(start)
    } else if i == 0 {
      OpenStart(end)
    } else {
      Bounded(start, end)
    }
  })
}

///|
fn CtidRange::predicate(self : CtidRange) -> String {
  match self {
    OpenStart(end) => "ctid < '(\{end},1)'::tid"
    Bounded(start, end) =>
      "ctid >= '(\{start},1)'::tid AND ctid < '(\{end},1)'::tid"
    OpenEnd(start) => "ctid >= '(\{start},1)'::tid"
  }
}

///|
priv struct CopyEstimate {
  qualified : String
  blocks : UInt64
  rows : UInt64
}

///|
priv struct SnapshotCopyJob {
  qualified : String
  range : CtidRange
  estimated_blocks : UInt64
}

///|
fn snapshot_copy_partition_target(
  estimates : Array[CopyEstimate],
  connections : Int,
) -> Int raise {
  validate_copy_connections(connections)
  if estimates.all(estimate => estimate.blocks == 0UL) {
    return 0
  }
  let mut rows = 0UL
  for estimate in estimates {
    rows = batch_add(rows, estimate.rows)
  }
  let row_partitions = rows / 250000UL +
    (if rows % 250000UL == 0UL { 0UL } else { 1UL })
  let row_partitions = if row_partitions > 1024UL {
    1024
  } else {
    row_partitions.to_int()
  }
  if connections == 1 {
    1
  } else {
    (connections * 4).max(row_partitions).min(1024)
  }
}

///|
fn snapshot_copy_jobs(
  estimates : Array[CopyEstimate],
  connections : Int,
) -> Array[SnapshotCopyJob] raise {
  let target = snapshot_copy_partition_target(estimates, connections)
  let mut weight = 0.0
  let use_rows = estimates.all(estimate => estimate.rows > 0UL)
  for estimate in estimates {
    weight += (if use_rows { estimate.rows } else { estimate.blocks }).to_double()
  }
  let jobs : Array[SnapshotCopyJob] = []
  for estimate in estimates {
    let share = if weight == 0.0 {
      1
    } else {
      let local_weight = if use_rows { estimate.rows } else { estimate.blocks }
      (local_weight.to_double() / weight * target.to_double())
      .ceil()
      .to_int()
      .max(1)
    }
    for range in ctid_ranges(estimate.blocks, share.min(1024)) {
      let estimated_blocks = match range {
        OpenStart(end) => end
        Bounded(start, end) => end - start
        OpenEnd(start) => estimate.blocks - start
      }
      jobs.push({ qualified: estimate.qualified, range, estimated_blocks, })
    }
  }
  jobs
}

///|
async fn Connection::copy_snapshot_cursor(
  self : Connection,
  columns : Int,
  batch_rows : Int,
  timeout : Int,
  max_batch_bytes : Int,
  activity : ActivityHandle?,
  consume : async (ReadOnlyArray[ReadOnlyArray[Value]]) -> Unit,
  before_read? : async (ActivityHandle?) -> Unit,
  batch? : BatchRuntime,
) -> Unit {
  let started = @bench.monotonic_clock_start()
  let mut total_rows = 0UL
  for ;; {
    if before_read is Some(before_read) {
      before_read(activity)
    }
    if batch is Some(batch) {
      let count = self.fetch_snapshot_rows(
        batch_rows, columns, timeout, batch, activity, consume,
      )
      if count == 0 {
        break
      }
      total_rows = batch_add(total_rows, count.to_uint64())
      continue
    }
    let rows = self.snapshot_query(
      "FETCH FORWARD \{batch_rows} FROM mooncdc_snapshot",
      timeout,
      max_batch_bytes,
    )
    if rows.is_empty() {
      break
    }
    let values : Array[ReadOnlyArray[Value]] = rows.map(row => {
      if row.length() != columns {
        raise Protocol("snapshot tuple column count")
      }
      ReadOnlyArray::from_array(
        row.map(value => {
          match value {
            Some(text) => Text(text)
            None => Null
          }
        }),
      )
    })
    consume(ReadOnlyArray::from_array(values))
    total_rows = batch_add(total_rows, values.length().to_uint64())
    if activity is Some(activity) {
      activity.ping()
    }
  }
  ignore(
    self.snapshot_query("CLOSE mooncdc_snapshot", timeout, max_batch_bytes),
  )
  if activity is Some(activity) {
    let labels = "{" + activity.metric_worker() + "}"
    activity.metrics.increment(
      "mooncdc_copy_partitions_completed_total" + labels,
    )
    activity.metrics.duration(
      "mooncdc_copy_partition_seconds" + labels,
      started,
    )
    activity.metrics.sample(
      "mooncdc_copy_partition_rows" + labels,
      total_rows.to_double(),
      [0, 1, 10, 100, 1000, 10000, 100000, 1000000],
    )
  }
}

///|
async fn Connection::copy_snapshot_parallel(
  self : Connection,
  config : ConnectionConfig,
  snapshot : String,
  relation : Relation,
  partitioned : Bool,
  row_filter : String?,
  connections : Int,
  batch_rows : Int,
  max_frame_bytes : Int,
  max_batch_bytes : Int,
  planning_activity : ActivityHandle?,
  consume : async (ReadOnlyArray[ReadOnlyArray[Value]]) -> Unit,
  activity_control? : ApplyControl,
  worker? : SessionWorker,
  before_read? : async (ActivityHandle?) -> Unit,
  batch? : BatchRuntime,
) -> Unit {
  let oid = relation.oid
  let selected = if partitioned {
    "c.oid IN (SELECT relid::oid FROM pg_partition_tree(\{oid}::regclass) WHERE isleaf AND relid::oid != \{oid})"
  } else {
    "c.oid = \{oid}"
  }
  let meta = self.snapshot_query(
    "SELECT n.nspname, c.relname, (pg_relation_size(c.oid) / current_setting('block_size')::bigint)::text, greatest(c.reltuples,0)::bigint::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE \{selected} ORDER BY c.oid",
    config.timeout_ms,
    max_batch_bytes,
  )
  let estimates = meta.map(row => CopyEstimate::{
    qualified: sql_identifier(cell([row], 0, 0)) +
    "." +
    sql_identifier(cell([row], 0, 1)),
    blocks: @string.parse_uint64(cell([row], 0, 2)),
    rows: @string.parse_uint64(cell([row], 0, 3)),
  })
  let jobs = snapshot_copy_jobs(estimates, connections)
  if planning_activity is Some(activity) {
    let labels = "{" + activity.metric_worker() + "}"
    let bounds : Array[Double] = [
      0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
    ]
    activity.metrics.sample(
      "mooncdc_copy_planned_partitions" + labels,
      snapshot_copy_partition_target(estimates, connections).to_double(),
      bounds,
    )
    activity.metrics.sample(
      "mooncdc_copy_effective_partitions" + labels,
      jobs.length().to_double(),
      bounds,
    )
    for job in jobs {
      activity.metrics.sample(
        "mooncdc_copy_partition_estimated_blocks" + labels,
        job.estimated_blocks.to_double(),
        [
          0, 1, 8, 64, 512, 4096, 32768, 262144, 2097152, 16777216, 134217728, 1073741824,
          4294967295,
        ],
      )
    }
  }
  let count = connections.min(jobs.length())
  if count == 0 {
    return
  }
  if planning_activity is Some(activity) {
    activity.source_paused(true)
  }
  defer (if planning_activity is Some(activity) {
    activity.source_paused(false)
  })
  let projection = relation.columns.map(c => sql_identifier(c.name)).join(", ")
  let filter = row_filter.map_or("", expression => " AND (" + expression + ")")
  let mutex = @async.Mutex()
  let mut next = 0
  let run = async fn() -> Unit {
    @async.with_task_group(group => {
      for _ in 0..<count {
        group.spawn_bg(() => {
          let activity = activity_control.map(control => {
            control.observe(InitialTableCopy)
          })
          defer (if activity is Some(activity) { activity.close() })
          let reader = connect(
            config,
            max_frame_bytes,
            false,
            workload=SnapshotReader,
            worker?,
            metrics?=activity_control.map(control => control.metrics_data),
          )
          defer reader.close()
          ignore(
            reader.snapshot_query(
              "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT \{snapshot}; SET LOCAL search_path = pg_catalog; SET LOCAL row_security = off",
              config.timeout_ms,
              max_batch_bytes,
            ),
          )
          // Claim a job without an await. MoonBit tasks share a cooperative loop.
          while next < jobs.length() {
            let job = jobs[next]
            next += 1
            ignore(
              reader.snapshot_query(
                "DECLARE mooncdc_snapshot NO SCROLL CURSOR FOR SELECT \{projection} FROM ONLY \{job.qualified} WHERE \{job.range.predicate()}\{filter}",
                config.timeout_ms,
                max_batch_bytes,
              ),
            )
            reader.copy_snapshot_cursor(
              relation.columns.length(),
              batch_rows,
              config.timeout_ms,
              max_batch_bytes,
              activity,
              rows => {
                mutex.acquire()
                defer mutex.release()
                consume(rows)
              },
              before_read?,
              batch?,
            )
          }
          ignore(
            reader.snapshot_query("COMMIT", config.timeout_ms, max_batch_bytes),
          )
        })
      }
    })
  }
  // The containing table-copy scope already registers one batch position.
  if batch is Some(batch) && count > 1 {
    batch.governor.with_slots((count - 1).to_uint64(), run)
  } else {
    run()
  }
}
