///|
async fn source_monitor_wait(sub : Subscription, minimum : Lsn) -> Unit {
  @async.with_timeout(15000, () => {
    let observation = sub.activity.unwrap().metrics.source_monitors[sub.activity.unwrap().id]
    while !observation.current
          .map(current => current >= minimum)
          .unwrap_or(false) {
      @async.sleep(20)
    }
  })
}

///|
async fn source_monitor_capture(
  sub : Subscription,
  control : ApplyControl,
  sql : Connection,
  phase : String,
) -> Unit {
  let observation = control.metrics_data.source_monitors[sub.activity.unwrap().id]
  let confirmed = cell(
    sql.query(
      "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='\{sub.source.slot}'",
    ),
    0,
    0,
  )
  let metrics = control.prometheus()
  if @env.get_env_var("MOONCDC_SOURCE_MONITOR_EVIDENCE") is Some(dir) {
    @fs.write_file(dir + "/" + phase + ".metrics", metrics)
    @fs.write_file(
      dir + "/" + phase + ".json",
      Json::Json({
        "phase": Json::Json(phase),
        "source": ToJson::to_json(sub.source),
        "current": Json::Json(observation.current.map(lsn => lsn.to_string())),
        "received": Json::Json(sub.received.to_string()),
        "durable": Json::Json(sub.durable.to_string()),
        "server_end": Json::Json(sub.server_end.to_string()),
        "checkpoint": Json::Json(
          load_checkpoint(sub.path, sub.source).to_string(),
        ),
        "confirmed_flush": Json::Json(confirmed),
        "sample_age_seconds": Json::Json(
          observation.sampled.map(sampled => {
            @bench.monotonic_clock_end(sampled) / 1000000.0
          }),
        ),
        "health": Json::Json(control.health(stall_timeout_ms=10).status),
      }).stringify(indent=2),
    )
  }
}

///|
priv struct SourceMonitorStallDestination {
  control : ApplyControl
  sub : Subscription
  sql : Connection
  start : Lsn
  sender_timeout_ms : Int32
  flush_only : Bool
}

///|
async fn SourceMonitorStallDestination::stall(
  self : SourceMonitorStallDestination,
) -> Unit {
  let phase = if self.flush_only { "blocked-flush" } else { "blocked-write" }
  let before = self.control.metrics_data.counters
    .get("mooncdc_source_monitor_samples_total{worker_type=\"apply\"}")
    .unwrap_or(0UL)
  ignore(
    self.sql.query(
      "INSERT INTO source_monitor_noise SELECT g,repeat('n',1024) FROM generate_series(1,100)g; SELECT pg_log_standby_snapshot()",
    ),
  )
  let current = Lsn::parse(
    cell(self.sql.query("SELECT pg_current_wal_lsn()::text"), 0, 0),
  )
  source_monitor_wait(self.sub, current)
  assert_eq(self.sub.durable, self.start)
  assert_eq(load_checkpoint(self.sub.path, self.sub.source), self.start)
  source_monitor_capture(self.sub, self.control, self.sql, phase + "-begin")
  @async.sleep(self.sender_timeout_ms + 1100)
  let observation = self.control.metrics_data.source_monitors[self.sub.activity.unwrap().id]
  assert_true(
    @bench.monotonic_clock_end(observation.sampled.unwrap()) / 1000000.0 < 1.0,
  )
  assert_true(
    self.control.metrics_data.counters["mooncdc_source_monitor_samples_total{worker_type=\"apply\"}"] >
    before + 2UL,
  )
  assert_true(observation.current.unwrap() > self.sub.received)
  assert_eq(self.sub.durable, self.start)
  assert_eq(load_checkpoint(self.sub.path, self.sub.source), self.start)
  let confirmed = Lsn::parse(
    cell(
      self.sql.query(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='\{self.sub.source.slot}'",
      ),
      0,
      0,
    ),
  )
  assert_true(confirmed <= self.start)
  assert_eq(self.control.health(stall_timeout_ms=10).status, "inactive")
  source_monitor_capture(self.sub, self.control, self.sql, phase + "-end")
  self.control.stop()
}

///|
impl Destination for SourceMonitorStallDestination with fn reset_table(
  _,
  _,
  attempt~,
) {
  ignore(attempt)
}

///|
impl Destination for SourceMonitorStallDestination with fn write_rows(
  _,
  _,
  _,
  _,
) {
  Durable
}

///|
impl Destination for SourceMonitorStallDestination with fn write_transaction(
  self,
  _,
) {
  if self.flush_only {
    Accepted
  } else {
    self.stall()
    Durable
  }
}

///|
impl Destination for SourceMonitorStallDestination with fn flush(self) {
  self.stall()
}

///|
async test "live independent source samples advance during gated reads and blocked destination writes and flushes" {
  if @env.get_env_var("MOONCDC_SOURCE_MONITOR_PORT") is Some(port) {
    let config : ConnectionConfig = {
      hostaddr: None,
      keepalive: TcpKeepaliveConfig::new(),
      host: "127.0.0.1",
      port: @string.parse_int(port),
      user: "cdc",
      password: "mooncdc-test-only",
      database: "postgres",
      security: LocalPlaintext,
      timeout_ms: 5000,
    }
    @async.with_timeout(90000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      let dir = @fs.tmpdir(prefix="mooncdc-source-monitor-live-")
      defer @fs.rmdir(dir, recursive=true)
      ignore(
        sql.query(
          "CREATE TABLE source_monitor_items(id int PRIMARY KEY); CREATE TABLE source_monitor_noise(id int, value text); CREATE PUBLICATION source_monitor_live FOR TABLE source_monitor_items",
        ),
      )
      defer @async.protect_from_cancel(() => {
        ignore(
          sql.query(
            "DROP PUBLICATION source_monitor_live; DROP TABLE source_monitor_items,source_monitor_noise",
          ),
        )
      })
      let mut used = 90UL
      @memory.with_sampler(
        20,
        Some((0.85F, 0.75F)),
        _ => { snapshot: { used, total: 100UL, source: System, }, fresh: true, },
        monitor => {
          assert_true(monitor.is_paused())
          let start = fresh_slot(sql, "source_monitor_paused")
          defer drop_slot(sql, "source_monitor_paused")
          let control = ApplyControl()
          control.lifecycle = Running
          control.metrics_data.memory = Some(monitor)
          subscribe_impl(
            config,
            slot="source_monitor_paused",
            publication="source_monitor_live",
            start=At(start),
            checkpoint_path=dir + "/paused",
            activity_control=control,
            memory=monitor,
            source_monitor_interval_ms=20,
            sub => {
              let checkpoint = @fs.read_file(sub.path).binary()
              ignore(
                sql.query(
                  "INSERT INTO source_monitor_noise SELECT g,repeat('n',1024) FROM generate_series(1,100)g; SELECT pg_log_standby_snapshot()",
                ),
              )
              let current = Lsn::parse(
                cell(sql.query("SELECT pg_current_wal_lsn()::text"), 0, 0),
              )
              source_monitor_wait(sub, current)
              assert_true(current > start)
              assert_eq(sub.server_end, start)
              assert_eq(sub.received, start)
              assert_eq(sub.durable, start)
              assert_true(@fs.read_file(sub.path).binary() == checkpoint)
              let confirmed = Lsn::parse(
                cell(
                  sql.query(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='source_monitor_paused'",
                  ),
                  0,
                  0,
                ),
              )
              assert_true(confirmed <= start)
              source_monitor_capture(
                sub, control, sql, "synthetic-receiver-paused",
              )
              used = 74UL
              @async.with_timeout(5000, () => {
                while monitor.is_paused() {
                  @async.sleep(10)
                }
              })
              assert_eq(monitor.transitions(), (1UL, 1UL))
              ignore(sql.query("INSERT INTO source_monitor_items VALUES(1)"))
              let tx = sub.next()
              sub.ack(tx)
              assert_true(sub.durable > start)
              source_monitor_capture(
                sub, control, sql, "synthetic-receiver-resumed",
              )
            },
          )
          assert_true(control.metrics_data.source_monitors.is_empty())
          assert_true(control.activities().is_empty())
        },
      )
      let sender = sql.wal_activity_timeout(config.timeout_ms)
      assert_true(sender <= 5000UL && sender > 0UL)
      for flush_only in [false, true] {
        let slot = if flush_only {
          "source_monitor_flush"
        } else {
          "source_monitor_write"
        }
        let start = fresh_slot(sql, slot)
        defer drop_slot(sql, slot)
        let control = ApplyControl()
        control.lifecycle = Running
        subscribe_impl(
          config,
          slot~,
          publication="source_monitor_live",
          start=At(start),
          checkpoint_path=dir + "/" + slot,
          activity_control=control,
          source_monitor_interval_ms=20,
          sub => {
            ignore(
              sql.query(
                "INSERT INTO source_monitor_items VALUES(\{if flush_only { 3 } else { 2 }})",
              ),
            )
            sub.apply_to(
              SourceMonitorStallDestination::{
                control,
                sub,
                sql,
                start,
                sender_timeout_ms: sender.to_int(),
                flush_only,
              },
              control~,
              flush_interval_ms=20,
            )
            assert_true(sub.durable > start)
            assert_eq(load_checkpoint(sub.path, sub.source), sub.durable)
            source_monitor_capture(
              sub,
              control,
              sql,
              if flush_only {
                "flush-released-durable"
              } else {
                "write-released-durable"
              },
            )
          },
        )
        assert_true(control.metrics_data.source_monitors.is_empty())
        assert_true(control.activities().is_empty())
      }
      assert_eq(
        cell(
          sql.query(
            "SELECT count(*)::text FROM pg_replication_slots WHERE slot_name LIKE 'source_monitor_%'",
          ),
          0,
          0,
        ),
        "0",
      )
      println(
        "SOURCE MONITOR LIVE: synthetic receiver pause/resume and real blocked destination write/flush; source sampling continued without checkpoint/feedback/health mutation; workers retired",
      )
    })
  }
}
