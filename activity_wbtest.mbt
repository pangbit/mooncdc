///|
test "health follows lifecycle and the slowest independent worker" {
  for lifecycle in [Initializing, Running, Stopping] {
    let empty = activity_health(lifecycle, [], 100UL)
    assert_true(empty.live)
    assert_false(empty.ready)
    assert_eq(
      empty.status,
      if lifecycle == Stopping {
        "stopping"
      } else {
        "initializing"
      },
    )
    for age in [99UL, 100UL, 101UL] {
      let health = activity_health(
        lifecycle,
        [
          { kind: InitialTableCopy, inactive_ms: age, },
          { kind: WalApply(wal_sender_timeout_ms=60000UL), inactive_ms: 0UL, },
        ],
        100UL,
      )
      assert_eq(health.live, lifecycle == Stopping || age < 100UL)
      assert_eq(health.ready, lifecycle == Running && age < 100UL)
    }
  }
  for
    (stall, sender, boundary) in [
      (100UL, 60000UL, 60000UL),
      (1000UL, 500UL, 1000UL),
    ] {
    for age in [boundary - 1UL, boundary] {
      let health = activity_health(
        Running,
        [{ kind: WalApply(wal_sender_timeout_ms=sender), inactive_ms: age, }],
        stall,
      )
      assert_eq(health.ready, age < boundary)
      assert_eq(health.live, age < boundary)
    }
  }
}

///|
async test "activity handles retire independently and cannot revive a worker" {
  let control = ApplyControl()
  control.lifecycle = Running
  let copy = control.observe(InitialTableCopy)
  let wal = control.observe(WalApply(wal_sender_timeout_ms=60000UL))
  assert_eq(control.activities().length(), 2)
  @async.sleep(30)
  wal.ping()
  assert_eq(control.health(stall_timeout_ms=10).status, "inactive")
  copy.close()
  copy.ping()
  assert_eq(control.activities().length(), 1)
  assert_true(control.health(stall_timeout_ms=10).ready)
  wal.close()
  assert_eq(control.health().status, "initializing")
  control.stop()
  assert_eq(control.health().status, "stopping")
}

///|
priv struct ActivityStallDestination {
  control : ApplyControl
  flush_only : Bool
  timeout_ms : Int32
}

///|
async fn ActivityStallDestination::stall(
  self : ActivityStallDestination,
) -> Unit {
  @async.sleep(self.timeout_ms + 1100)
  assert_eq(self.control.health(stall_timeout_ms=10).status, "inactive")
  assert_false(self.control.health(stall_timeout_ms=10).live)
  self.control.stop()
}

///|
impl Destination for ActivityStallDestination with fn reset_table(
  _,
  _,
  attempt~,
) {
  ignore(attempt)
}

///|
impl Destination for ActivityStallDestination with fn write_rows(_, _, _, _) {
  Durable
}

///|
impl Destination for ActivityStallDestination with fn write_transaction(self, _) {
  if self.flush_only {
    Accepted
  } else {
    self.stall()
    Durable
  }
}

///|
impl Destination for ActivityStallDestination with fn flush(self) {
  self.stall()
}

///|
async test "live health observes idle WAL but not keepalives during blocked apply" {
  if live_config() is Some(config) {
    @async.with_timeout(45000, () => {
      let sql = connect(config, 1048576, false)
      defer sql.close()
      let dir = @fs.tmpdir(prefix="mooncdc-activity-")
      defer @fs.rmdir(dir, recursive=true)
      let timeout = sql.wal_activity_timeout(config.timeout_ms)
      assert_true(timeout > 1000UL && timeout <= 5000UL)
      for flush_only in [false, true] {
        let slot = "activity_" + pipeline_nonce()
        let start = fresh_slot(sql, slot)
        defer drop_slot(sql, slot)
        let control = ApplyControl()
        control.lifecycle = Running
        subscribe_impl(
          config,
          slot~,
          publication="mooncdc_test",
          start=At(start),
          checkpoint_path=dir + "/" + slot,
          activity_control=control,
          worker_retry=true,
          sub => {
            assert_eq(control.activities().length(), 1)
            assert_eq(
              control.activities()[0].kind,
              WalApply(wal_sender_timeout_ms=timeout),
            )
            // Longer than the server allowance, without any publication writes.
            @async.sleep(timeout.to_int() + 1100)
            assert_true(control.health(stall_timeout_ms=10).ready)
            assert_eq(sub.diagnostics().durable, start)
            ignore(
              sql.query(
                "INSERT INTO items VALUES(91234,'activity') ON CONFLICT(id) DO UPDATE SET value=excluded.value",
              ),
            )
            let target = ActivityStallDestination::{
              control,
              flush_only,
              timeout_ms: timeout.to_int(),
            }
            sub.apply_to(target, control~, flush_interval_ms=20)
            assert_true(sub.diagnostics().durable > start)
            assert_eq(control.health().status, "stopping")
          },
        )
        assert_eq(control.activities().length(), 0)
      }
      println(
        "LIVE: idle keepalive activity and stalled destination write/flush health passed",
      )
    })
  }
}
