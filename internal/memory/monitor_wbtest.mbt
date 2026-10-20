///|
fn test_snapshot(used : UInt64, total : UInt64) -> Snapshot {
  { used, total, source: ProcessCgroup, }
}

///|
test "monitor revisions only track fresh capacity and pressure has hysteresis" {
  let monitor = Monitor::new(test_snapshot(85UL, 100UL), Some((0.85F, 0.75F)))
  assert_true(monitor.is_paused())
  assert_eq(monitor.transitions(), (1UL, 0UL))
  assert_eq(monitor.capacity_revision(), 0UL)
  monitor.publish({ snapshot: test_snapshot(75UL, 100UL), fresh: true, })
  assert_true(monitor.is_paused())
  assert_eq(monitor.capacity_revision(), 0UL)
  monitor.publish({ snapshot: test_snapshot(10UL, 1000UL), fresh: false, })
  assert_true(monitor.is_paused())
  assert_eq(monitor.current(), test_snapshot(75UL, 100UL))
  monitor.publish({
    snapshot: { used: 74UL, total: 100UL, source: System, },
    fresh: true,
  })
  assert_false(monitor.is_paused())
  assert_eq(monitor.transitions(), (1UL, 1UL))
  assert_eq(monitor.capacity_revision(), 0UL)
  monitor.publish({ snapshot: test_snapshot(10UL, 200UL), fresh: true, })
  assert_eq(monitor.capacity_revision(), 1UL)
  monitor.revision = 0xffffffffffffffffUL
  monitor.publish({ snapshot: test_snapshot(10UL, 300UL), fresh: true, })
  assert_eq(monitor.capacity_revision(), 0UL)
}

///|
async test "memory pressure gates source reads while owned batches drain" {
  let monitor = Monitor::new(test_snapshot(90UL, 100UL), Some((0.85F, 0.75F)))
  let started = @async.CondVar::Cond()
  let mut source_polls = 0
  let mut drained = false
  @async.with_task_group(group => {
    let source = group.spawn(() => {
      started.signal()
      monitor.wait_for_capacity()
      source_polls += 1
    })
    started.wait()
    // Destination completion does not pass through the source gate.
    drained = true
    assert_eq(source_polls, 0)
    monitor.publish({ snapshot: test_snapshot(75UL, 100UL), fresh: true, })
    @async.pause()
    assert_eq(source_polls, 0)
    monitor.publish({ snapshot: test_snapshot(74UL, 100UL), fresh: true, })
    source.wait()
    assert_true(drained)
    assert_eq(source_polls, 1)
  })
}

///|
async test "monitor task is joined even when shared readings escape scope" {
  let refresh_started = @async.CondVar::Cond()
  let mut reads = 0
  let mut cancelled = false
  let read : async (Snapshot?) -> Refresh = previous => {
    reads += 1
    if previous is Some(_) {
      defer {
        cancelled = true
      }
      refresh_started.signal()
      @async.sleep(60000)
    }
    { snapshot: test_snapshot(90UL, 100UL), fresh: true, }
  }
  let retained = with_sampler(10, Some((0.85F, 0.75F)), read, monitor => {
    assert_true(monitor.is_paused())
    assert_eq(monitor.transitions(), (1UL, 0UL))
    refresh_started.wait()
    monitor
  })
  assert_true(cancelled)
  assert_eq(reads, 2)
  assert_true(retained.is_paused())
  @async.pause()
  assert_eq(reads, 2)
}

///|
async test "governor slot scopes release on success error and cancellation" {
  let monitor = Monitor::new(test_snapshot(100UL, 10000UL), None)
  let governor = Governor::new(monitor, 0.2F, 33554432UL)
  assert_eq(governor.target_bytes(), 2000UL)
  governor.with_slots(2UL, () => {
    assert_eq(governor.target_bytes(), 1000UL)
    governor.with_slots(2UL, () => {
      assert_eq(governor.registered_slots(), 4UL)
      assert_eq(governor.target_bytes(), 500UL)
      monitor.publish({ snapshot: test_snapshot(9500UL, 10000UL), fresh: true, })
      assert_eq(governor.target_bytes(), 500UL)
      monitor.publish({ snapshot: test_snapshot(100UL, 20000UL), fresh: true, })
      assert_eq(governor.target_bytes(), 1000UL)
    })
    assert_eq(governor.target_bytes(), 2000UL)
  })
  assert_eq(governor.registered_slots(), 0UL)
  try
    governor.with_slots(1UL, fn() -> Unit raise {
      raise ConfigError("test failure")
    })
  catch {
    ConfigError(_) => ()
    error => raise error
  } noraise {
    _ => fail("expected failure")
  }
  assert_eq(governor.registered_slots(), 0UL)
  let entered = @async.CondVar::Cond()
  @async.with_task_group(group => {
    group.spawn_bg(no_wait=true, () => {
      governor.with_slots(3UL, () => {
        entered.signal()
        monitor.active = true
        monitor.wait_for_capacity()
      })
    })
    entered.wait()
    assert_eq(governor.registered_slots(), 3UL)
  })
  assert_eq(governor.registered_slots(), 0UL)
  assert_eq(governor.target_bytes(), 4000UL)
}

///|
async test "invalid monitor configuration fails before sampling or running consumer" {
  let mut calls = 0
  for (interval, thresholds) in [(0, None), (10, Some((0.8F, 0.8F)))] {
    try
      with_sampler(
        interval,
        thresholds,
        _ => {
          calls += 1
          { snapshot: test_snapshot(0UL, 100UL), fresh: true, }
        },
        _ => calls += 1,
      )
    catch {
      ConfigError(_) => ()
      error => raise error
    } noraise {
      _ => fail("expected invalid configuration")
    }
  }
  assert_eq(calls, 0)
}

///|
async test "monitor joins its sampler on consumer error and owner cancellation" {
  for fail_consumer in [true, false] {
    let refresh_started = @async.CondVar::Cond()
    let mut reading = false
    let mut joined = false
    let read : async (Snapshot?) -> Refresh = previous => {
      if previous is Some(_) {
        defer {
          joined = true
        }
        reading = true
        refresh_started.broadcast()
        @async.sleep(60000)
      }
      { snapshot: test_snapshot(90UL, 100UL), fresh: true, }
    }
    let run : async (Monitor) -> Unit = monitor => {
      while !reading {
        refresh_started.wait()
      }
      if fail_consumer {
        raise ConfigError("consumer failed")
      }
      monitor.wait_for_capacity()
    }
    if fail_consumer {
      try with_sampler(10, Some((0.85F, 0.75F)), read, run) catch {
        ConfigError(_) => ()
        error => raise error
      } noraise {
        _ => fail("consumer failure must propagate")
      }
    } else {
      @async.with_task_group(group => {
        group.spawn_bg(no_wait=true, () => {
          with_sampler(10, Some((0.85F, 0.75F)), read, run)
        })
        while !reading {
          refresh_started.wait()
        }
      })
    }
    assert_true(joined)
  }
}

///|
async test "disabled backpressure still updates governor capacity" {
  let monitor = Monitor::new(test_snapshot(100UL, 100UL), None)
  let governor = Governor::new(monitor, 0.2F, 1000UL)
  assert_false(monitor.is_paused())
  monitor.wait_for_capacity()
  assert_eq(governor.target_bytes(), 20UL)
  monitor.publish({ snapshot: test_snapshot(1000UL, 1000UL), fresh: true, })
  assert_false(monitor.is_paused())
  assert_eq(governor.target_bytes(), 200UL)
}

///|
async test "pressure notification wakes destination drain independently of source resume" {
  let monitor = Monitor::new(test_snapshot(74UL, 100UL), Some((0.85F, 0.75F)))
  @async.with_task_group(group => {
    let mut drained = false
    let drain = group.spawn(() => {
      monitor.wait_for_pressure()
      drained = true
    })
    @async.pause()
    assert_false(drained)
    monitor.publish({ snapshot: test_snapshot(85UL, 100UL), fresh: true, })
    drain.wait()
    assert_true(drained)
    assert_true(monitor.is_paused())
  })
}
