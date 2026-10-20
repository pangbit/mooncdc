///|
priv struct MemoryCopyDestination {
  pressure : MemoryPressure
  monitor : @memory.Monitor
  initial_pressure : Bool
  pending : Array[String]
  durable : Array[String]
  mut writes : Int
  mut flushes : Int
}

///|
impl Destination for MemoryCopyDestination with fn reset_table(_, _, attempt~) {
  ignore(attempt)
}

///|
impl Destination for MemoryCopyDestination with fn write_rows(self, _, rows, _) {
  for row in rows {
    guard row is [Text(id)] else { fail("expected one id") }
    self.pending.push(id)
    self.writes += 1
    if self.writes == 1 {
      self.pressure.used = 90UL
      while !self.monitor.is_paused() {
        @async.sleep(1)
      }
    }
  }
  Accepted
}

///|
impl Destination for MemoryCopyDestination with fn write_transaction(_, _) {
  Durable
}

///|
impl Destination for MemoryCopyDestination with fn flush(self) {
  self.flushes += 1
  if self.flushes == 2 || (self.flushes == 1 && self.initial_pressure) {
    assert_true(self.monitor.is_paused())
  }
  for id in self.pending {
    self.durable.push(id)
  }
  self.pending.clear()
  if self.flushes == 2 {
    self.pressure.used = 74UL
  }
}

///|
async test "live memory pressure drains accepted snapshot rows before pausing FETCH" {
  if live_config() is Some(config) {
    for
      (adaptive, initial_pressure) in [
        (false, true),
        (true, true),
        (true, false),
      ] {
      @async.with_timeout(15000, () => {
        let sql = connect(config, 1048576, false)
        defer sql.close()
        ignore(
          sql.query(
            "CREATE SCHEMA memory_copy; CREATE TABLE memory_copy.items(id int PRIMARY KEY); INSERT INTO memory_copy.items VALUES(1),(2),(3); CREATE PUBLICATION memory_copy FOR TABLE memory_copy.items",
          ),
        )
        defer @async.protect_from_cancel(() => {
          drop_slot(sql, "memory_copy")
          ignore(
            sql.query(
              "DROP PUBLICATION memory_copy; DROP SCHEMA memory_copy CASCADE",
            ),
          )
        })
        let pressure = MemoryPressure::{
          used: if initial_pressure {
            90UL
          } else {
            0UL
          },
        }
        @memory.with_sampler(
          2,
          Some((0.85F, 0.75F)),
          previous => pressure.sample(previous),
          monitor => {
            let destination : MemoryCopyDestination = {
              pressure,
              monitor,
              initial_pressure,
              pending: [],
              durable: [],
              writes: 0,
              flushes: 0,
            }
            let control = ApplyControl()
            control.lifecycle = Running
            let batch = if adaptive {
              Some(
                BatchRuntime::new(
                  monitor,
                  BatchConfig::new(max_bytes=1UL, max_fill_ms=0),
                ),
              )
            } else {
              None
            }
            @async.with_task_group(group => {
              if initial_pressure {
                group.spawn_bg(() => {
                  while destination.flushes == 0 {
                    @async.sleep(1)
                  }
                  @async.sleep(20)
                  assert_eq(destination.writes, 0)
                  assert_true(control.health(stall_timeout_ms=10).ready)
                  pressure.used = 74UL
                })
              }
              let copy = async fn() -> Unit {
                ignore(
                  copy_snapshot_to_impl(
                    config,
                    destination,
                    slot="memory_copy",
                    publication="memory_copy",
                    batch_rows=1,
                    max_frame_bytes=1048576,
                    max_batch_bytes=1048576,
                    temporary=false,
                    table=None,
                    activity_control=control,
                    memory=monitor,
                    batch?,
                  ),
                )
              }
              match batch {
                Some(batch) => {
                  batch.governor.with_slots(1UL, copy)
                  assert_eq(batch.governor.registered_slots(), 0UL)
                }
                None => copy()
              }
            })
            assert_eq(destination.durable, ["1", "2", "3"])
            assert_true(destination.pending.is_empty())
            assert_eq(destination.flushes, if adaptive { 4 } else { 3 })
            assert_eq(control.activities().length(), 0)
          },
        )
        println(
          "LIVE: copy adaptive=\{adaptive}, initial_pressure=\{initial_pressure}, accepted barrier/rows drained and all three rows resumed",
        )
      })
    }
  }
}
