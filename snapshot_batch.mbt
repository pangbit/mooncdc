///|
// Read DataRow messages directly into decoded batches. A fill timeout cancels
// only the wait on an owned read task, never the partially consumed wire frame.
async fn Connection::fetch_snapshot_rows(
  self : Connection,
  maximum_rows : Int32,
  columns : Int32,
  timeout : Int32,
  batch : BatchRuntime,
  activity : ActivityHandle?,
  consume : async (ReadOnlyArray[ReadOnlyArray[Value]]) -> Unit,
) -> Int32 {
  // One sample for the entire FETCH, including result consumption/backpressure
  // and cancellation, not one per row/frame or per adaptive flush.
  let query_started = @bench.monotonic_clock_start()
  defer (if self.queries is Some(observer) { observer.record(query_started) })
  let out = @buffer.Buffer()
  cstring(out, "FETCH FORWARD \{maximum_rows} FROM mooncdc_snapshot")
  self.send(b'Q', out.to_bytes())
  let mut rows : Array[ReadOnlyArray[Value]] = []
  let mut bytes = 0UL
  let mut started : @bench.Timestamp? = None
  let mut count = 0
  let flush = async fn() -> Unit {
    if !rows.is_empty() {
      let current = rows
      rows = []
      bytes = 0UL
      started = None
      consume(ReadOnlyArray::from_array(current))
      if activity is Some(activity) {
        activity.ping()
      }
    }
  }
  @async.with_task_group(group => {
    for ;; {
      if batch.monitor.is_paused() ||
        started.map_or(false, started => {
          @bench.monotonic_clock_end(started) / 1000.0 >=
          batch.max_fill_ms.to_double()
        }) {
        flush()
      }
      wait_source_capacity(Some(batch.monitor), activity)
      let reading = group.spawn(() => {
        @async.with_timeout(timeout, () => self.read())
      })
      let mut frame = None
      while frame is None {
        if rows.is_empty() {
          frame = Some(reading.wait())
        } else {
          let remaining = (batch.max_fill_ms.to_double() -
            @bench.monotonic_clock_end(started.unwrap()) / 1000.0)
            .max(0.0)
            .to_int()
          frame = @async.any(
            [
              () => {
                batch.monitor.wait_for_pressure()
                None
              },
              () => {
                @async.sleep(remaining)
                None
              },
              () => Some(reading.wait()),
            ],
            allow_failure=false,
          )
          if frame is None {
            flush()
            // A frame already being read may finish while source is paused.
            // Its raw bytes stay owned by reading until capacity is available.
            wait_source_capacity(Some(batch.monitor), activity)
          }
        }
      }
      let (tag, body) = frame.unwrap()
      match tag {
        b'D' => {
          let cursor = Cursor::new(body)
          if cursor.u16() != columns {
            raise Protocol("snapshot tuple column count")
          }
          let row = ReadOnlyArray::makei(columns, _ => {
            let length = cursor.i32()
            if length == -1 {
              Null
            } else {
              Text(@utf8.decode(cursor.take(length), ignore_bom=true))
            }
          })
          cursor.done()
          count += 1
          if count > maximum_rows {
            raise Protocol("snapshot FETCH row count exceeded")
          }
          if rows.is_empty() {
            started = Some(@bench.monotonic_clock_start())
          }
          rows.push(row)
          bytes = batch_add(bytes, row_size_hint(row))
          if bytes >= batch.governor.target_bytes() ||
            batch.max_fill_ms == 0 ||
            batch.monitor.is_paused() {
            flush()
          }
        }
        b'E' => raise server_error(body)
        b'Z' => {
          flush()
          break
        }
        b'T' | b'C' | b'N' | b'S' => ()
        _ => raise Protocol("unexpected snapshot FETCH message")
      }
    }
    count
  })
}
