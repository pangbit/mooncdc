///|
async test "frame limit diagnostic preserves payload and following frame" {
  let payload = b"SENSITIVE-PAYLOAD"
  assert_eq(payload.length(), 17)
  let encoded = packet(b'\xff', payload) + packet(b'Z', b"I")
  for split in 1..<5 {
    let reader = @io.MemoryReader(writer => {
      writer.write(encoded[:split])
      @async.pause()
      writer.write(encoded[split:])
    })
    defer reader.close()
    try read_frame(reader, 16) catch {
      LimitExceeded(message) => {
        assert_eq(
          message, "message frame: type=255 declared_length=21 max_body_bytes=16 role=unknown worker=unspecified",
        )
        assert_false(message.contains("SENSITIVE-PAYLOAD"))
      }
      error => raise error
    } noraise {
      _ => fail("oversized frame accepted")
    }
    assert_eq(reader.read_exactly(17), payload)
    assert_eq(read_frame(reader, 16), (b'Z', b"I"))
  }
}

///|
async test "frame limit diagnostic keeps exact-limit and empty-body frames valid" {
  let body = b"0123456789abcdef"
  let encoded = packet(b'd', body) + packet(b'Z', b"")
  for split in 1..<encoded.length() {
    let reader = @io.MemoryReader(writer => {
      writer.write(encoded[:split])
      @async.pause()
      writer.write(encoded[split:])
    })
    defer reader.close()
    assert_eq(read_frame(reader, 16), (b'd', body))
    assert_eq(read_frame(reader, 0), (b'Z', b""))
  }
}

///|
async test "frame limit diagnostic retains truncated and invalid-length categories" {
  for header in [b"d", b"d\x00\x00\x00"] {
    let reader = @io.MemoryReader(writer => writer.write(header))
    defer reader.close()
    try read_frame(reader, 16) catch {
      @io.ReaderClosed => ()
      error => raise error
    } noraise {
      _ => fail("truncated header accepted")
    }
  }
  for header in [b"d\x00\x00\x00\x03", b"d\xff\xff\xff\xff"] {
    let reader = @io.MemoryReader(writer => writer.write(header))
    defer reader.close()
    try read_frame(reader, 16) catch {
      Protocol(message) => assert_eq(message, "frame length below four")
      error => raise error
    } noraise {
      _ => fail("invalid length accepted")
    }
  }
}

///|
async test "frame limit diagnostic records only fixed connection and worker roles" {
  let workloads : Array[(SessionWorkload, String)] = [
    (Queries, "queries"),
    (Replication, "replication"),
    (SnapshotReader, "snapshot_reader"),
    (SnapshotExporter, "snapshot_exporter"),
    (StateStore, "state_store"),
    (SourceMigrations, "source_migrations"),
  ]
  let workers : Array[(SessionWorker?, String)] = [
    (None, "unspecified"),
    (Some(ApplyWorker("SENSITIVE-SLOT")), "apply"),
    (Some(TableWorker("SENSITIVE-SLOT", 987654321U)), "table_sync"),
  ]
  for (workload, expected_role) in workloads {
    for (worker, expected_worker) in workers {
      let reader = @io.MemoryReader(writer => writer.write(b"d\x00\x00\x00\x15"))
      defer reader.close()
      try
        read_frame(reader, 16, role=frame_connection_role(workload, worker))
      catch {
        LimitExceeded(message) => {
          assert_eq(
            message,
            "message frame: type=100 declared_length=21 max_body_bytes=16 role=\{expected_role} worker=\{expected_worker}",
          )
          assert_false(message.contains("SENSITIVE-SLOT"))
          assert_false(message.contains("987654321"))
        }
        error => raise error
      } noraise {
        _ => fail("oversized frame accepted")
      }
    }
  }
}

///|
async test "frame limit metadata reconstructs the consumed header without a body" {
  for header in [b"d\x00\x00\x00\x15", b"\xff\x7f\xff\xff\xff"] {
    let reader = @io.MemoryReader(writer => writer.write(header))
    defer reader.close()
    try read_frame(reader, 16) catch {
      LimitExceeded(message) => {
        let fields = message.split(" ").collect()
        let message_type = @string.parse_int(
          fields[2].split("=").last().unwrap(),
        )
        let declared_length = @string.parse_int(
          fields[3].split("=").last().unwrap(),
        )
        let restored = @buffer.Buffer()
        restored.write_byte(message_type.to_byte())
        restored.write_int_be(declared_length)
        assert_eq(restored.to_bytes(), header)
        assert_eq(reader.read_some(), None)
      }
      error => raise error
    } noraise {
      _ => fail("oversized frame accepted")
    }
  }
}
