///|
async test "frame splits at every boundary" {
  let encoded = packet(b'd', b"hello") + packet(b'Z', b"I")
  for split in 1..<encoded.length() {
    let reader = @io.MemoryReader(writer => {
      writer.write(encoded[:split])
      @async.pause()
      writer.write(encoded[split:])
    })
    defer reader.close()
    assert_eq(read_frame(reader, 32), (b'd', b"hello"))
    assert_eq(read_frame(reader, 32), (b'Z', b"I"))
  }
}

///|
async test "truncated header body invalid and excessive lengths fail" {
  for
    bytes in [
      b"d", b"d\x00\x00\x00\x03", b"d\x7f\xff\xff\xff", b"d\x00\x00\x00\x06x",
    ] {
    let reader = @io.MemoryReader(writer => writer.write(bytes))
    defer reader.close()
    try read_frame(reader, 16) catch {
      _ => ()
    } noraise {
      _ => fail("expected frame error")
    }
  }
}

///|
test "SCRAM rejects nonce mismatch duplicate mandatory and weak iteration" {
  for
    challenge in [
      "r=wrong,s=eA==,i=4096", "r=noncex,r=noncey,s=eA==,i=4096", "m=x,r=noncex,s=eA==,i=4096",
      "r=noncex,s=eA==,i=1",
    ] {
    try scram_response("secret", "nonce", challenge) catch {
      _ => ()
    } noraise {
      _ => fail("expected authentication error")
    }
  }
}
