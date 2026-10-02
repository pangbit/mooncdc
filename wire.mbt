///|
priv struct Cursor {
  data : Bytes
  mut pos : Int
}

///|
fn Cursor::new(data : Bytes) -> Cursor {
  { data, pos: 0, }
}

///|
fn Cursor::take(self : Cursor, size : Int) -> Bytes raise {
  if size < 0 || size > self.data.length() - self.pos {
    raise Protocol("truncated or invalid length")
  }
  let result = self.data[self.pos:self.pos + size].to_owned()
  self.pos += size
  result
}

///|
fn Cursor::byte(self : Cursor) -> Byte raise {
  self.take(1)[0]
}

///|
fn Cursor::u16(self : Cursor) -> Int raise {
  let b = self.take(2)
  (b[0].to_int() << 8) | b[1].to_int()
}

///|
fn Cursor::i32(self : Cursor) -> Int raise {
  let b = self.take(4)
  (b[0].to_int() << 24) |
  (b[1].to_int() << 16) |
  (b[2].to_int() << 8) |
  b[3].to_int()
}

///|
fn Cursor::u64(self : Cursor) -> UInt64 raise {
  let hi = self.i32().reinterpret_as_uint().to_uint64()
  (hi << 32) | self.i32().reinterpret_as_uint().to_uint64()
}

///|
fn Cursor::cstring(self : Cursor) -> String raise {
  let start = self.pos
  while self.byte() != b'\x00' {

  }
  @utf8.decode(self.data[start:self.pos - 1], ignore_bom=true)
}

///|
fn Cursor::done(self : Cursor) -> Unit raise {
  if self.pos != self.data.length() {
    raise Protocol("trailing bytes")
  }
}

///|
fn cstring(out : @buffer.Buffer, value : String) -> Unit {
  out.write_string_utf8(value)
  out.write_byte(b'\x00')
}

///|
fn packet(tag : Byte, body : Bytes) -> Bytes {
  let out = @buffer.Buffer()
  out.write_byte(tag)
  out.write_int_be(body.length() + 4)
  out.write_bytes(body)
  out.to_bytes()
}

///|
async fn read_frame(reader : &@io.Reader, limit : Int) -> (Byte, Bytes) {
  let header = reader.read_exactly(5)
  let size = Cursor::new(header[1:].to_owned()).i32()
  if size < 4 {
    raise Protocol("frame length below four")
  }
  if size - 4 > limit {
    raise LimitExceeded("message frame")
  }
  (header[0], reader.read_exactly(size - 4))
}

///|
fn server_error(body : Bytes) -> CdcError raise {
  let c = Cursor::new(body)
  let mut code = ""
  let mut message = ""
  while c.pos < body.length() {
    let tag = c.byte()
    if tag == b'\x00' {
      break
    }
    let value = c.cstring()
    if tag == b'C' {
      code = value
    }
    if tag == b'M' {
      message = value
    }
  }
  c.done()
  Server(code, message)
}
