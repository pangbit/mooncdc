///|
priv struct StreamBuffer {
  mut changes : Array[Change]
  // First change offset, in arrival order rather than numeric XID order.
  mut subtransactions : Array[(UInt, Int)]
  mut bytes : Int
  mut events : Int
}

///|
// v2 is a separate whole-transaction collector. Its provisional records never
// enter a Subscription queue or become an acknowledgement position.
priv struct StreamDecoder {
  decoder : Decoder
  streams : Map[UInt, StreamBuffer]
  mut active : UInt?
  mut bytes : Int
  mut events : Int
  stream_limit : Int
}

///|
fn StreamDecoder::new(decoder : Decoder, stream_limit : Int) -> StreamDecoder {
  {
    decoder,
    streams: Map([]),
    active: None,
    bytes: 0,
    events: 0,
    stream_limit,
  }
}

///|
fn StreamDecoder::charge(
  self : StreamDecoder,
  stream : StreamBuffer,
  count : Int,
) -> Unit raise {
  if stream.bytes > self.decoder.limit - count ||
    self.bytes > self.decoder.limit - count {
    raise LimitExceeded("streamed transaction bytes")
  }
  stream.bytes += count
  self.bytes += count
}

///|
fn StreamDecoder::decode(
  self : StreamDecoder,
  data : Bytes,
) -> (UInt, Lsn, Lsn, Int64, ReadOnlyArray[Change])? raise {
  let c = Cursor::new(data)
  let tag = c.byte()
  match tag {
    b'S' => {
      if self.active is Some(_) || self.decoder.xid is Some(_) {
        raise Protocol("Stream Start inside transaction")
      }
      let xid = c.i32().reinterpret_as_uint()
      let first = c.byte()
      c.done()
      if xid == 0 || (first != b'\x00' && first != b'\x01') {
        raise Protocol("Stream Start XID or flag")
      }
      let stream = if first == b'\x01' {
        if self.streams.contains(xid) {
          raise Protocol("duplicate first stream segment")
        }
        if self.streams.length() >= self.stream_limit {
          raise LimitExceeded("in-progress streamed transactions")
        }
        let stream : StreamBuffer = {
          changes: [],
          subtransactions: [],
          bytes: 0,
          events: 0,
        }
        self.streams[xid] = stream
        stream
      } else {
        guard self.streams.get(xid) is Some(stream) else {
          raise Protocol("unknown continuation stream")
        }
        stream
      }
      self.charge(stream, data.length())
      self.active = Some(xid)
      None
    }
    b'E' => {
      c.done()
      guard self.active is Some(xid) else {
        raise Protocol("Stream Stop without Start")
      }
      self.charge(self.streams[xid], data.length())
      self.active = None
      None
    }
    b'c' | b'A' => {
      if self.active is Some(_) || self.decoder.xid is Some(_) {
        raise Protocol("stream boundary inside transaction")
      }
      let xid = c.i32().reinterpret_as_uint()
      guard self.streams.get(xid) is Some(stream) else {
        raise Protocol("unknown stream boundary XID")
      }
      if tag == b'A' {
        let subxid = c.i32().reinterpret_as_uint()
        c.done()
        if subxid == 0 {
          raise Protocol("Stream Abort subtransaction XID")
        }
        self.charge(stream, data.length())
        if subxid == xid {
          self.bytes -= stream.bytes
          self.events -= stream.events
          self.streams.remove(xid)
        } else {
          // PostgreSQL truncates to the first record of this subtransaction,
          // also removing later records/subtransactions. Unknown empty aborts
          // are harmless. Do not reclaim cumulative wire quotas on subabort.
          for index = stream.subtransactions.length() - 1
              index >= 0
              index = index - 1 {
            if stream.subtransactions[index] is (seen, offset) && seen == subxid {
              stream.changes = stream.changes[:offset].to_owned()
              stream.subtransactions = stream.subtransactions[:index].to_owned()
              break
            }
          }
        }
        None
      } else {
        if c.byte() != b'\x00' {
          raise Protocol("Stream Commit flags")
        }
        let commit_lsn = c.u64()
        let end_lsn = c.u64()
        let time = c.u64().reinterpret_as_int64()
        c.done()
        if commit_lsn == 0 || end_lsn <= commit_lsn {
          raise Protocol("Stream Commit boundary")
        }
        self.charge(stream, data.length())
        self.bytes -= stream.bytes
        self.events -= stream.events
        self.streams.remove(xid)
        Some(
          (
            xid,
            { value: commit_lsn, },
            { value: end_lsn, },
            time,
            ReadOnlyArray::from_array(stream.changes),
          ),
        )
      }
    }
    _ =>
      match self.active {
        Some(root) => {
          let stream = self.streams[root]
          self.charge(stream, data.length())
          if stream.events >= self.decoder.event_limit ||
            self.events >= self.decoder.event_limit {
            raise LimitExceeded("streamed transaction events")
          }
          let (xid, payload) = match tag {
            b'R' | b'Y' | b'I' | b'U' | b'D' | b'T' | b'M' => {
              let xid = c.i32().reinterpret_as_uint()
              if xid == 0 {
                raise Protocol("streamed change XID")
              }
              (xid, Bytes::make(1, tag) + data[c.pos:].to_owned())
            }
            b'O' => (root, data)
            _ => raise Protocol("unsupported message in stream segment")
          }
          if xid != root && !stream.subtransactions.any(pair => pair.0 == xid) {
            stream.subtransactions.push((xid, stream.changes.length()))
          }
          // Reuse the strict v1 body parser and global metadata cache. No
          // provisional header is exposed; Stream Commit supplies the real one.
          self.decoder.xid = Some(root)
          self.decoder.bytes = 0
          self.decoder.events = 0
          self.decoder.ordinal = 0
          defer {
            self.decoder.xid = None
            self.decoder.bytes = 0
            self.decoder.events = 0
            self.decoder.ordinal = 0
          }
          guard self.decoder.decode_event(payload) is Change(_, change, _) else {
            raise Protocol("streamed change expected")
          }
          stream.changes.push(change)
          stream.events += 1
          self.events += 1
          None
        }
        None => {
          // Interleaved ordinary transactions share the aggregate cap with all
          // open stream buffers. Their Begin/Commit wire contract stays v1.
          if self.bytes >
            self.decoder.limit - self.decoder.bytes - data.length() {
            raise LimitExceeded("aggregate transaction bytes")
          }
          if tag != b'B' &&
            tag != b'C' &&
            self.events >= self.decoder.event_limit - self.decoder.events {
            raise LimitExceeded("aggregate transaction events")
          }
          self.decoder.decode(data)
        }
      }
  }
}
