///|
priv struct Decoder {
  relations : Map[UInt, Relation]
  types : Map[UInt, TypeInfo]
  mut xid : UInt?
  mut final_lsn : UInt64
  mut time : Int64
  mut changes : Array[Change]
  mut bytes : Int
  limit : Int
  event_limit : Int
  metadata_limit : Int
}

///|
fn Decoder::new(
  limit : Int,
  event_limit : Int,
  metadata_limit : Int,
) -> Decoder {
  {
    relations: Map([]),
    types: Map([]),
    xid: None,
    final_lsn: 0,
    time: 0,
    changes: [],
    bytes: 0,
    limit,
    event_limit,
    metadata_limit,
  }
}

///|
fn Decoder::relation(self : Decoder, oid : UInt) -> Relation raise {
  guard self.relations.get(oid) is Some(r) else {
    raise Protocol("unknown relation OID")
  }
  r
}

///|
fn Cursor::tuple(
  self : Cursor,
  relation : Relation,
) -> ReadOnlyArray[Value] raise {
  let n = self.u16()
  if n != relation.columns.length() {
    raise Protocol("tuple column count mismatch")
  }
  ReadOnlyArray::makei(n, _ => {
    match self.byte() {
      b'n' => Null
      b'u' => UnchangedToast
      b't' => Text(@utf8.decode(self.take(self.i32()), ignore_bom=true))
      b'b' => Binary(self.take(self.i32()))
      _ => raise Protocol("unknown tuple field")
    }
  })
}

///|
fn Cursor::old_tuple(
  self : Cursor,
  tag : Byte,
  relation : Relation,
) -> OldTuple raise {
  match tag {
    b'K' => Key(self.tuple(relation))
    b'O' => Full(self.tuple(relation))
    _ => raise Protocol("invalid old tuple tag")
  }
}

///|
fn Decoder::decode(
  self : Decoder,
  data : Bytes,
) -> (UInt, Lsn, Lsn, Int64, ReadOnlyArray[Change])? raise {
  let c = Cursor::new(data)
  let tag = c.byte()
  if self.bytes > self.limit - data.length() {
    raise LimitExceeded("transaction bytes")
  }
  self.bytes += data.length()
  let mut result = None
  match tag {
    b'B' => {
      if self.xid is Some(_) {
        raise Protocol("nested Begin")
      }
      self.final_lsn = c.u64()
      self.time = c.u64().reinterpret_as_int64()
      self.xid = Some(c.i32().reinterpret_as_uint())
    }
    b'C' => {
      guard self.xid is Some(xid) else {
        raise Protocol("Commit without Begin")
      }
      if c.byte() != b'\x00' {
        raise Protocol("Commit flags")
      }
      let commit_lsn = c.u64()
      let end_lsn = c.u64()
      let time = c.u64().reinterpret_as_int64()
      if commit_lsn != self.final_lsn ||
        end_lsn < commit_lsn ||
        time != self.time {
        raise Protocol("inconsistent commit boundary")
      }
      result = Some(
        (
          xid,
          { value: commit_lsn, },
          { value: end_lsn, },
          time,
          ReadOnlyArray::from_array(self.changes),
        ),
      )
      self.xid = None
      self.changes = []
      self.bytes = 0
    }
    _ => {
      if self.xid is None {
        raise Protocol("change outside transaction")
      }
      if self.changes.length() >= self.event_limit {
        raise LimitExceeded("transaction events")
      }
      let change = match tag {
        b'R' => {
          let oid = c.i32().reinterpret_as_uint()
          let namespace_ = c.cstring()
          let name = c.cstring()
          let identity = c.byte()
          if identity != b'd' &&
            identity != b'n' &&
            identity != b'f' &&
            identity != b'i' {
            raise Protocol("replica identity")
          }
          let columns = ReadOnlyArray::makei(c.u16(), _ => {
            let flags = c.byte()
            if flags != b'\x00' && flags != b'\x01' {
              raise Protocol("column flags")
            }
            Column::{
              name: c.cstring(),
              type_oid: c.i32().reinterpret_as_uint(),
              type_modifier: c.i32(),
              key: flags == b'\x01',
            }
          })
          let r : Relation = {
            oid,
            namespace_,
            name,
            replica_identity: identity,
            columns,
          }
          if !self.relations.contains(oid) &&
            self.relations.length() >= self.metadata_limit {
            raise LimitExceeded("relations")
          }
          self.relations[oid] = r
          RelationChanged(r)
        }
        b'Y' => {
          let t : TypeInfo = {
            oid: c.i32().reinterpret_as_uint(),
            namespace_: c.cstring(),
            name: c.cstring(),
          }
          if !self.types.contains(t.oid) &&
            self.types.length() >= self.metadata_limit {
            raise LimitExceeded("types")
          }
          self.types[t.oid] = t
          TypeChanged(t)
        }
        b'I' => {
          let r = self.relation(c.i32().reinterpret_as_uint())
          if c.byte() != b'N' {
            raise Protocol("Insert tuple tag")
          }
          Insert(r, c.tuple(r))
        }
        b'U' => {
          let r = self.relation(c.i32().reinterpret_as_uint())
          let tag = c.byte()
          let old = if tag == b'N' {
            None
          } else {
            let old = c.old_tuple(tag, r)
            if c.byte() != b'N' {
              raise Protocol("Update new tuple tag")
            }
            Some(old)
          }
          Update(r, old, c.tuple(r))
        }
        b'D' => {
          let r = self.relation(c.i32().reinterpret_as_uint())
          Delete(r, c.old_tuple(c.byte(), r))
        }
        b'T' => {
          let n = c.i32()
          if n < 0 || n > (data.length() - c.pos - 1) / 4 {
            raise Protocol("Truncate relation count")
          }
          let flags = c.byte().to_int()
          if (flags & 0xfc) != 0 {
            raise Protocol("Truncate flags")
          }
          let rs = ReadOnlyArray::makei(n, _ => {
            self.relation(c.i32().reinterpret_as_uint())
          })
          Truncate(
            rs,
            cascade=(flags & 1) != 0,
            restart_identity=(flags & 2) != 0,
          )
        }
        b'O' => Origin({ value: c.u64(), }, c.cstring())
        _ => raise Protocol("unsupported pgoutput message \{tag.to_int()}")
      }
      self.changes.push(change)
    }
  }
  c.done()
  result
}
