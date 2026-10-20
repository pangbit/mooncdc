///|
async test "external destinations can fence an escaped state session" {
  let store : ConsumerStore = { owner: None, records: Map([]), }
  let escaped = @mooncdc.with_state_store(store, session => {
    session.check()
    session
  })
  try escaped.check() catch {
    @mooncdc.CheckpointError(message) =>
      assert_eq(message, "state session is closed")
    error => raise error
  } noraise {
    _ => fail("closed session accepted an external side effect")
  }
}

///|
priv struct ConsumerStore {
  mut owner : String?
  records : Map[String, Bytes]
}

///|
fn record_key(key : @mooncdc.StateRecord) -> String {
  match key {
    Pipeline => "pipeline"
    ReplicationCheckpoint => "checkpoint"
    Schemas => "schemas"
    TableMetadata(oid) => oid.to_string()
  }
}

///|
impl @mooncdc.StateStore for ConsumerStore with fn acquire(self, owner) {
  assert_true(self.owner is None)
  self.owner = Some(owner)
}

///|
impl @mooncdc.StateStore for ConsumerStore with fn release(self, owner) {
  assert_eq(self.owner, Some(owner))
  self.owner = None
}

///|
impl @mooncdc.StateStore for ConsumerStore with fn load(self, key) {
  self.records.get(record_key(key))
}

///|
impl @mooncdc.StateStore for ConsumerStore with fn save(self, owner, key, bytes) {
  assert_eq(self.owner, Some(owner))
  self.records[record_key(key)] = bytes
}

///|
async test "custom StateStore implements public ownership and durable metadata API" {
  let store : ConsumerStore = { owner: None, records: Map([]), }
  @mooncdc.with_state_store(store, session => {
    assert_true(session.get_metadata(42U) is None)
    session.put_metadata(42U, { "target": "test", "generation": "1" })
  })
  assert_true(store.owner is None)
  @mooncdc.with_state_store(store, session => {
    assert_eq(
      session.get_metadata(42U),
      Some({ "target": "test", "generation": "1" }),
    )
  })
  assert_true(@mooncdc.state_store_status(store) is None)
  let escaped = @mooncdc.with_state_schema_store(store, identity="test", schemas => {
    assert_eq(schemas.all().length(), 0)
    schemas
  })
  assert_true(store.records.contains("schemas"))
  assert_true(store.owner is None)
  try escaped.all() catch {
    @mooncdc.Closed => ()
    error => raise error
  } noraise {
    _ => fail("escaped schema store remained open")
  }
}
