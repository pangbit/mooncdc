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
}
