///|
priv struct ConsumerSchemaStore {}

///|
impl @mooncdc.SchemaStore for ConsumerSchemaStore with fn get(_self, _oid, _id) {
  None
}

///|
impl @mooncdc.SchemaStore for ConsumerSchemaStore with fn all(_self) {
  []
}

///|
impl @mooncdc.SchemaStore for ConsumerSchemaStore with fn put(_self, _schema) {
  ()
}

///|
impl @mooncdc.SchemaStore for ConsumerSchemaStore with fn prune(_self, _bounds) {
  0
}

///|
async test "ClickHouse public destination validates configuration and session lifetime" {
  let target = @mooncdc.ClickHouseDestination::new(
    endpoint="http://127.0.0.1:1",
    user="cdc",
    password="test-secret",
    database="test",
    publication="p",
    schema_store=ConsumerSchemaStore::{ },
  )
  let destination : &@mooncdc.Destination = target
  try destination.startup() catch {
    @mooncdc.InvalidConfig("ClickHouse requires an active StateStore session") =>
      ()
    e => raise e
  } noraise {
    _ => fail("startup accepted missing ownership")
  }
  let state : ConsumerStore = { owner: None, records: Map([]), }
  @mooncdc.with_state_store(state, session => destination.bind_state(session))
  try destination.flush() catch {
    @mooncdc.CheckpointError("state session is closed") => ()
    e => raise e
  } noraise {
    _ => fail("expired state session accepted")
  }
  destination.shutdown()
  for
    endpoint in [
      "ftp://host", "http://user:secret@host", "http://host/?query=secret", "http://host/\r\nX: y",
    ] {
    try
      ignore(
        @mooncdc.ClickHouseDestination::new(
          endpoint~,
          user="cdc",
          password="test-secret",
          database="test",
          publication="p",
          schema_store=ConsumerSchemaStore::{ },
        ),
      )
    catch {
      @mooncdc.InvalidConfig(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid endpoint accepted")
    }
  }
}
