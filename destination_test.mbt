///|
priv struct PublicDestination {
  mut flushed : Bool
}

///|
impl @mooncdc.Destination for PublicDestination with fn reset_table(
  _self,
  _relation,
  attempt~,
) {
  assert_true(!attempt.is_empty())
}

///|
impl @mooncdc.Destination for PublicDestination with fn write_rows(
  _self,
  _relation,
  _rows,
  _id,
) {
  @mooncdc.Accepted
}

///|
impl @mooncdc.Destination for PublicDestination with fn write_transaction(
  _self,
  _tx,
) {
  @mooncdc.Accepted
}

///|
impl @mooncdc.Destination for PublicDestination with fn flush(self) {
  self.flushed = true
}

///|
async test "custom destination can implement public contract with default lifecycle" {
  let destination = PublicDestination::{ flushed: false, }
  @mooncdc.with_destination(destination, () => {
    let target : &@mooncdc.Destination = destination
    target.flush()
  })
  assert_true(destination.flushed)
  let control = @mooncdc.ApplyControl()
  control.stop()
  control.stop()
}

///|
async test "pipeline public configuration rejects limits before connecting" {
  let destination = PublicDestination::{ flushed: false, }
  let config : @mooncdc.ConnectionConfig = {
    host: "127.0.0.1",
    port: 1,
    user: "user",
    password: "",
    database: "db",
    security: @mooncdc.LocalPlaintext,
    timeout_ms: 10,
  }
  for concurrency in [0, 17] {
    try
      @mooncdc.run_pipeline(
        config,
        destination,
        publication="pub",
        destination_id="target",
        state_path="unused",
        control=@mooncdc.ApplyControl(),
        copy_concurrency=concurrency,
      )
    catch {
      @mooncdc.InvalidConfig(_) => ()
      e => raise e
    } noraise {
      _ => fail("invalid pipeline limit accepted")
    }
  }
  let phase : @mooncdc.TableSyncPhase = @mooncdc.Catchup(
    @mooncdc.Lsn::parse("0/10"),
  )
  assert_eq(phase, @mooncdc.Catchup(@mooncdc.Lsn::parse("0/10")))
}
