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
