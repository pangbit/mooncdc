///|
#borrow(text, status)
extern "c" fn parse_float_ffi(
  text : Bytes,
  single : Int32,
  status : FixedArray[Int32],
) -> Double = "mooncdc_parse_float"

///|
fn pg_float(text : String, single~ : Bool) -> Double raise {
  lexmatch text.to_lower() {
    re"^(\+|-)?(([0-9]+(\.[0-9]*)?|\.[0-9]+)(e(\+|-)?[0-9]+)?|inf(inity)?|nan)$" =>
      ()
    _ => raise Conversion("invalid floating point value")
  }
  let status : FixedArray[Int32] = [0]
  let value = parse_float_ffi(
    @utf8.encode(text),
    if single {
      1
    } else {
      0
    },
    status,
  )
  if status[0] != 0 {
    raise Conversion("native floating point conversion failed")
  }
  value
}
