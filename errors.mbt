///|
/// Failures which require caller intervention; transport errors retain their original type.
#derive(Debug)
pub(all) suberror CdcError {
  Protocol(String)
  Conversion(String)
  Authentication(String)
  Server(String, String)
  InvalidConfig(String)
  LimitExceeded(String)
  UnsafeResume(String)
  CheckpointError(String)
  Closed
}
