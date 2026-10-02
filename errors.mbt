///|
/// Failures which require caller intervention; transport errors retain their original type.
pub suberror CdcError {
  Protocol(String)
  Authentication(String)
  Server(String, String)
  InvalidConfig(String)
  LimitExceeded(String)
  UnsafeResume(String)
  CheckpointError(String)
  Closed
} derive(Debug)
