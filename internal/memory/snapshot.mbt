///|
pub(all) enum Source {
  System
  ProcessCgroup
  RootCgroup
} derive(Eq, Debug)

///|
pub extend Source with Eq::{equal, not_equal}

///|
pub extend Source with Debug::{to_repr}

///|
pub(all) struct Snapshot {
  used : UInt64
  total : UInt64
  source : Source
} derive(Eq, Debug)

///|
pub extend Snapshot with Eq::{equal, not_equal}

///|
pub extend Snapshot with Debug::{to_repr}

///|
pub(all) struct Refresh {
  snapshot : Snapshot
  fresh : Bool
} derive(Eq, Debug)

///|
pub extend Refresh with Eq::{equal, not_equal}

///|
pub extend Refresh with Debug::{to_repr}

///|
fn saturating_sub(a : UInt64, b : UInt64) -> UInt64 {
  if a > b {
    a - b
  } else {
    0UL
  }
}

///|
fn saturating_add(a : UInt64, b : UInt64) -> UInt64 {
  if 0xffffffffffffffffUL - a < b {
    0xffffffffffffffffUL
  } else {
    a + b
  }
}

///|
fn constrained(
  host : Snapshot,
  limits : (UInt64, UInt64),
  source : Source,
) -> Snapshot {
  let (total, free) = limits
  if total < host.total {
    { used: saturating_sub(total, free), total, source, }
  } else {
    host
  }
}

///|
fn select_snapshot(
  host : Snapshot,
  process : (UInt64, UInt64)?,
  root : (UInt64, UInt64)?,
  previous : Snapshot?,
) -> Refresh {
  if process is Some(limits) {
    return { snapshot: constrained(host, limits, ProcessCgroup), fresh: true, }
  }
  if previous is Some({ source: ProcessCgroup, .. } as snapshot) {
    return { snapshot, fresh: false, }
  }
  if root is Some(limits) {
    return { snapshot: constrained(host, limits, RootCgroup), fresh: true, }
  }
  if previous is Some({ source: RootCgroup, .. } as snapshot) {
    return { snapshot, fresh: false, }
  }
  { snapshot: host, fresh: true, }
}
