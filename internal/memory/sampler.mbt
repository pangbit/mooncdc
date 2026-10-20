///|
extern "c" fn platform() -> Int = "mooncdc_memory_platform"

///|
#borrow(output)
extern "c" fn host_memory(output : Bytes) -> Int = "mooncdc_host_memory"

///|
fn read_u64_le(bytes : Bytes, offset : Int) -> UInt64 {
  let mut value = 0UL
  for i in 0..<8 {
    value = value | (bytes[offset + i].to_uint64() << (8 * i))
  }
  value
}

///|
async fn read_system_file(path : String) -> String? {
  Some(@utf8.decode(@fs.read_file(path).binary())) catch {
    _ => None
  }
}

///|
/// One coherent domain sample. A lost cgroup read retains that domain rather
/// than expanding to host capacity. Task lifetime and cadence belong to caller.
pub async fn sample(previous : Snapshot?) -> Refresh {
  if platform() == 1 {
    return sample_linux(read_system_file, previous)
  }
  let output = Bytes::make(16, b'\x00')
  if host_memory(output) == 1 {
    return {
      snapshot: {
        used: read_u64_le(output, 0),
        total: read_u64_le(output, 8),
        source: System,
      },
      fresh: true,
    }
  }
  unavailable(previous)
}

///|
fn unavailable(previous : Snapshot?) -> Refresh {
  {
    snapshot: previous.unwrap_or({ used: 0UL, total: 0UL, source: System, }),
    fresh: false,
  }
}

///|
async fn sample_linux(
  read : async (String) -> String?,
  previous : Snapshot?,
) -> Refresh {
  let host = match read("/proc/meminfo").bind(s => linux_host(s)) {
    Some(host) => host
    None => return unavailable(previous)
  }
  let path = read("/proc/self/cgroup").bind(s => cgroup_path(s))
  let process = match path {
    Some(path) => linux_limits(read, path, host.total)
    None => None
  }
  if process is Some(_) || previous is Some({ source: ProcessCgroup, .. }) {
    return select_snapshot(host, process, None, previous)
  }
  let root = linux_limits(read, "", host.total)
  select_snapshot(host, None, root, previous)
}
