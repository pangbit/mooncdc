///|
// Decimal syntax follows Rust u64 parsing, including an optional leading plus.
fn decimal(text : StringView) -> UInt64? {
  let text = text.trim()
  let digits = if text.has_prefix("+") { text[1:] } else { text }
  if digits.is_empty() || !digits.iter().all(c => c >= '0' && c <= '9') {
    return None
  }
  Some(@string.parse_uint64(digits, base=10)) catch {
    _ => None
  }
}

///|
fn table(text : StringView, separator : StringView) -> Map[String, UInt64] {
  let values = Map([])
  for line in text.split("\n") {
    let columns = line.split(separator)
    if (columns.next(), columns.next()) is (Some(key), Some(value)) {
      let token = value.trim_start().split(" ").next().unwrap_or("")
      if decimal(token) is Some(number) {
        values[key.to_owned()] = number
      }
    }
  }
  values
}

///|
fn linux_host(text : StringView) -> Snapshot? {
  let fields = table(text, ":")
  for key, value in fields {
    fields[key] = if value > 0xffffffffffffffffUL / 1024UL {
      0xffffffffffffffffUL
    } else {
      value * 1024UL
    }
  }
  let total = match fields.get("MemTotal") {
    Some(v) => v
    None => return None
  }
  let available = fields
    .get("MemAvailable")
    .unwrap_or_else(() => {
      let free = saturating_add(
        fields.get("MemFree").unwrap_or(0UL),
        fields.get("Buffers").unwrap_or(0UL),
      )
      let free = saturating_add(free, fields.get("Cached").unwrap_or(0UL))
      let free = saturating_add(free, fields.get("SReclaimable").unwrap_or(0UL))
      saturating_sub(free, fields.get("Shmem").unwrap_or(0UL))
    })
  Some({ used: saturating_sub(total, available), total, source: System, })
}

///|
fn cgroup_path(text : StringView) -> String? {
  for line in text.split("\n") {
    let (id, rest) = match line.split_once(":") {
      Some(v) => v
      None => return None
    }
    let (controllers, path) = match rest.split_once(":") {
      Some(v) => v
      None => return None
    }
    if (id == "0" && controllers.is_empty()) ||
      controllers.split(",").any(c => c == "memory") {
      if !path.has_prefix("/") {
        return None
      }
      // Kernel paths can contain colons. Keep them after the second separator.
      return Some(path[1:].to_owned())
    }
  }
  None
}

///|
async fn cgroup_limits(
  read : async (String) -> String?,
  root : String,
  relative : String,
  total : UInt64,
  v2 : Bool,
) -> (UInt64, UInt64)? {
  let limit_file = if v2 { "/memory.max" } else { "/memory.limit_in_bytes" }
  let usage_file = if v2 { "/memory.current" } else { "/memory.usage_in_bytes" }
  let parts = relative
    .split("/")
    .filter(p => !p.is_empty() && p != ".")
    .map(p => p.to_owned())
    .to_array()
  let base = if parts.is_empty() { root } else { root + "/" + parts.join("/") }
  let leaf_limit = read(base + limit_file).bind(s => decimal(s))
  if !v2 && leaf_limit is None {
    return None
  }
  let leaf_used = match read(base + usage_file).bind(s => decimal(s)) {
    Some(v) => v
    None => return None
  }
  let mut capacity = total
  let mut free = saturating_sub(total, leaf_used)
  for n = parts.length(); n >= 0; n = n - 1 {
    let path = if n == 0 {
      root
    } else {
      root + "/" + parts[:n].to_owned().join("/")
    }
    let limit = if n == parts.length() {
      leaf_limit
    } else {
      read(path + limit_file).bind(s => decimal(s))
    }
    let limit = limit.unwrap_or(0xffffffffffffffffUL)
    if limit <= total {
      let used = if n == parts.length() {
        leaf_used
      } else {
        match read(path + usage_file).bind(s => decimal(s)) {
          Some(v) => v
          None => return None
        }
      }
      if limit < capacity {
        capacity = limit
      }
      let headroom = saturating_sub(limit, used)
      if headroom < free {
        free = headroom
      }
    }
  }
  let stat = match read(base + "/memory.stat") {
    Some(v) => v
    None => return None
  }
  let key = if v2 { "anon" } else { "total_rss" }
  if !table(stat, " ").contains(key) {
    return None
  }
  Some((capacity, free))
}

///|
async fn linux_limits(
  read : async (String) -> String?,
  path : String,
  total : UInt64,
) -> (UInt64, UInt64)? {
  match cgroup_limits(read, "/sys/fs/cgroup", path, total, true) {
    Some(v) => Some(v)
    None => cgroup_limits(read, "/sys/fs/cgroup/memory", path, total, false)
  }
}
