///|
test "Linux host bytes and old kernel availability" {
  assert_eq(
    linux_host("MemTotal: 2000 kB\nMemAvailable: 500 kB\n"),
    Some({ used: 1536000UL, total: 2048000UL, source: System, }),
  )
  assert_eq(
    linux_host(
      "MemTotal: 100 kB\nMemFree: 10 kB\nBuffers: 5 kB\nCached: 20 kB\nSReclaimable: 3 kB\nShmem: 8 kB\n",
    ),
    Some({ used: 71680UL, total: 102400UL, source: System, }),
  )
  assert_eq(
    linux_host(
      "MemTotal: 18446744073709551615 kB\nMemAvailable: 18446744073709551615 kB",
    ),
    Some({ used: 0UL, total: 0xffffffffffffffffUL, source: System, }),
  )
  assert_eq(linux_host("MemTotal: broken\n"), None)
  for invalid in ["1_000", "0xff", "-1", "18446744073709551616", ""] {
    assert_eq(decimal(invalid), None)
  }
  assert_eq(decimal(" +10\n"), Some(10UL))
}

///|
test "cgroup membership preserves colons and chooses memory controller" {
  assert_eq(cgroup_path("0::/user.slice/a:b\n"), Some("user.slice/a:b"))
  assert_eq(
    cgroup_path("12:cpu:/\n11:cpuset,memory:/parent/child\n"),
    Some("parent/child"),
  )
  assert_eq(cgroup_path("12:cpu:/\n"), None)
  assert_eq(cgroup_path("bad\n0::/valid"), None)
  assert_eq(cgroup_path("0::relative"), None)
  assert_eq(cgroup_path("0::/"), Some(""))
}

///|
async test "cgroup hierarchy minimizes capacity and headroom independently" {
  for v2 in [true, false] {
    let limit = if v2 { "/memory.max" } else { "/memory.limit_in_bytes" }
    let usage = if v2 { "/memory.current" } else { "/memory.usage_in_bytes" }
    let stat = if v2 { "anon 30\n" } else { "total_rss 30\n" }
    let files : Map[String, String] = Map([
      ("/root/parent/child" + limit, "200"),
      ("/root/parent/child" + usage, "100"),
      ("/root/parent/child/memory.stat", stat),
      ("/root/parent" + limit, "500"),
      ("/root/parent" + usage, "450"),
      ("/root" + limit, "18446744073709551615"),
    ])
    let read : async (String) -> String? = path => files.get(path)
    assert_eq(
      cgroup_limits(read, "/root", "parent/child", 2000UL, v2),
      Some((200UL, 50UL)),
    )
    files["/root/parent/child" + limit] = "18446744073709551615"
    assert_eq(
      cgroup_limits(read, "/root", "parent/child", 2000UL, v2),
      Some((500UL, 50UL)),
    )
    files["/root/parent" + usage] = "550"
    assert_eq(
      cgroup_limits(read, "/root", "parent/child", 2000UL, v2),
      Some((500UL, 0UL)),
    )
    files["/root/parent" + usage] = "broken"
    assert_eq(cgroup_limits(read, "/root", "parent/child", 2000UL, v2), None)
    files["/root/parent" + limit] = "broken"
    assert_eq(
      cgroup_limits(read, "/root", "parent/child", 2000UL, v2),
      Some((2000UL, 1900UL)),
    )
    files["/root/parent/child/memory.stat"] = "other 100"
    assert_eq(cgroup_limits(read, "/root", "parent/child", 2000UL, v2), None)
    assert_eq(cgroup_limits(read, "/root", "../outside", 2000UL, v2), None)
  }
}

///|
test "domain selection retains failed constrained reads without broadening budget" {
  let host : Snapshot = { used: 100UL, total: 2000UL, source: System, }
  let process : Snapshot = { used: 450UL, total: 500UL, source: ProcessCgroup, }
  let root : Snapshot = { used: 700UL, total: 1000UL, source: RootCgroup, }
  assert_eq(
    select_snapshot(host, Some((500UL, 50UL)), Some((1000UL, 300UL)), None),
    { snapshot: process, fresh: true, },
  )
  assert_eq(select_snapshot(host, None, Some((1000UL, 300UL)), Some(process)), {
    snapshot: process,
    fresh: false,
  })
  assert_eq(select_snapshot(host, None, None, Some(root)), {
    snapshot: root,
    fresh: false,
  })
  assert_eq(select_snapshot(host, None, Some((1000UL, 300UL)), None), {
    snapshot: root,
    fresh: true,
  })
  // A successful unlimited process read deliberately returns host before root.
  assert_eq(
    select_snapshot(
      host,
      Some((2000UL, 1000UL)),
      Some((1000UL, 300UL)),
      Some(process),
    ),
    { snapshot: host, fresh: true, },
  )
  assert_eq(select_snapshot(host, None, None, None), {
    snapshot: host,
    fresh: true,
  })
}

///|
async test "sampler uses full cgroup charge and retains failed host or process reads" {
  let files : Map[String, String] = {
    "/proc/meminfo": "MemTotal: 2 kB\nMemAvailable: 1 kB\n",
    "/proc/self/cgroup": "0::/pod/worker\n",
    "/sys/fs/cgroup/pod/worker/memory.max": "max",
    "/sys/fs/cgroup/pod/worker/memory.current": "100",
    "/sys/fs/cgroup/pod/worker/memory.stat": "anon 1\nfile 99\n",
    "/sys/fs/cgroup/pod/memory.max": "500",
    "/sys/fs/cgroup/pod/memory.current": "450",
  }
  let read : async (String) -> String? = path => files.get(path)
  let first = sample_linux(read, None)
  assert_eq(first, {
    snapshot: { used: 450UL, total: 500UL, source: ProcessCgroup, },
    fresh: true,
  })
  files["/sys/fs/cgroup/pod/worker/memory.current"] = "invalid"
  assert_eq(sample_linux(read, Some(first.snapshot)), {
    snapshot: first.snapshot,
    fresh: false,
  })
  files["/proc/meminfo"] = "invalid"
  assert_eq(sample_linux(read, Some(first.snapshot)), {
    snapshot: first.snapshot,
    fresh: false,
  })
  assert_eq(sample_linux(read, None), {
    snapshot: { used: 0UL, total: 0UL, source: System, },
    fresh: false,
  })
}

///|
async test "native host sampling yields a real bounded memory domain" {
  if platform() == 0 {
    return
  }
  for _ in 0..<20 {
    let reading = sample(None)
    assert_true(reading.fresh)
    assert_true(reading.snapshot.total > 0UL)
    assert_true(reading.snapshot.used <= reading.snapshot.total)
    if platform() == 2 {
      assert_eq(reading.snapshot.source, System)
    }
  }
}
