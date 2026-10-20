///|
async test "sysinfo cgroup hierarchy differential fixtures" {
  guard @env.get_env_var("MOONCDC_SAMPLER_FIXTURES") is Some(directory) else {
    return
  }
  let input : Array[String] = []
  let actual : Array[String] = []
  for v2 in [true, false] {
    for leaf in ["200", "max", "broken", "18446744073709551615"] {
      for parent in ["500", "max", "broken", "2000"] {
        for used in ["100", "600", "broken"] {
          let root = directory + "/case" + input.length().to_string()
          @fs.mkdir(root + "/parent/child", recursive=true)
          let limit = if v2 { "/memory.max" } else { "/memory.limit_in_bytes" }
          let usage = if v2 {
            "/memory.current"
          } else {
            "/memory.usage_in_bytes"
          }
          @fs.write_file(root + limit, "18446744073709551615")
          @fs.write_file(root + "/parent" + limit, parent)
          @fs.write_file(root + "/parent" + usage, "450")
          @fs.write_file(root + "/parent/child" + limit, leaf)
          @fs.write_file(root + "/parent/child" + usage, used)
          @fs.write_file(
            root + "/parent/child/memory.stat",
            if v2 {
              "anon 30\n"
            } else {
              "total_rss 30\n"
            },
          )
          let mode = if v2 { "v2" } else { "v1" }
          input.push("\{mode}\t\{root}\tparent/child\t2000")
          let result = cgroup_limits(
            read_system_file, root, "parent/child", 2000UL, v2,
          )
          actual.push(
            match result {
              Some((total, free)) => "\{total},\{free}"
              None => "None"
            },
          )
        }
      }
    }
  }
  @fs.write_file(directory + "/input.tsv", input.join("\n") + "\n")
  @fs.write_file(directory + "/actual.txt", actual.join("\n") + "\n")
  assert_eq(input.length(), 96)
  println("SAMPLER generated 96 cgroup hierarchy vectors")
}
