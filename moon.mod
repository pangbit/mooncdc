// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "pangbit/mooncdc"

version = "0.1.0"

readme = "README.mbt.md"

repository = "https://github.com/pangbit/mooncdc"

license = "Apache-2.0"

keywords = [ "postgresql", "cdc", "replication" ]

preferred_target = "native"

supported_targets = "native"

description = "PostgreSQL pgoutput CDC with committed transactions and durable acknowledgements"

import {
  "moonbitlang/async@0.22.4",
  "moonbitlang/x@0.5.5",
}
