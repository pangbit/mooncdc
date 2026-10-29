<a id="dependencies-and-references"></a>

# 依赖与参照

<!-- language-switch -->
[English](THIRD_PARTY.md) | [简体中文](THIRD_PARTY.zh-CN.md)

英文版本为权威版本。


- [moonbitlang/core](https://github.com/moonbitlang/core)，Apache-2.0：
  标准集合、二进制编码和 JSON。
- [moonbitlang/async](https://github.com/moonbitlang/async)，Apache-2.0：
  native TCP、结构化取消、TLS 和文件 I/O。TLS 使用 macOS/Linux 的系统 OpenSSL 运行时，
  本项目未内置 OpenSSL 源码。
- [moonbitlang/x](https://github.com/moonbitlang/x)，Apache-2.0：
  SHA-256 和 HMAC。版本记录在 `moon.mod`。
- [PostgreSQL 流复制协议](https://www.postgresql.org/docs/18/protocol-replication.html)。
- [PostgreSQL 逻辑消息格式](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html)。
- [PostgreSQL SASL 认证](https://www.postgresql.org/docs/18/sasl-authentication.html)。
- [RFC 7677 SCRAM-SHA-256](https://www.rfc-editor.org/rfc/rfc7677)。

`integration/reference/etl/` 保留来自
[Supabase ETL](https://github.com/supabase/etl/tree/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4)
的两份未修改 DDL 迁移源码及其 Apache-2.0 许可证。它们为隔离测试提供真实上游输入，
并由仓库内的 `.mbtx` 生成器原样嵌入 `source_migrations_sql.mbt`。
可选迁移器遵循 SQLx 0.9.0 的 PostgreSQL 锁、历史和校验和格式，实现由 MoonBit 独立编写。

`tools/codec-reference-test.mbtx` 下载相同的固定 ETL 提交，验证归档哈希，并使用上游
lockfile 构建未修改的 codec。`integration/reference/codec_driver.rs` 和 `schema_driver.rs`
是比较夹具，只调用上游 codec/planner，不复制它们的实现。
生产浮点转换使用带私有 C locale 的系统 C 运行时。

<a id="implementation-provenance"></a>

## 实现来源

`internal/saslprep/` 的 SCRAM 密码准备逻辑改写自 PostgreSQL 18.0 的 SASLprep 和
NFKC 算法，包含完整 Unicode 规范化表及 RFC3454 范围表，没有新增运行时依赖。
固定源文件、SHA-256、PostgreSQL 许可证与 Unicode License V3 保存在
[`internal/saslprep/data/`](internal/saslprep/data/README.md)。

其他生产代码依据协议参照独立实现，没有复制或改写其他 PostgreSQL 客户端或 Mooncakes CDC 包。
可执行示例的包布局参考了官方 `moonbitlang/async` 示例。
