<a id="reference-tests-and-retained-contracts"></a>

# 参照测试与保留契约

<!-- language-switch -->
[English](REFERENCE_TESTS.md) | [简体中文](REFERENCE_TESTS.zh-CN.md)

英文版本为权威版本。


生产代码独立实现 PostgreSQL 线协议和复制格式。维护中的参照 runner 对照固定上游实现
比较解析值和错误，不把客户端实现复制到生产模块。

| Runner | 比较内容 |
| --- | --- |
| `tools/codec-reference-test.mbtx` | Supabase ETL codec、schema/默认表达式向量 |
| `tools/config-reference-test.mbtx` | YAML 解析器和配置语义 |
| `tools/protocol-reference-test.mbtx` | pglogrepl v1 向量 |
| `tools/streaming-reference-test.mbtx` | pglogrepl v2 streaming 向量 |
| `tools/memory-reference-test.mbtx` | 内存策略计算 |
| `tools/memory-sampler-reference-test.mbtx` | Linux 内存/cgroup 观察 |

每个 runner 以及 `integration/reference/` 固定版本、归档哈希和夹具输入。
参照测试可能需要 Rust/Go 和上游源码缓存，与离线 native 单测分开。
执行前先读 runner，不要仅为让比较通过而重录向量。

目标 `Accepted` 不表示持久确认。必须先验证 Commit 和连续持久屏障，才能推进 checkpoint
或 feedback。快照 TableEnd 不是事务提交。槽失效和不安全时间线恢复必须明确失败。
契约见[API](API.zh-CN.md)，对应源码测试负责验证。

2026-10-02 在 `d0172f86c8398cbad3cc239a9504db1e5b6a64b1` 的审阅属于历史来源，
不是当前发布树的执行结果。见[验证范围](VALIDATION.zh-CN.md)和[第三方说明](../THIRD_PARTY.zh-CN.md)。
