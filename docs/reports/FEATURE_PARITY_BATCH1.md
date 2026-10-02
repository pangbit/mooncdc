# 功能对齐首批交付

日期：2026-10-02。实现验证基线：`a883181`。功能对齐尚未完成。
主参照与差距清单见 [FEATURE_PARITY](../FEATURE_PARITY.md)。

## 本地提交

| 提交 | 内容 |
|---|---|
| `5089e3a` | 恢复拒绝及真实复制槽推进、失效、低流量 WAL 安全测试 |
| `50d9ff8` | 分阶段功能对齐计划 |
| `ea502b3` | 可选 binary 传输及重连协商 |
| `b09f3f2` | 一致性快照、投影/过滤、分区和增量衔接 |
| `558f4b3` | 事务/非事务逻辑消息，回滚与错误语义 |
| `7b1f4cf` | 自定义目标端的接收/持久化屏障、停止与快照 checkpoint |
| `a883181` | INCLUDE 列不属于索引身份键的回归修复 |

## 验证

本机 macOS native：moon 0.1.20260920，moonc v0.10.14+7d59c7ec9。
隔离 Docker：PostgreSQL 17.11、18.6。

- `moon check --target native --deny-warn` 通过。
- `moon test --target native`：43/43。未设置真实库变量时，真实库/故障 worker 条件测试体不执行。
- 带 `MOONCDC_TEST_PORT` 和测试 CA 的 `moon test --target native --filter 'live*' -v`：
  PG17 / PG18 各 11/11，包含原有 TLS、恢复、checkpoint、心跳、资源限制和新增功能。
- INCLUDE 修复在 PG18 的完整套件之后发现；专门用新增回归先证实失败，修复后 PG17/18
  均通过该回归。PG17 的完整套件运行于修复之后；未把 PG18 前一次全套结果冒充修复后全套。
- `moon info --target native`、`moon fmt --check`、release 构建及 `moon doc` 通过。
- `moon run tools/package-test.mbtx`：源码包独立解压、消费者 check/run 通过，
  检查新源文件进入包，公共 binary、目标端状态、停止控制及快照 API 可用。

详细场景见 [快照报告](SNAPSHOT_VALIDATION.md)、[目标端报告](DESTINATION_VALIDATION.md)。
本地完整 live 日志保存在忽略目录 `.test-artifacts/validation/feature-parity-pg17.log`
与 `feature-parity-pg18.log`。测试使用的 `mooncdc-test` 容器与卷在验证后移除。

## 未完成项

逐表持久同步状态、并行复制与失败自动重建；schema 版本和 DDL 消息解释；原生类型转换；
ClickHouse、BigQuery、DuckLake、Snowflake、Iceberg 目标端；独立 replicator；
上游进程差分和长期资源验证。协议 v2 仍作为补充项保留。
这些差距没有因为基础 API 实现而被认定完成。

本轮只有本地提交和本地验证，没有推送、发布到 Mooncakes 或部署。
