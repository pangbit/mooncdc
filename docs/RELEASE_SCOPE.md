# 当前交付范围

2026-10-15 确认：MoonCDC 专注 PostgreSQL → ClickHouse，交付可嵌入的 MoonBit 库和
可独立运行的 replicator CLI。两种入口同等支持，共用核心；首批源端版本为 PG17/18。
本文件定义当前版本的边界；[功能映射](FEATURE_PARITY.md)、
[发布检查](COMPLETION_CHECKLIST.md) 与[验收版本绑定](VALIDATION.md) 分别说明实现、
待办与证据范围，不能把范围收敛本身当作新源码的验收结果。

## 产品与参考边界

- 核心模块：`pangbit/mooncdc`，native 后端，PostgreSQL → ClickHouse 单向同步。
- 独立工具：`cmd/replicator`；JSON/YAML 与环境变量配置、离线 `--check`、信号排空、
  健康检查和指标。工具调用库的 `run_pipeline`，不另写复制或恢复引擎。
- 固定主参照：[Supabase ETL](https://github.com/supabase/etl/tree/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4)，
  提交 `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`；只对齐所选源端、ClickHouse、
  状态/恢复与运行语义。pglogrepl、Debezium 继续作为协议与异常场景补充。
- BigQuery、Snowflake、DuckLake 和 Iceberg 退出当前交付、默认依赖、构建、配置与验收。
  已有实现先保留在未发布扩展或 Git 历史；DuckLake 仍是路线图，不能称为已实现。
  扩展可独立维护，但失败或真云验证缺失不阻塞本版本；隔离进度见
  本批审查[验收版本绑定](VALIDATION.md)。
- 不做 PostgreSQL → PostgreSQL、反向/双向同步或管理平台。
  PostgreSQL StateStore/SchemaStore 是恢复元数据后端，不是数据目标。

## 库与 CLI 的公共边界

| 层 | 当前职责 | 边界与依据 |
| --- | --- | --- |
| 公共库 | `ConnectionConfig`、`ClickHouseDestination`、`run_pipeline`、持久 StateStore/SchemaStore、`ApplyControl` | 宿主组装连接、目标、状态与控制作用域；错误、停止和资源释放可观察。见 [API](API.md) |
| 共用核心 | pgoutput v1、逐表快照、主/私有 WAL 交接、schema 版本、目标持久屏障、checkpoint、重连及晋升证明 | 库与 CLI 不可各自实现另一套确认或恢复算法。公共入口在 `state_runtime.mbt`，实现主要在 `pipeline.mbt`、`pipeline_copy.mbt`、`subscription.mbt` 和 `clickhouse_*.mbt` |
| CLI 适配 | 配置解析、默认值、构造库对象、进程信号、探针/指标及退出码 | `cmd/replicator/main.mbt` 的 `ReplicatorConfig::pipeline` 调用公共 `run_pipeline`；配置字段见 [映射](PIPELINE_CONFIG_MAPPING.md) |
| 低层库兼容 | `subscribe`、`copy_snapshot`、`Destination`、原始事件与稳定 ID | 保留可组合 API；泛型接口的存在不扩大本版本内置连接器范围。独立 v2 订阅保持既有边界，不新增 v2 流水线承诺 |

同等支持指两种入口都要验证相同业务能力和故障语义，不要求配置语法、生命周期封装或
默认值完全相同。CLI 的 COPY 默认并发 4/4、batch 与压力控制默认开启、源迁移默认 true；
库兼容默认值为 2/1、batch/压力显式启用、源迁移 false。应显式配置比较用例，记录差异；
不在本批改变这些兼容默认值。

宿主负责保管配置、保持 namespace/源目标身份稳定、调用停止并等待排空。
CLI 承担对应的进程适配职责。源迁移所需权限与 schema/状态配置沿用现有 API 契约；
未经支持的 schema 或不安全恢复必须报错，不能静默跳过。

## 首批验收顺序

下列判据同时适用于库与 CLI。业务矩阵至少包含 PG17/18 与 ClickHouse 两种现有引擎；
CLI 另覆盖 JSON/YAML、环境覆盖及离线拒绝。故障组合分批执行，必须逐项列明入口、
版本、引擎、故障点和实际执行结果，不能用未启用测试体或普通测试总数替代。

| 顺序 | 必须证明的行为 | 可复用入口或证据 |
| --- | --- | --- |
| 1 | 非空初始快照期间并发 I/U/D，快照与 WAL 交接后无遗漏；完成表不被无故重置，复制失败可恢复 | `tools/parallel-copy-test.mbtx`、`tools/replicator-test.mbtx`；并行 COPY[验收版本绑定](VALIDATION.md) |
| 2 | 更新、删除、NULL/TOAST、主键/replica identity 与类型映射符合契约；当前值视图与操作历史分别核验 | `tools/clickhouse-test.mbtx`；CLI/ClickHouse 组合[验收版本绑定](VALIDATION.md)、类型与快照/WAL[验收版本绑定](VALIDATION.md) |
| 3 | 目标写入到 checkpoint/反馈之间中断允许重放；稳定事件身份和两种引擎的重复边界可解释，无静默遗漏 | `tools/checkpoint-crash-test.mbtx`、`tools/clickhouse-crash-test.mbtx`；[API 的持久契约](API.md#destinations-and-durability) |
| 4 | 新进程恢复保留持久进度，拒绝损坏/身份不符/不可恢复位置；显式失效槽重建有完整持久意图 | `tools/pipeline-crash-test.mbtx`、`tools/slot-recreate-process-test.mbtx`；重建进程[验收版本绑定](VALIDATION.md) |
| 5 | 主库晋升前隔离旧写入者，验证 ancestry、fork、写入上界及状态 owner；含 COPY 期间切换与 owner 丢失 | `tools/timeline-promotion-test.mbtx`、`tools/cli-active-promotion-test.mbtx`；活跃 COPY 晋升[验收版本绑定](VALIDATION.md) |
| 6 | ClickHouse 超时、503、未知写入结果、取消和重新启动不提前确认；正常停止排空，目标故障保留恢复位置 | `tools/clickhouse-test.mbtx`、`tools/clickhouse-crash-test.mbtx`；取消[验收版本绑定](VALIDATION.md) |
| 7 | 已支持列 DDL 与全量/增量、重启及 publication 投影正确衔接；持久保存 schema，拒绝未知或不支持变化 | [API 的 schema 与 ClickHouse 支持范围](API.md#ddl-schema-versions)、状态 schema[验收版本绑定](VALIDATION.md)；固定源迁移 fixture 在 `integration/reference/etl/` |

采用至少一次投递。`Accepted` 不授权确认；只有有效 Commit 与覆盖连续写入的目标持久
屏障允许推进 checkpoint/源反馈。v1 批次可在 Commit 帧前向目标写入已提交事务的片段，
不能据此宣称目标具有源事务原子可见性。ReplacingMergeTree 当前值与 MergeTree 操作历史
分别验证，不承诺跨系统恰好一次。晋升恢复不等于自动选主或自动 HA 路由。

## 交付门槛与版本绑定

当前支持范围是 PostgreSQL → ClickHouse 库与 CLI。BigQuery、Iceberg、Snowflake
及云端验收不进入本发布树。JSON/YAML、持久状态、快照/WAL 交接、目标持久屏障、
重连和受支持的晋升/重建行为沿用 API 契约；不扩大到自动选主或恰好一次。

验收必须区分产品、编译器、测试工具、环境和未执行步骤。历史 Linux 结果及
256 编译预算、264 实测峰值、512 单次诊断上限见 [版本绑定](VALIDATION.md)。
这些结果不代表最终发布分支的远端 CI 已运行，也不授权修改宿主限制。
当前验收命令和独立维护方法见 [测试](TESTING.md)，待办见 [发布检查](COMPLETION_CHECKLIST.md)。
