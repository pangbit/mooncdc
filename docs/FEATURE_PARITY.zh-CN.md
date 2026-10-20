<a id="supported-feature-map"></a>

# 支持能力映射

<!-- language-switch -->
[English](FEATURE_PARITY.md) | [简体中文](FEATURE_PARITY.zh-CN.md)

英文版本为权威版本。


本表描述 0.1.0 的实现范围，不表示完成百分比，也不表示所有平台与故障组合都已通过。
见[交付范围](RELEASE_SCOPE.zh-CN.md)、[API](API.zh-CN.md)和[版本绑定验证](VALIDATION.zh-CN.md)。

| 能力 | 库与 CLI 的关系 | 限制 |
| --- | --- | --- |
| PostgreSQL 快照与 pgoutput v1 CDC | 共用流水线核心 | PG17/18 夹具；至少一次投递 |
| 持久 checkpoint 与恢复 | 共用状态/运行契约 | 连续持久前缀；拒绝过期归属或身份不匹配 |
| ClickHouse RowBinary 与 DDL | 相同目标实现 | ReplacingMergeTree 与 MergeTree；类型/DDL 为已记录子集 |
| 并行表 COPY 与 WAL 交接 | 共用表状态机 | 选择默认值和配置可不同；不允许静默遗漏 |
| 时间线/槽安全 | 共用 ancestry 与重建保障 | 不提供自动 HA 选主或跨系统恰好一次 |
| 健康、活动、指标和压力控制 | 公共 API 与 CLI 适配 | 活动或 ready 不授权持久确认 |
| JSON/YAML 与环境配置 | CLI 适配层 | 见配置映射与严格拒绝测试 |
| 独立 pgoutput v2 | 库订阅兼容入口 | 不扩大 v1 流水线承诺 |

BigQuery、Iceberg、Snowflake 实现及云端验收不进入当前发布树。
可选报告和 ConfigCat 只覆盖已记录的本地契约，不承诺完整服务/SDK 兼容。
历史阶段报告与旧测试总数不构成最终发布提交的验收结果。
