# 固定流水线配置映射

参照 Supabase ETL `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4` 的
`crates/etl-config/src/shared/pipeline.rs` 与 `replicator.rs`。本入口保持自己的
JSON/YAML 结构，不直接兼容上游文件语法。表中默认是 CLI 默认；库兼容差异单独列出。

当前产品范围为 [PostgreSQL → ClickHouse 库与 CLI](RELEASE_SCOPE.md)。本表只映射该范围；
PostgreSQL 状态/schema 存储用于恢复元数据，不是 PostgreSQL 目标连接器。

| 固定 ETL 字段 | MoonCDC 字段／实际行为 |
|---|---|
| `id`、`publication_name` | `id`、`publication`；id 为非空字符串，生成独占 state/schema namespaces |
| `pg_connection` | `source`；host/hostaddr/port/database/user/password/security/keepalive，额外支持 timeout；验证 TLS、回环明文和整数范围 |
| `store_pg_connection` | `state.connection`，省略复用 source；可指定 state.schema/table |
| `replication_slot.failover` | 同名嵌套字段，默认 false，PG17/18 支持，启动验证并保留重建设置 |
| `batch` | 默认启用 ratio=0.2、max_bytes=33554432、max_fill_ms=10000；ClickHouse 主/私有 WAL 使用有界事件批次，null 关闭退回完整事务；确认仍在 Commit 和目标屏障后 |
| `table_error_retry_delay_ms` | `table_retry_delay_ms`，默认10000，CLI限制1000–86400000 |
| `table_error_retry_max_attempts` | `max_table_retries`，默认5，正 Int |
| `max_table_sync_workers` | `copy_concurrency`，默认4，1–256 |
| `max_copy_connections_per_table` | `copy_connections_per_table`，默认4，1–256；两者乘积≤256，先除后比避免溢出 |
| `memory_refresh_interval_ms` | 同名，默认100，正 Int；压力与批次共用采样器 |
| `memory_backpressure` | 同名，默认activate=0.85/resume=0.75，null关闭；暂停源读取但排空/确认/心跳继续 |
| `table_sync_copy` | 同名四种 OID 选择策略；空/跳过复制仍做目标与持久屏障 |
| `table_sync_monitor_refresh_interval_ms` | 同名，默认10000，正 Int；复制期检查槽、主/私有 WAL 独立采样当前源/standby replay LSN，采样失败只计数 |
| `invalidated_slot_behavior` | 同名error/recreate，默认error；持久重建全部表，不跳过丢失WAL |
| `run_source_migrations` | 同名，默认true；健康/metrics绑定错误在迁移前拒绝 |
| `destination` | 当前仅支持 ClickHouse url/user/password/database/engine，两种引擎；核心 CLI 拒绝其他目标，见[CLI](REPLICATOR.md)。CLI语法仍为自己的配置对象 |
| `health` | 独立探针，可选；metrics另在默认9000端口 |
| `sentry.dsn`、`supabase.project_ref/api_url/api_key` | 可选CLI报告已接入；终止表状态先持久化、TimedRetry抑制、最终失败报告、失败不覆盖原错误；有界本地peer和JSON/YAML CLI验证，服务/SDK剩余差距见报告[验收版本绑定](VALIDATION.md) |
| `supabase.configcat_sdk_key` | 可选初始化与作用域轮询已接入：默认Global/60s/30s、默认project user、V6偏好/ETag、失败保留缓存、退出取消并join；JSON/YAML受控CLI历史结果见[验收版本绑定](VALIDATION.md)。固定入口没有求值调用；完整SDK/真实CDN仍待补 |

库 `run_pipeline` 继续保留 copy_concurrency=2、copy_connections_per_table=1、batch
显式启用、source_migrations=false，以及正数 retry delay 的兼容契约。CLI 默认4/4和
batch启用的改变会增加连接数；每表除reader还有exporter、metadata和短期独立监测连接，
256乘积仅指reader预算。环境字段覆盖遵循同一校验，不回显秘密。

MoonBit Int 字段限于2147483647，不能声称接受上游全部u64配置。ConfigCat、Sentry完整SDK
能力/真实服务验收仍有差距；这些可选服务的完整对齐与其他目标端不作为当前版本交付门槛。
已保留的可选功能仍需正确关闭、脱敏及退出。库协议/确认语义、CLI语法、默认值和类型宽度
差异均在本清单保留，当前阶段只覆盖上述可运行流水线设置。
