<a id="runtime-metrics"></a>

# 运行指标

<!-- language-switch -->
[English](METRICS.md) | [简体中文](METRICS.zh-CN.md)

英文版本为权威版本。


`ApplyControl::prometheus` 与 `with_metrics_server` 提供按 control 隔离的
Prometheus 0.0.4 文本。CLI 默认独立监听 9000，配置和关闭方式见
[replicator](REPLICATOR.zh-CN.md)。指标读取不改变流水线状态，也不写 checkpoint。

全局标签为可选 `pipeline_id`、`destination`、`project`，每个值最多 1024 UTF-8
字节。CLI 设置前两个标签，并可由 `supabase.project_ref` 设置project。内置维度只有固定的
worker、路径、状态和内存来源，不以表、槽、错误或连接字符串建立时序。
计数器以 control 为生命周期；恢复、重放及重建会继续累加，不能作为业务去重计数。
仅观察到事件后才出现事件计数器和直方图，未启用的功能不输出伪造的零样本。

| 指标 | 类型与含义 |
|---|---|
| `mooncdc_live`、`mooncdc_ready`、`mooncdc_lifecycle{state}` | 健康和 initializing/running/stopping 的实时 gauge，与健康探针使用相同 stall 判定 |
| `mooncdc_subscriptions_active`、`mooncdc_outstanding_transactions`、`mooncdc_queued_transactions` | 按 worker 的当前注册 subscription 数、未连续确认的事务数、待消费队列数 |
| `mooncdc_observed_wal_lag_bytes` | 活跃 subscription 已观察到的 server-end 减 durable 的最大字节差；不是定时 SQL 查询得到的当前源库 lag |
| `mooncdc_source_lsn_monitors_active{worker_type}` | 主 apply 和私有 table_sync 追赶作用域中的独立 SQL 监测数，包含尚未取得有效样本的监测 |
| `mooncdc_current_wal_received_lag_bytes`、`mooncdc_current_wal_durable_lag_bytes` | 最近成功采样的当前源 LSN 减收到的完整提交位置／目标屏障后的 durable 位置；各 worker 取最大差、负值钳为零，只有有效样本时输出 |
| `mooncdc_received_durable_gap_bytes`、`mooncdc_source_lsn_sample_age_seconds` | 对应作用域 received 减 durable 的最大差，以及最近有效样本的最大单调时钟年龄；不能把陈旧或尚无的样本当成已追平 |
| `mooncdc_source_monitor_samples_total`、`mooncdc_source_monitor_failures_total` | 独立 LSN 监测接受的样本数及连接／查询／形状／LSN 回退失败数；均按 worker 聚合，失败不授权确认或重试源流 |
| `mooncdc_tables{state}` | 活跃流水线持久状态中的 pending/copying/catchup/ready/errored 表数；流水线结束后移除 |
| `mooncdc_table_copy_current_wal_lag_bytes`、`mooncdc_table_copy_monitors_active` | 各复制槽成功采样的当前源 LSN 减初始一致位置的最大差与有样本的活动监测数；监测结束后移除 |
| `mooncdc_slot_invalidations_total{worker_type="table_sync"}`、`mooncdc_copy_monitor_failures_total` | 独立采样发现复制槽消失/失效的次数，以及一般连接/查询采样失败次数 |
| `mooncdc_replication_messages_total`、`mooncdc_replication_payload_bytes_total` | 接收的 CopyData 帧数与 payload 字节数，包含 keepalive；不含外层帧头、TCP/TLS 开销 |
| `mooncdc_replication_stream_ends_total{message_type,worker_type}` | 解析成功的command_complete或copy_done流结束数；不计确认、业务事务或成功恢复 |
| `mooncdc_transactions_received_total`、`mooncdc_changes_received_total` | Commit 解码次数与 Change 数，包含重放；事件路径在每个 Change 解码时计数，可先于 Commit，整事务路径在 Commit 计数 |
| `mooncdc_transactions_written_total`、`mooncdc_changes_written_total` | 整事务路径成功的事务调用、事件路径成功的 Commit 批次，以及成功传入的 Change 数；Accepted 也计入。主 apply 投影前计数，旧私有整事务路径投影后计数，新私有事件路径投影前计数 |
| `mooncdc_wal_batches_written_total` | 成功返回的事件批次调用，包括元数据、空 Commit 投影与部分事务；不能当作提交或确认数 |
| `mooncdc_transactions_durable_total` | 主 subscription 成功存储 checkpoint 后新增的连续事务数；私有 WAL 在目标屏障及 handover 判断成功返回后计数，未达到主流位置时仍未持久交接，不能与 written 或 Ready 等同 |
| `mooncdc_checkpoint_updates_total` | subscription 实际成功的 checkpoint 更新次数；不是 source feedback 帧数 |
| `mooncdc_copy_rows_received_total`、`mooncdc_copy_rows_written_total` | 快照回调接收的行数，以及 destination 成功返回的行数；written 可以尚未 durable |
| `mooncdc_copy_rows_completed_total`、`mooncdc_table_copies_completed_total` | TableEnd 的目标 flush 成功后累加本次快照行数和表数；后续 WAL catchup/handover 可能仍未完成 |
| `mooncdc_copy_planned_partitions`、`mooncdc_copy_effective_partitions` | 每次复制规划的目标分区数及实际 CTID 游标任务数的直方图；按 table_sync 聚合，不带表标签。串行兼容路径每个逻辑表为 1，平行路径物理空表为 0；跳过读取不记录 |
| `mooncdc_copy_partition_estimated_blocks` | 平行路径各实际任务按规划时物理块估计得到的跨度；不是实际扫描块数，开放末尾仍仅使用估算值 |
| `mooncdc_copy_partitions_completed_total`、`mooncdc_copy_partition_seconds`、`mooncdc_copy_partition_rows` | 游标读取、行回调与 CLOSE 成功后的任务数、耗时及行数。回调可以仅返回 Accepted，后续 TableEnd flush 失败仍保留这些完成样本；失败或取消未到 CLOSE 不计完成 |
| `mooncdc_schema_cleanup_attempts_total`、`mooncdc_schema_cleanup_errors_total` | 主 apply 实际检查非空待清理集合的次数及捕获的非取消失败数；失败保留候选和既有 checkpoint |
| `mooncdc_schema_cleanups_total`、`mooncdc_schema_cleanup_tables_total`、`mooncdc_schema_cleanup_pruned_versions_total` | 成功 prune 调用数、传入保留边界表数，以及实际删掉的历史版本数；一次 prune 可删零个版本 |
| `mooncdc_schema_cleanup_skipped_tables_total` | 本轮已评估、有权处理但尚无目标保留边界的表数；未持有其同步归属的候选仍保留，且不计入 |
| `mooncdc_worker_errors_total`、`mooncdc_worker_retries_total{trigger}` | 被 apply/table worker 捕获的失败及实际安排的 timed/manual 重试；不是所有 SQL 错误数 |
| `mooncdc_slot_recreation_attempts_total` | 实际进入显式槽重建的次数，失败也计入，不能作为完成次数 |
| `mooncdc_timeline_recovery_attempts_total` | 主worker发现timeline变化、旧worker已关闭并flush后安排的晋升重入次数；后续证明可能失败，不能作为完成数 |
| `mooncdc_memory_used_bytes`、`mooncdc_memory_total_bytes` | 共享采样器当前容量，来源为 system/process_cgroup/root_cgroup；不是进程 heap 或 RSS |
| `mooncdc_memory_backpressure_active`、`mooncdc_memory_backpressure_transitions_total{direction}` | 当前压力暂停与采样器发布的 activate/resume 转换数 |
| `mooncdc_batch_target_bytes`、`mooncdc_batch_registered_slots` | 当前 governor 启发式字节目标与活动容量位置数，不是硬内存上限 |
| `mooncdc_destination_write_seconds{worker_type,replication_path,status}` | 成功返回的 COPY/CDC destination write 耗时；不包含随后独立 flush |
| `mooncdc_private_wal_write_and_flush_seconds` | 兼容整事务私有 WAL 路径成功 write 与必要 flush 的合计耗时；新事件路径使用 destination write 直方图，不包含随后独立 flush |
| `mooncdc_table_copy_seconds` | 成功 TableBegin 到 TableEnd 屏障的耗时，不含私有 WAL catchup |
| `mooncdc_postgres_query_seconds{pool}` | 进入简单查询或自适应FETCH后，在成功、错误或取消结束时记录一次的秒直方图；pool仅out_of_band/postgres_store。多语句SQL批次计一次，FETCH包含消费/回调/压力等待，不按行或flush重复计数 |

当前真实CLI→ClickHouse八组已抓取两个query pool及COPY游标完成指标：四个默认
reader处理16个分区，TERM/RECREATE分别完成1001/1003行。五个非INIT阶段都要求
两个pool的count为正；INIT可能尚未建立连接。见
当前CLI验收[验收版本绑定](VALIDATION.zh-CN.md)，不扩大为所有查询路径或云服务验收。

秒直方图使用固定边界 0.005、0.01、0.025、0.05、0.1、0.25、0.5、1、2.5、5、10。
分区数边界为 0、1、2、4、8、16、32、64、128、256、512、1024；分区行数边界为
0、1、10、100、1000、10000、100000、1000000；估算块数为
0、1、8、64、512、4096、32768、262144、2097152、16777216、134217728、1073741824、4294967295。
各 family 再提供 `+Inf`，输出累计 bucket、sum、count。计数器及 bucket 饱和到 UInt64 最大值。

当前自动挂接的 wire/checkpoint/live-state 指标范围是 `run_pipeline`。独立
`Subscription::apply_to(control=...)` 提供 destination write 指标，但公共 `subscribe`
没有 control 参数，不能用它声称已有 wire/checkpoint 指标。私有表事件 WAL 已注册
subscription diagnostics；兼容整事务私有路径仅提供当前源 LSN 和 received/durable 观察，
未注册队列/observed-lag gauge。独立源采样使用 `table_sync_monitor_refresh_interval_ms`，
在源读取暂停和目标 write/flush 阻塞时继续；primary 查询当前 WAL，standby 查询 replay LSN。
每个监测至多一个有界查询连接，按完成后的固定延迟重采样；关闭作用域取消并等待查询。
回退 LSN、NULL 和失败保留旧样本及年龄，不写 checkpoint、不发送 feedback、不刷新业务活动。
流水线退出会解除持久 store、monitor、batch 和 subscription 引用，累计事件仍可读取。

查询观察自动接入流水线的启动/重入、动态表发现、主流重连、快照exporter/元数据/
串行与平行reader，以及COPY槽/当前源LSN独立采样。CLI也向状态/schema库和源迁移传递
同一个control。库的`PostgresStateStore::new(..., control=...)`与
`run_source_migrations(config, control=...)`显式启用相应观察；未传入不输出。
公共独立subscribe/copy_snapshot没有control，不能据其结果声称已记录这些SQL。
query计时不含连接建立/认证或状态mutex等待，不计START_REPLICATION/流帧及隐式连接关闭
回滚。固定ETL的SQLx pool计时包含pool等待，这里保留两种固定pool名称表示请求类别，
并不表示实现了连接池。观察错误不授权重试/重连、反馈或checkpoint，详见
查询指标验收[验收版本绑定](VALIDATION.zh-CN.md)。

固定参照 Supabase ETL `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4` 的独立端口、
`/health` 和其他路径文本行为已映射。上述 family 映射了事务/事件/字节/消息、写入、
复制、持久确认、worker、内存和批次的已有能力，保留 MoonCDC 自己的名称与明确语义。
复制期槽和主/私有 WAL 当前源 LSN 定时 SQL 采样已接入，验证见
独立源 LSN 报告[验收版本绑定](VALIDATION.zh-CN.md)。分区和 schema 清理观察已接入，
见分区指标验收[验收版本绑定](VALIDATION.zh-CN.md)；SQL query 计时已接入上述范围。
可选Sentry/Supabase报告的本地实现及剩余差距见报告[验收版本绑定](VALIDATION.zh-CN.md)，
ConfigCat作用域轮询见验收[验收版本绑定](VALIDATION.zh-CN.md)；轮询失败只打印固定诊断，
未挂接Prometheus family。
Rust/Tokio/jemalloc 专属指标没有 MoonBit 等价实现。本阶段不能作为
完整 C8 配置与观测验收，也未部署 Prometheus 服务或验证真实受限环境的压力指标。
