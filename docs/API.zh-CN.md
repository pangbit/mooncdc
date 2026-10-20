<a id="api-and-support-contract"></a>

# API 与支持契约

<!-- language-switch -->
[English](API.md) | [简体中文](API.zh-CN.md)

英文版本为权威版本。


编译器生成的[接口](../pkg.generated.mbti)为权威。运行 `moon doc` 可生成浏览用 API 文档。
公共类型属于 root 包 `pangbit/mooncdc`；示例是独立可执行包。

0.1.0 模块通过同一流水线核心支持 PostgreSQL → ClickHouse 库与 replicator CLI。
见[交付范围](RELEASE_SCOPE.zh-CN.md)。其他目标实现不在本模块范围内。

<a id="connection-settings"></a>

## 连接设置

建议使用 `ConnectionConfig::new(host~, user~, password?, database?, port?, security?,
timeout_ms?, hostaddr?, keepalive?)`，构造时检查配置但不打开 socket。
默认端口 5432、数据库 postgres、空密码、验证 TLS，以及 5000ms 连接超时。
Debug 输出始终排除密码。

`hostaddr` 是可选数字 IPv4/IPv6，不含端口或 IPv6 scope，只覆盖 TCP 目的地址；
`host` 仍用于 TLS 证书名。明文保留数字回环 host 限制，非回环 hostaddr 也拒绝。
只有省略 hostaddr 时才做 DNS。

`TcpKeepaliveConfig::new(idle_secs?=30, interval_secs?=10, retries?=3)` 为每条 PostgreSQL
连接启用 TCP keepalive。数值均须为正，系统可能拒绝不支持的值。
它不替代复制反馈、业务进度观察或查询截止时间，也不承诺精确断连时间。

源码兼容：`ConnectionConfig` 增加了 `hostaddr` 和 `keepalive`，旧 record literal 需加
`hostaddr: None, keepalive: TcpKeepaliveConfig::new()` 或改用构造器。
`..config` record update 自动保留两个字段。

PostgreSQL 启动参数按负载设置，与连接和客户端操作时限独立。
元数据/状态/迁移会话的 statement、lock、idle-in-transaction 超时为 30s/10s/60s；
WAL 和快照 reader 为 0/30s/0，使长流和 copy 回调保留快照。
快照 exporter 为 0/0/0，因为建槽可能等待 writer，且后续查询不能使已导出快照失效；
独立 reader 仍有 30s lock 限制。流水线建槽期间暂时关闭 lock_timeout，复用前恢复 30s。
客户端时限和取消仍生效。`pg_stat_activity` 的 application name 区分 query、state、
migration、WAL、snapshot reader/exporter，尚不包含逐流水线或逐表后缀。

<a id="lifecycle"></a>

## 生命周期

`ApplyControl::prometheus(pipeline_id?, destination?, project?, stall_timeout_ms?=300000)`
渲染作用域内 Prometheus 文本，不改 worker 状态。计数器随 control 存活并包含重试/重放；
新的指标生命周期应使用新的 control。可选标签经过转义，每个最多 1024 UTF-8 字节。
内置标签不含 relation 名、连接串或错误消息。
`with_metrics_server(control, run, port?=9000, listen_address?, pipeline_id?, destination?, project?,
stall_timeout_ms?=300000)` 在调用 `run` 前验证并绑定独立 IPv4/IPv6 listener。
`/health` 返回 `OK`，其他路径输出相同指标，接受所有方法，HEAD 无正文。
正常返回、失败和取消均关闭 listener 及已接纳连接。请求超时 5s，最多同时 128 客户端。
指标不会确认数据或停止 worker。见[指标定义与限制](METRICS.zh-CN.md)。
省略 `listen_address` 保留原来的全接口 IPv4/IPv6 行为。
显式数字 IPv4 或无方括号 IPv6 仅绑定该地址，不做 DNS 或 IPv4 回退。
`validate_metrics_listen_address(address)` 不创建 listener，只检查语法；端口单独配置。

`run_pipeline(..., memory_backpressure=MemoryBackpressureConfig::new())` 启用每 100ms
共享主机/cgroup 压力采样，默认 0.85 激活、0.75 恢复，恢复要求使用量严格低于阈值。
库为显式启用；在打开流水线连接前验证配置。

压力门控新的主 WAL 读取、快照 FETCH 和私有表 WAL 读取。已开始读取可以结束；
目标写入、定时 flush、checkpoint 确认和周期 feedback 继续。
压力会早于普通计时器立即 flush 待 apply 写入；快照 Accepted 行在等待前 flush。
停止取消阻塞的源等待，消费者退出后流水线等待 sampler。
暂停的源活动不算 stalled，但并发阻塞的目标写入仍可 stalled。

这是紧急暂停源读取，不是硬内存限制；事务和帧上限相互独立。
replicator CLI 默认开启压力，除非 `memory_backpressure` 显式 null，见[配置](REPLICATOR.zh-CN.md)。

`run_pipeline(..., batch=BatchConfig::new())` 独立启用建议解码批次目标：
`memory_budget_ratio=0.2`、`max_bytes=33554432`、`max_fill_ms=10000`。
比例须为 (0,1]，字节数为正，填充时间非负；零表示每项 flush。
填充截止时间从第一项开始，不因后续数据重置。native UTF-16 行/元数据大小提示是估算，
不是 heap 测量。容量和活动归属决定每个目标，并以 `max_bytes` 封顶；不可拆项仍可能超出。

启用 `batch` 且目标 `supports_wal_batches` 返回 true 时，主/私有表 WAL 逐事件解码和释放。
ClickHouse 已启用，自定义目标默认 false。`write_wal_batch` 收到有序 `WalBatch`，
只有验证过 Commit 后 `end_lsn` 才为 Some。跨批次拆分、表投影和重放时，`event_id`
保留原始行序号或逻辑消息 LSN，`replicated_schemas` 保留逐事件精确 schema。
默认 writer 拒绝事件批次，不向 `write_transaction` 传递部分对象。

receiver 一个交付 slot、apply worker 一个积累批次，共注册两个 governor 位置。
copy worker 在快照时占一个位置，事件 catch-up 时另加 receiver 位置；额外快照 reader
也注册归属。字节目标、首事件时限、压力和停止触发部分批次排空。
每个 Accepted 批次在积累下一个前 flush；Commit 也排空，空表投影亦然。
checkpoint 和表交接只能在 Commit 与累计目标屏障后推进。
取消、畸形 Commit 或提前断连可留下可见片段，但保留旧 checkpoint 以便按稳定 ID 重放。
此契约不提供整个源事务的原子可见性。

公共 `subscribe`/`next` 仍交付完整 `Transaction`。未启用事件模式时，流水线保留兼容的
整事务 collector，注册 16 个 pending 位置和一个 decoder，可能超目标一个整事务。
两种 decoder 都受累计事务字节/事件及元数据限制，释放事件不会重置这些限制。
单个事件、线帧、元数据缓存或目标编码可能超出建议目标。失败和取消均释放注册。
此 v1 事件路径与后述可选 pgoutput v2 collector 独立，流水线继续使用 v1。

两个选项共享 sampler。仅开 `batch` 时仍刷新容量，不暂停压力。
`memory_refresh_interval_ms` 覆盖 `MemoryBackpressureConfig` 周期，否则使用其周期或 100ms；
显式周期须为正。两项均省略保持库原行为。CLI 省略 `batch` 时使用默认，显式 null 关闭；
压力关闭时仍共享 `memory_refresh_interval_ms`。库调用仍显式启用，见[配置](REPLICATOR.zh-CN.md)。

`run_pipeline(..., table_sync_monitor_refresh_interval_ms?=10000)` 在每个 copy 持有导出槽时
启动独立采样，含 catch-up，要求正 Int 毫秒。使用自有连接，primary 查当前 WAL，standby
查 replay WAL。私有槽缺失/失效会取消该 copy 并进入现有表重试；不兼容元数据为致命错误。
其他采样错误计数并在周期后重试，不推进 checkpoint。归属完成、失败、取消都中断并等待
采样握手/查询。相同周期也为主 apply 和私有 catch-up 的当前源 WAL/standby replay LSN
提供独立有界 SQL 采样；源暂停和目标阻塞期间继续，不改 checkpoint、feedback 或 worker 活动。
lag 位置、样本年龄和聚合见[指标](METRICS.zh-CN.md)。

`subscribe(config, slot~, publication~, start~, checkpoint_path~, consume, ...)` 完成认证、
识别源、验证槽、进入 COPY BOTH，并启动 reader/heartbeat 任务。
回调收到 `Subscription`；返回、抛错或取消都停止任务，关闭本地 socket 和 checkpoint 锁。
服务端槽在 TCP 关闭后停用，槽本身保留。打开订阅会在连接超时内等待槽停用，不终止其他
槽 owner，也支持立即 close/resume。

`next()` 返回不可变 `Transaction`，应由一个任务调用。
多个任务可乱序完成业务后调用 `ack(tx)`，确认会序列化。订阅不能超出回调寿命。
`close()` 唤醒等待者并关闭连接，不确认也不删槽。

`ack(tx)` 记录完成但只持久化连续前缀。事务 2 未完成时确认 3，不能越过 1。
同会话重复确认无害，其他会话事务被拒绝。调用者须先原子持久化业务变化和去重 ID，
再 `ack`。示例内存状态无法抵御进程失败。

checkpoint 使用 SHA-256 完整性校验、版本化源绑定、独占 advisory lock、临时文件 fsync、
原子 rename 和父目录 fsync。父目录必须已存在，并使用具备这些 POSIX 语义的本地文件系统。
退出后 `.lock` 文件保留，其存在不表示锁仍被持有；旧 `.tmp` 被忽略，下次写入时替换。

<a id="external-state-stores"></a>

## 外部状态存储

`subscribe(..., checkpoint_store=store)` 用 `StateStore` 替代 `checkpoint_path`；
`run_pipeline(..., state_store=store)` 同样替代 `state_path` 及其 checkpoint。
二选一，文件路径调用保持兼容。namespace 属于一个流水线/流，重启须稳定，不得复用于
不同源或目标。源绑定、校验和及重放检查相同。
`SnapshotPosition.save_checkpoint_in(store)` 保存初始快照供后续 Resume。

`StateStore` 为 Pipeline、ReplicationCheckpoint、Schemas、TableMetadata 记录提供独占
acquire/release、原子 load 和 owner 校验的持久 save。实现须排除竞争进程、拒绝过期 owner；
save 成功即建立持久性，失败不推进内存流水线/checkpoint。
`state_store_status(store)` 不获取归属，只观察原子状态。
`with_state_store(store, session => ...)` 在返回、错误、取消时释放归属；保留的 session 拒绝再写。

`session.check()` 向外部目标提供已有本地寿命检查。外部副作用前及可能结束作用域的异步
等待后调用。它仅检查此 scope 是否仍活动，不查询底层存储、续约、隔离远程 writer 或授权确认。

`FileStateStore.new(existing_directory)` 使用 advisory `.lock` 和原子 fsync 记录。
`PostgresStateStore.new(config, store_id~, schema?="public", table?="mooncdc_state", control?)`
为每个 store ID 使用一条 PostgreSQL session advisory lock，并共享记录表。
acquire 时可建表，但 schema 必须存在；schema/table 名和 ID 都转义。
写入使用 synchronous_commit=on 和事务 upsert，并发建表序列化。
owner 连接失败后关闭，不静默重连，需要重新 acquire；backend 终止释放 PostgreSQL 锁。
状态表不能在源 publication 内；源为所有表时使用独立元数据库。
传入 `control` 后，显式状态查询（含失败/取消）记入 `postgres_store` 直方图，不改变归属或重连规则。

对 store-backed 流水线，验证源/目标后、启动前调用 `Destination.bind_state(session)`。
目标在对应操作持久后，可用 `session.get_metadata(oid)` / `put_metadata(oid, json)` 保存
创建/schema/generation 元数据。元数据有校验和、OID 绑定、精确 JSON 数字文本，
每条最多 16 MiB，JSON 容器最多 127 层。默认 bind hook 无操作。
自定义目标仍须隔离自己此前异步写入；存储归属本身不构成目标数据的分布式 fencing。

`with_state_schema_store(schema_state, identity~, schemas => ...)` 用 StateStore 持久保存完整
版本化 schema 历史，替代本地 schema 文件。向目标和 `run_pipeline(..., schema_store=schemas)`
传入同一 `schemas`。它与流水线 StateStore 必须使用独立 namespace（目录或 PostgreSQL
store ID），因为两个 scope 同时持独占归属。例如 PostgreSQL ID `pipeline-1/state` 和
`pipeline-1/schemas` 可共享表而独立拥有归属。重启保持 namespace 和 schema identity，
支持无需本地 schema 文件的恢复。

schema envelope 的校验和、16 MiB 限制、精确版本冲突检查和按 checkpoint 清理与文件实现一致。
仅记录保存成功后更新缓存；写入/prune 失败保留旧缓存。scope 返回后 SchemaStore 拒绝使用。
已有 `with_file_schema_store` 文件仍兼容，不自动迁移。对记录种类做穷尽匹配的自定义
StateStore 必须加入新的 `Schemas` 种类。

<a id="positions-and-replay"></a>

## 位置与重放

`At(nonzero_lsn)` 要求新的 checkpoint 路径。使用建槽返回的一致位置；显式选更晚位置
即有意省略更早变化。`Resume` 要求完整的已有 checkpoint，绑定 PostgreSQL system
identifier、timeline、database、slot、publication。restart 或 confirmed flush 位置
超过 checkpoint、槽缺失或失效均拒绝。消费者不能共享槽，也不要手工推进或重建槽。

瞬时连接失败时，内存保留完整待处理事务，并从最后保留的完整事务后重连；不完整事务重新
解码，已经完整保留的重放事务跳过。进程退出后以持久 checkpoint 为准，未确认事务可能重放。
致命协议、配置、上限及 checkpoint 错误绝不重试。SQLSTATE 57P01/57P02/57P03 的服务端
关闭可重试，其他服务端错误直接返回。一次重连尝试覆盖连接/认证、识别源和启动 COPY BOTH；
任一阶段瞬时错误或超时均消耗同一有界预算，失败连接关闭。预算耗尽返回最后一次错误。
源身份变化立即失败，即使仍有预算。

`Transaction.id` 组合源 system、database、slot 和 commit LSN。XID 回绕后不能单独唯一。
`event_id(index)` 只给数据变化追加序号；relation、type、origin 返回 `None`，因为重连公告
可能不同。`commit_time_us` 使用 PostgreSQL epoch（2000-01-01 UTC）。
JSON 表示包含业务事务，不含进程本地确认 token。

<a id="data"></a>

## 数据

`Change` 支持 RelationChanged、TypeChanged、Insert、Update、Delete、Truncate、Origin
及可选事务 Message。每个行变化携带不可变 Relation 快照，含列、key 标志、type OID 和
type modifier。schema 变化体现于后续 relation 元数据，但这不是完整 DDL 流。
自定义类型保留 OID/name 元数据及文本值。

`OldTuple.Key` 是 replica-identity tuple，非 key 位置可能为 NULL 占位。
`OldTuple.Full` 表示 FULL identity 的旧行，update 也可能无旧 tuple。
需要任意旧值时应设置 FULL，并处理无法获取的 TOAST。`UnchangedToast` 表示保留已知旧值，
若未知则获取或重建状态，绝不表示 SQL NULL。Truncate 可涉及多个 relation，并含
cascade/restart-identity 标志。

v1 默认文本传输，或显式 `subscribe(..., binary=true)`。二进制字段以
`Value::Binary(Bytes)` 原样暴露，不转 UTF-8，也不直接解码 native SQL 类型；按列 OID
解释线格式。没有二进制输出的类型可能回退文本，因此需同时处理 Text 和 Binary。
重连保留传输选择。Value 穷尽匹配的消费者需添加 Binary 分支。
仅支持已提交交付和 UTF-8 文本；不实现两阶段事务、自动 endpoint 选择或 HA 路由。
未知消息明确失败。

`subscribe(..., protocol=V2Streaming)` 协商 `proto_version=2, streaming=on`，默认 `V1`。
v2 接受 Stream Start/Stop/Commit/Abort，以及带 XID 前缀的 row、relation、type、truncate
和事务消息。交错未提交事务保存在私有缓冲，只在 Stream Commit 交付完整 Transaction。
root abort 丢弃缓冲；subtransaction abort 从该子事务首个到达 offset 截断，包括后续嵌套
或已 released 记录。relation/type 线元数据仍属于连接。

事务字节/事件上限还限制所有开放 stream 缓冲与普通事务的总量；`max_pending_transactions`
也限制开放 streamed 事务数。控制字节计入字节预算，subtransaction abort 不重置累计额度。
超限不交付临时结果、不推进 checkpoint；调整上限后重开以重放。这不承诺精确 RSS 上限。
重连保留 v2/binary/messages 协商、丢弃不完整缓冲，并从持久 checkpoint 重放。
已提交行/消息 ID 与 v1 相同。独立模式与流水线 WalBatch 交付分离；
`run_pipeline`、私有 COPY catch-up 和 CLI 继续使用 v1。

`subscribe(..., messages=true)` 请求逻辑消息。`pg_logical_emit_message` 的事务消息按
commit 顺序成为 `Change::Message(LogicalMessage)`，回滚消息不交付。
`prefix` 是文本，`content` 是原始 Bytes，`lsn` 是消息自身 WAL 位置。
消息 `event_id` 为 `tx.id + "/message/" + lsn`，不改变已有行事件序号。

非事务消息交给可选异步回调 `nontransactional_message`，即使产生它的事务后来回滚。
没有 handler 时收到消息会明确失败。返回前须持久完成必要写入；handler 失败停止订阅，
不重连。回调在 reader 上运行并形成背压，心跳独立。完成回调不单独推进 checkpoint 或
feedback；后来确认的事务可覆盖其位置，此前重启/重连可能重放。用源身份加消息 LSN 去重。
只有非事务消息的流会在原持久位置保留 WAL。此协议 API 不解释 Supabase ETL 的
`supabase_etl_ddl` JSON，也不安装 event trigger 或更新 schema store。

<a id="initial-snapshot"></a>

## 初始快照

`copy_snapshot(config, slot~, publication~, consume, ...)` 创建带导出快照的新持久
pgoutput 槽，将视图导入只读 repeatable read 事务，顺序复制 publication 表。
回调依次收到 `TableBegin(Relation)`、零或多个 `TableRows(Relation, rows)`、
`TableEnd(Relation)`；空表也有 begin/end。行包含 Text 或 Null，TOAST 值完整。
回调返回前必须持久完成目标写入，可在 TableBegin 清表，只有 TableEnd 标记该表 copy 完成。

所有回调和读取事务成功后返回不透明 `SnapshotPosition`，其 `lsn` 可读。
以新 checkpoint 路径传入 `subscribe(...,
start=AfterSnapshot(position))`，交接在创建 checkpoint 前检查源 system、timeline、database、
slot、publication；copy 期间提交的写入从槽的一致点重放。
也可在 copy 持久完成后 `position.save_checkpoint(path)`，再在新进程 `Resume`。
保存采用相同 lock/fsync/原子替换，拒绝覆盖已有 checkpoint。
不要在快照与订阅之间手工推进、重建或消费该槽。

初始 copy 应用 publication 列清单和行过滤，分区 root/leaf 身份遵循
`publish_via_partition_root`。普通继承表分别复制，不重复 children。需要 SELECT 权限。
reader 禁用 RLS，使权限不足失败，而非静默只复制子集。copy 期间不要改 schema 或
publication 成员；并发 DDL 协调与自动表同步未实现。

`batch_rows` 默认 256（1–1024），`max_frame_bytes` 默认 16 MiB，
`max_batch_bytes` 默认 64 MiB，后者限制每个查询累计 DataRow 线字节。
超大行/批次明确失败，不回调半批。元数据查询当前最多 1024 表、每表 1024 published 列。
回调提供背压，每个数据库操作采用 `connection.timeout_ms`。
对象/字符串开销及消费者持有数据不受这些线字节限制覆盖。

`copy_snapshot`、`copy_snapshot_to` 的 `copy_connections` 默认 1（1–256）。
大于 1 时，多 reader 导入相同导出快照。物理表按不重叠 CTID 范围划分，首尾边界开放；
分区 root 调度 leaf。规划使用当前存储大小和 catalog 行估算，不先扫描表。
reader 不超过配置数量，另有 exporter 和 metadata reader。空表仍触发 begin/end 和目标设置。
行顺序未定义；回调及目标操作仍串行，一个快照 attempt 共享 copy batch 序号。
任一 worker 失败或取消会取消其余 worker，并在返回前关闭所有导入事务。

已有槽被拒绝。回调失败、超时或取消时关闭全部 socket、结束读取事务，不产生
SnapshotPosition 或 checkpoint。新持久槽仍存在并保留 WAL；开始新 copy 前必须明确检查/
删除自有槽并丢弃目标半成品。此 API 不保存逐表进度，也不恢复部分快照。
内存交接位置在保存任一 checkpoint 前丢失，同样需要有意执行全新 bootstrap。

<a id="destinations-and-durability"></a>

## 目标与持久性

实现公共 `Destination` trait 接收快照和已提交事务。

外部适配器可构造已有 `CdcError` variant，共用错误分类。variant 与 payload 未变；
公开构造器不改变核心匹配或恢复语义。

`Transaction.destination_events()`、`WalBatch.destination_events()` 为适配器提供只读
`DestinationEvents`，保留有序变化、稳定事件 ID 和逐事件精确 schema。
`session` 是隔离内存重放债务的不透明交付寿命身份，不是持久事件 ID。
视图没有 `end_lsn` 或确认 API。片段的 `commit_lsn` 不证明收到 Commit，绝不能据此推进
checkpoint。已有 Transaction、WalBatch、Destination 和确认 API 不变。

向 `copy_snapshot_to` 或 `run_pipeline` 提供 `schema_store` 时，
`bind_snapshot_schema(relation, schema)` 在 schema 持久存储后、`reset_table` 前收到
导入快照的精确 schema，默认空实现。类型化目标应使用此版本，不应通过无上界 schema-store
查询猜测 copy 版本。每次写入返回 `Accepted` 或 `Durable`：Accepted 表示目标已接管工作；
Durable 表示该写入**及同一有序实例所有更早接纳写入**均已持久。
`flush()` 必须等待全部更早 Accepted 持久完成，或抛错。
未持久便成功返回违反契约，可能丢数据。

`with_destination(destination, run)` 先 startup，再运行流水线，成功后 shutdown；
默认 startup/shutdown 无操作。错误立即传播并跳过 shutdown，目标归属者须用自身资源
scope 在失败时取消和释放任务。一个实例不能共享给并发流水线：此契约是单一有序 writer
和累计全局持久屏障。

`copy_snapshot_to(config, destination, slot~, publication~, ...)` 在复制前重置每张表。
`reset_table` 必须先隔离此前 attempt，再清数据和重放标记。
空行写入无 batch ID，用于建空表；非空写入携带
`TableCopyBatchId { attempt, relation_id, sequence }`，每次新 copy 生成新 attempt。
表完成前，最后 Accepted 必须 flush。写入或 flush 失败不返回快照交接位置。
helper 不自行 startup/shutdown，应放在 with_destination 中。

订阅内调用 `sub.apply_to(destination, control~, ...)` 按序写事务，仅 Durable 或 flush 成功
后确认。Accepted 在 `flush_every` 个事务后 flush（默认 8，不大于 max_pending_transactions），
空闲时按 `flush_interval_ms`（默认 1000），或 `control.stop()` 请求正常停止时 flush。
Stop 排空已接纳工作再返回；已排队但未分派事务仍不确认。
失败和取消不排空或确认未完成写入。不要并发调用 next/ack 和 apply_to。

用事务 ID 和 copy batch ID 实现目标幂等。此契约不自行提供 schema planning 或内置云目标。
后述流水线增加持久逐表协调。checkpoint 仍是事务恢复依据；目标保存自己的业务/重放状态。
并发表同步时，同一主事务的投影在重试间可能增加表。流水线行效果应按 `event_id` 去重，
不能只按 `tx.id`，否则先前部分投影可能压制新归属表的重放事件。

<a id="persistent-table-pipeline"></a>

## 持久逐表流水线

`run_pipeline(config, destination, publication~, destination_id~, state_path~, control~,
slot_prefix?="mooncdc", copy_concurrency?=2, copy_connections_per_table?=1,
table_sync_monitor_refresh_interval_ms?=10000, batch_rows?=256,
table_retry_delay_ms?=10000, max_table_retries?=5,
table_sync_copy?=IncludeAllTables, slot_failover?=false,
invalidated_slot_behavior?=RejectInvalidatedSlot)` 拥有随机命名主复制槽，默认用临时槽做独立表快照。
它管理目标 startup/shutdown，保存带校验和、原子替换的状态文件及同级 `.checkpoint`。
两个文件、锁和目标持久数据必须一起保留。调用者提供能唯一表示目标数据集的 destination identity。
源、目标或 slot prefix 不符时，在目标 startup 前失败。

`slot_failover=true` 要求 PostgreSQL 17 或以上，主槽和表槽都启用 failover，必要时升级
已有主槽。关闭不降级已有槽。私有 copy 槽改为持久槽，由持久表状态拥有确定名称，
成功或失败均在关闭 exporter 后删除。启动在协调 publication 成员前清理已记录 copy 槽，
也覆盖表被移除或重启关闭 failover 的情况。清理在连接超时内等待槽 inactive，拒绝
不符 plugin/database 或 temporary 槽。清理失败停止流水线，已完成交接保留。

`copy_snapshot`、`copy_snapshot_to` 接受相同 `slot_failover`，但独立快照槽成功或失败后
仍由调用者拥有。物理 standby 槽同步需单独配置；独立快照交接和 `subscribe` 保留严格时间线绑定。

新 `run_pipeline` 进程以 `slot_failover=true` 可在晋升源恢复已有状态，但 System ID、
database、slot、publication、destination、slot prefix 仍须匹配。目标 startup 前验证新的
`TIMELINE_HISTORY`，寻找原时间线 fork，要求 checkpoint、初始快照、逐表 cutoff、
持久目标写入上界不晚于 fork。主槽必须已存在、持久、有效且启用 failover，restart 和
confirmed-flush 均不超过 checkpoint。重启前配置晋升后的连接并隔离旧 primary。

新流水线在主/私有 WAL 的目标/schema 写入以及快照 schema 持久化前，先保存写入上界。
部分 v1 批次用 Commit-start LSN 加一，因此 fork 恰在 Commit 记录起点会拒绝这些行。
这不是确认；每次增大的上界都增加一次持久状态写入。缺少该字段的旧状态可继续用于原时间线，
但不能自动改绑，后续写入不能虚构缺失历史。显式槽重建在全表重同步时初始化新上界。

晋升先保存 intent，在相同位置仅改写 checkpoint 的源绑定，再完成流水线绑定。
重启重复验证 ancestry 和槽，只接受 intent 指明的旧/新 checkpoint 对。
完成表保留状态，未完成 intent 不得开始目标工作；未完成期间再次晋升会保守失败。
运行中的主 worker 以 `slot_failover=true` 重连到同源不同 timeline 时，也可重新进入启动检查。
先关闭并等待所有旧源 worker，flush 已接纳目标写入，再重载 owner 保护的记录。
保留目标及状态 owner；owner 连接失败仍致命，要求新 scope。
晋升重入有独立 `max_table_retries` 上限，在 Initializing 等待 `table_retry_delay_ms`，可被停止中断。
controller 必须隔离旧 primary 并把配置 endpoint 路由到新源；这里不配置 standby、DNS/HA
路由或分布式归属。active CLI 矩阵覆盖已完成 COPY、独立健康 PostgreSQL state/schema store；
active COPY 重叠和其他 state backend 仍需进一步真实证据，见[验证范围](VALIDATION.zh-CN.md)。

`invalidated_slot_behavior=RecreateInvalidatedSlot` 显式允许从自有且 WAL 已失效的现有槽恢复。
默认拒绝，不重置目标、不改 checkpoint。槽缺失/不兼容、源/目标身份变更、checkpoint 损坏
和空 publication 仍致命；该选项不绕过这些检查，也不启用时间线晋升恢复。

重建先把所有表置 Pending，删除自有 copy 槽及失效主槽，再按 failover 配置创建新主槽，
保存其一致位置为新 checkpoint，然后重新复制选中表。
`table_sync_copy` 排除表仍重置并初始化为空，无法恢复旧 WAL 已丢事件；包含表获取新一致
快照并从该位置续接。目标元数据/schema 历史保留用于安全 reset 和协调。
不要手工消费或推进流水线槽。

临时流水线状态版本 2 记录持久重建 intent，仅含 Pending 表，不含已完成解码状态；
旧 checkpoint 不能作为新槽恢复前沿。带 recreate 选项重启时，任何目标工作前先完成或
重复 intent，创建回复中断后可能重复建槽。正常完成回到版本 1。
旧库拒绝重建中的版本 2；保留两份记录，使用支持实现及显式 recreate 完成恢复。

运行时槽失效会关闭旧源任务并 flush 目标，再启动新源 worker group，期间保留目标
bind/startup/shutdown 寿命。重建次数由 `max_table_retries` 限制，等待采用
`table_retry_delay_ms`。有意恢复等待为 initializing/unready，stop 立即唤醒。
flush 失败终止恢复，不发布新 checkpoint。这是丢 WAL 后的重新同步，不是恢复丢失事件。

流水线连接在 `pg_stat_activity` 暴露稳定 `application_name`：主 WAL 为
`mooncdc_replication:apply:<pipeline>`，快照 reader 为
`mooncdc_snapshot:table_sync:<pipeline>:<oid>`，exporter/私有 WAL 为
`mooncdc_snapshot_export:table_sync:<pipeline>:<oid>`。
`<pipeline>` 是持久主槽名 SHA256 的前 16 个十六进制字符，重启/重试稳定，不暴露目标配置，
也不增加用户标识。包含最大表 OID 仍满足 PostgreSQL 63 字节上限，其他连接保留 workload 名。

`table_sync_copy` 按源表 OID 选择初始行：默认 `IncludeAllTables`、`SkipAllTables`、
`IncludeTables([oid, ...])` 或 `SkipTables([oid, ...])`。
启动前拒绝重复 OID；空 IncludeTables 跳过全部初始行，空 SkipTables 复制全部。
这不是流过滤，所有 published 表仍加入 CDC。跳过表仍有独立快照/槽，捕获 schema 和解码
mask，重置/创建目标，完成空写持久屏障后交接；省略自身快照时已有的行及快照前 WAL 效果。
仅在有意接受这种部分目标状态时采用此策略。

重启改变策略只影响新表或未完成 copy（含重试），不重复制或清空已完成交接表。
策略每次运行提供，不作为不可变源身份持久化。独立 `copy_snapshot` 和 `copy_snapshot_to`
仍复制行。

`pipeline_status(path)` 按 OID 读取 Pending、Copying、Catchup(cutoff)、Ready(cutoff)、
Errored(reason, retry, attempts)。重启只有 Pending/Copying 获新快照和 reset，完成表保留
交接 cutoff，主流过滤 cutoff 前事件。行/消息保留原 `event_id`；truncate 按表拆成事件，
追加 `/table/<oid>` 防止不同 worker 片段冲突。目标持久先于交接。
其他表仍 Copying 时，主 checkpoint 可以前进；该表保留自有槽，崩溃后用新快照 reset。
未完成 copy 重启恢复，错误表保留重试策略。初始化后缺 checkpoint 会失败，不重置进度。
建槽 intent 恢复只能在任何目标写入前替换自己的随机槽。

复制并发为 1–256，与 `copy_connections_per_table` 的乘积不超过 256。
目标调用串行并采用累计屏障。主 worker apply ready 表，有界表 worker 同时复制和私有 WAL 重放；
Copying 状态包含该重放。表持久追上主 worker 后，在主投影共用 mutex 内保存 cutoff，再关临时槽。
私有重放事务 ID 带 `/copy/<temporary-slot>`，事件 ID 仍绑定主源血缘。
同步时跨表事务可能分投影交付，流水线不承诺跨表原子性。
receiver 采用有界背压和独立心跳。新 published 表在首个 WAL relation/row 或适用上游 DDL
消息时加入同队列；无这些消息的空新表在下次启动发现。
移出 publication 在启动协调，不删除目标数据。

copy/私有重放失败按表持久化，健康表继续。已知瞬时超时、断连、拒绝连接、shutdown/recovery、
连接 SQLSTATE 和 lock timeout 采用有界定时重试；协议、认证、schema/value、未分类错误需手动重试。
目标可用 `TableReplicationError::Retryable`、`Manual`、`Permanent` 分类表局部失败。
`Permanent` 记录 NoRetry，预算耗尽记录 ManualRetry；TimedRetry 保存 Unix 毫秒截止时间，
等待会释放 copy worker。活动流水线的 `control.retry_table(oid)` 重置预算并排新快照；
未知、健康或 NoRetry 表返回 false，单纯重启不重试 ManualRetry。
每次 retry 隔离并 reset 未完成表，不重放旧半快照。
state/checkpoint、不安全源身份、copy 侧累计 flush 失败仍停止整体流水线。
瞬时主 apply 源或目标 write/flush 失败会关闭失败订阅，新订阅从持久 checkpoint 重放。
复用 `table_retry_delay_ms` 和 `max_table_retries`，但使用覆盖整个 run 的独立预算；
初始源设置也消耗该预算，成功不重置。目标只 startup 一次，表 copy worker 保持活动，完成表不重复制。
手动/永久、未分类或预算耗尽错误返回原错误；retry 等待中 stop 立即唤醒、排空目标，失败工作不确认。
独立 `Subscription.apply_to` 仍直接传播错误，没有这个循环。

源连接、识别、建槽、publication 发现和 streaming I/O 都可进入 worker recovery。
每次 streaming 使用新订阅，从持久 checkpoint 重放 pending 事务。
流水线关闭订阅内层 reconnect，使源和目标共用一个有界预算；独立 `subscribe` 保留原策略和 pending 行为。
源身份/槽安全、认证、协议、回调、state/schema store 错误不会仅因发生在 worker 内就变可重试。
只有明确标记的 I/O/调用边界参与分类；源 socket OS 错误排除 permission/path 失败。
目标调用外的 startup 和 state/schema store 失败不重试。
目标必须保持幂等，在接纳重放前为旧 in-flight 操作保序；HTTP 超时不证明远端已停止。
目标自身不安全的元数据/恢复错误应标为不可重试，而非瞬时写入错误。
主流错误不跳过单表，只会保守重放或按策略/预算退出。额外表/目标传输分类是独立工作。
即使流水线运行，运维也应检查 Errored 状态。
初始 COPY 持有源表锁，后续私有 WAL catch-up 使用与主 apply 相同解码策略。
取消保留保守重放位置，`control.stop()` 排空已接纳写入。
退出后自有持久槽仍保留 WAL，永久退役时需运维显式清理。

<a id="ddl-schema-versions"></a>

## DDL schema 版本

`message.schema_snapshot(commit_lsn~, publication~)` 解析固定 Supabase ETL 的
`supabase_etl_ddl` JSON；其他 prefix/publication 返回 None，拒绝非事务 DDL 和畸形元数据。
保留物理列顺序、独立于 replica identity 的主键顺序、可空性、默认值和 type modifier。
未知 JSON 字段忽略。`schema.relation(published_columns)` 将 publication mask 与
DEFAULT/FULL/INDEX/NOTHING identity 语义相交；publication 成员变化后，新的 wire Relation 优先。

`SchemaId` 先按 commit LSN，再按 message LSN 排序，不能仅用 message LSN 排事务。
`SchemaId.before_lsn(checkpoint)` 将排他复制位置转换成包含边界的查找/保留上界，拒绝零。

公共 `SchemaStore` 支持 get/all/put/prune。`with_file_schema_store(path,
identity~, run)` 提供独占、带校验和的原子文件实现，限制 16 MiB。
identity 必须绑定 source/publication/destination。相同版本重放幂等，内容冲突失败。
文件持久成功后才发布缓存。`prune([(oid, bound), ...])` 保留每个 bound 之前/相等的最新版
及全部更新版。bound 不应新于持久源 checkpoint 或目标恢复仍需版本。
scope 关闭后访问失败。

`Destination.schema_retention(oid)` 返回持久目标元数据仍引用的最早 schema，使表参与清理；
默认 None，保留全部版本。ClickHouse 返回当前 schema，DDL Applying 时返回两端较早者。
checkpoint 成功持久后，流水线考虑 relation/row/DDL 事件涉及表，取此保留版本与
`SchemaId.before_lsn(persisted_checkpoint)` 的最小值，仅清理该 checkpoint 时主 apply 拥有的表。
排除 Copying、Errored 和未来 cutoff。清理持有目标锁，不与 reset/DDL 竞争。
lookup/prune 失败保留候选供后续 checkpoint，不使复制失败，也不授权额外确认。
这是尽力维护：无 retention 元数据的表在后续 relation/DDL 事件再考虑。
自定义目标启用前必须给出保守边界，包括待执行 DDL 和其他恢复引用。
已存交接 decoder 也固定其精确 schema；只有持久 checkpoint 到达交接点且当前连接已物化
该 decoder，才丢弃 payload。状态写失败保留 payload，并阻止本轮 prune。

通过 `run_pipeline(..., schema_store=store)` 传入 scope 内 store。
流水线在每个导入快照中、reset 前记录全表 bootstrap schema，使用 snapshot cutoff 和
message LSN 零。适用 DDL 在事务交给目标前持久化，仍以 Message 保留原事务顺序；
目标 apply 时调用 schema_snapshot，并实现自身 schema 演进策略。
copy 前过滤或其他 publication 的 DDL 不应用。
完成表恢复缺少所需保留 schema 时拒绝。store 仅保留消费元数据，丢弃 current_query 等上游 debug 字段。
外部 store 可保存 `schema.to_json()`，通过 `SchemaSnapshot.from_json(json)` 恢复；
验证后的 envelope 保留双 LSN、完整列和 replica identity，与文件 store 表示相同。

`schema.project(relation)` 创建 `ReplicatedSchema`，具有独立、全宽 publication 和 identity mask。
`identity_kind()` 区分 primary、full-row、alternative、missing identity；
`all_primary_keys_replicated()` 独立检查完整源 key。
`decode_row(values)` 按投影 type OID 转换 published 行，显式构造器验证 mask 宽度和成员。
`with_schema(next)` 在仅元数据 DDL 时保留 mask，成员/宽度变化需新的 Relation。

有 SchemaStore 时，`tx.replicated_schemas(index)` 返回事件选择的不可变 schema：
Relation/Insert/Update/Delete 一个，Truncate 每个 relation 一个，其他事件、无效 index 或 raw
订阅为空数组。一个事务中不同 schema 版本不混淆；raw changes、JSON、事件 ID 不变。
目标行写入前必须应用所返回 schema，ClickHouse 自动完成。

每次主 attempt 从持久 checkpoint 查 schema；表交接可提高上界，前置 DDL 选择精确版本，
重复 Relation 保留连接已建立的 schema 血缘。DDL 进入保留两份 mask 的 pending 状态。
行到达前若无替换 Relation，就组合 mask 与 pending snapshot；若无本地 mask，可在事务到
cutoff 后恢复已完成 copy 的持久 decoder。Truncate 要求完整连接 decoder。
copy 从导入快照初始化 decoder，只有完整状态可交接。
cutoff、紧凑 snapshot ID/publication/identity mask 在目标持久后一次性写流水线状态。
缺精确 snapshot、mask 无效或交接版本来自未来均保守失败。
旧状态无 decoder 时仍接受新 Relation，但不能提供无 Relation 回退。
源 retry 重置主 decoder，保留独立 copy decoder。

`before.plan_change(after, map_name?)` 按物理序规划列 DDL，区分源表变化和 publication 成员变化。
先验证目标名称冲突，先 drop，再 rename/add，用预留临时名拆 rename 环；
再进行 type/modifier、nullability、default 改变。每个操作含精确前后状态，主键改变另报。
确定性映射回调必须与创建/写入一致，默认 identity，计划中名称已映射。
规划不执行 DDL，也不表示目标支持所有操作；目标执行前必须检查能力。

流水线协商逻辑消息，拒绝非事务 DDL。产生真实 DDL 需要上游源 helper/event trigger。
应用可预装，调用 `run_source_migrations(connection, control?)`，或设置
`run_pipeline(..., source_migrations=true)`。库为兼容嵌入应用默认 false，独立 CLI
`run_source_migrations` 与参照一样默认 true。已停止 control 不执行迁移。
物理 standby 跳过安装，primary 必须先安装并回放后再复制。
可选 `control` 把显式迁移查询记为 `out_of_band`，流水线/CLI 传入已有 control；观察不推进状态。

两份固定 `integration/reference/etl` SQL 原样嵌入，安装需创建 schema/function 及数据库级
事件 trigger 权限，后者需 PostgreSQL superuser。迁移器与 SQLx 0.9.0 共用
`etl._sqlx_migrations`、SHA-384 校验和及数据库 advisory lock。
不相关成功版本保留，dirty 历史或自有版本 checksum 改变均拒绝。
每条待迁移及成功记录在一个事务提交；连接失败关闭并回滚未提交迁移，重试核验已提交历史。
不要修改已应用 SQL，也不要修 checksum 历史掩盖漂移。此 API 只装源 helper，
不装 Supabase state-store 表，不支持 down migration。

迁移会话使用 30s statement/client query、10s lock、60s idle transaction 超时。
连接设置采用 `ConnectionConfig.timeout_ms`，连接 scope 结束即释放迁移锁。
目标专属 DDL 能力仍属于各目标契约。

<a id="typed-text-cells"></a>

## 类型化文本 Cell

`value.decode_text(type_oid, array_element_oid?)` 将 PostgreSQL 文本传输转换为 `Cell`：
bool、有符号 int2/int4/int8、无符号 oid、float4/float8、精确 numeric 文本、bytea、UUID bytes、
JSON、date/time/timetz/timestamp/timestamptz，以及这些类型的一维数组。
text/name/varchar/bpchar 和未知标量 OID 保留 String。自定义数组需提供 catalog element OID。
拒绝嵌套数组；显式 lower bound 验证后丢弃，与参照的一维表示一致。
带引号 `"NULL"` 保留字符串，无引号 NULL 变 SqlNull，ToastUnchanged 与 SqlNull、JSON null 区分。
此纯文本 API 拒绝 Binary。内置 interval、network、range 及其他通用类型数组使用 String 元素，
遵循固定 rust-postgres 类型 catalog。

JSON 数字在 `Json::Number.repr` 保留精确十进制/指数文本，附带 Double 只是近似。
无损传输应使用 repr 或 `stringify()`。`Cell` 递归按精确表示比较 JSON 数字，
与 ETL arbitrary-precision JSON 配置一致，含 decimal scale。
JSON 容器上限 127，与 serde_json 默认预算一致。

Numeric 不经浮点展开指数为规范十进制，保留精确 scale 并规范负零。
浮点使用私有 C locale 的系统 C 运行时，float4 直接 binary32 舍入。
UUID 为 16 字节。日期采用天文年（1 BC = 0），有限范围 -262143 至 262142；
date/timestamp 无穷保留显式 variant。Time 以秒和纳秒表示，seconds=86400 区分 24:00:00 与午夜。
闰秒用 second 59 及 nanoseconds >= 1000000000，超过九位的小数截断，与参照一致。
Timetz 保留本地时间及偏移；timestamptz 跨日期/纪元规范为 UTC。
转换错误不静默代入 null 或舍入整数。

所有连接（含重连和快照 reader）启动请求 ISO/YMD 日期、UTC、十六进制 bytea、
extra_float_digits=3 和 PostgreSQL interval 格式。Raw Value 仍可用，类型转换显式进行；
本接口不增加 numeric 运算或目标特定强制转换。

<a id="clickhouse-destination"></a>

## ClickHouse 目标

`ClickHouseDestination::new` 接收 `endpoint`、`user`、`password`、`database`、
`publication`、`schema_store`。向 `run_pipeline` 传入同一 SchemaStore 和持久 StateStore，
数据库必须存在，每个目标使用专用 namespace。凭据放 HTTP header，HTTPS 用系统 trust 验证服务端。

默认引擎 `ReplacingMergeTree`，`MergeTree` 保存追加式 CDC 历史。
`max_insert_bytes` 默认 64 MiB，按编码字节拆批，但允许单行超限。
服务端预算：`connectivity_timeout_ms`（8000）、`schema_timeout_ms`（16000）、
`ddl_timeout_ms`（128000）、`insert_timeout_ms`（256000）。
每项加 `client_timeout_epsilon_ms`（4000），已有 `timeout_ms`（260000）仍是客户端总上限，
更长操作也需提高该上限。服务端设置使用整秒并至少为一，避免禁用限制。
连接/schema/DDL 用 `max_execution_time`，DDL 另加 `lock_acquire_timeout`；INSERT 使用 HTTP
send/receive 预算及逐语句客户端时限，匹配参照。客户端超时不证明服务端停止请求。
HTTP 写同步，只在服务端确认后返回 Durable。StateStore 归属不隔离此前远程 HTTP 请求。

已接纳的 ClickHouse 事务即使源调用者取消，仍持有实例顺序锁；重放、表 reset、shutdown
等待该批次在自身逐请求时限内完成或失败。等待接纳时取消仍立即生效，不发送请求。
同步目标将所有表串行化，因此屏障比参照的逐表后台 task fence 更广。
取消已接纳批次可能需等待全部剩余操作，外层超时不能覆盖排空。
请求自身超时或进程退出后，不承诺额外远程取消或分布式排序保障。

源下划线翻倍，schema/table 用 `_` 连接，例如 `public.my_table` → `public_my__table`。
ReplacingMergeTree 要求 published 主键，创建 `<target>__current` view，底层含
`_etl_version`、`_etl_deleted`。MergeTree 允许无主键表，增加 `cdc_operation`、
`cdc_lsn`、`cdc_tx_ordinal`。主键变化先写旧 key tombstone，再写新行。
缺失 unchanged TOAST 需 FULL 旧镜像，绝不替换 NULL。拒绝 NULL 数组和时间无穷，
可移植字面默认值保守转换。

重启保留完成表，写前验证持久目标身份、物理列名/顺序/类型和引擎。
快照重试 reset 未完成表。安装上游 DDL trigger 后，relation 事件或首个使用 pending schema
的行执行有序列 add/drop/rename 和放宽 nullability。
收紧 nullability 仍保持物理列可空；源默认值改变保留目标 add-time 默认值。
publication 新增标量列用无默认值 Nullable，使旧行仍 NULL；新增数组列要求重同步。
映射类型变化、ReplacingMergeTree 主键变化/重命名、嵌套 parent 移动在 DDL 前拒绝。
源表 rename 保留持久物理目标名。

外部变更前先保存 DDL intent。重启可恢复初始创建或明确的旧/目标 schema endpoint，
中间 DDL prefix 或列名复用需人工恢复。旧 schema 或乱序的同版本 publication mask 不驱动逆向 DDL。
测试覆盖注入状态写失败、重建中断状态，以及两种引擎各在四个创建/DDL 元数据边界 SIGKILL
后由新进程恢复。这不模拟断电或正在服务端执行的 INSERT，也不提供跨表事务提交保障。
copy batch 用稳定 dedup token；stream replay 去重取决于 ClickHouse 引擎与服务端窗口配置。

<a id="limits-and-diagnostics"></a>

## 上限与诊断

`ApplyControl.activities()` 返回 `run_pipeline` worker 的近似单调 inactivity 观察。
`health(stall_timeout_ms=300000)` 不查询数据库就评估。copy worker 用配置阈值；
WAL worker 使用该值与服务端 `wal_sender_timeout` 的较大者，后者禁用或不可用时取 60 秒。
age 等于阈值即 inactive。所有 worker 都参与，健康者不能掩盖 stalled 者。
获槽和 retry 等待不注册活动，完成/取消 worker 移除注册。

`PipelineHealth.live` 允许初始化和排空，但 inactivity 时失败。
`ready` 要求 Running、至少一个观察且无 inactive worker。
`status` 原因为 `ok`、`initializing`、`inactive`、`stopping`。
Stop 优先于 inactivity，立即移除 readiness 但保留 liveness。
初始 copy 期间也可能 ready，不表示持久或追平。流水线请求 WAL keepalive reply 以观察空闲流；
收到 keepalive 不能掩盖 apply write、flush 或 checkpoint 阻塞。
原始 `subscribe`/`apply_to` 不注册流水线观察。

`with_health_server(control, run, port=9001, stall_timeout_ms=300000)` 在异步宿主回调前绑定。
将初始化及运行放入回调，暴露 startup/running/draining；省略 wrapper 不监听。
GET/HEAD `/livez`、`/readyz` 返回匹配策略的 200/503 和纯文本原因。
未知路径 404，探针路径其他 verb 为 405。全接口 IPv4/IPv6 双栈，仅在不支持 family/protocol
错误时退回 IPv4；其他 bind 错误在回调前传播。端口 1–65535，超时须为正。

宿主负责 `control.stop()` 并等待排空。回调返回、抛错或取消关闭 listener 和连接，
wrapper 不发起 worker shutdown。每连接一请求后关闭，5 秒请求时限、最多并发 128。
探针不查数据库。[独立 replicator](REPLICATOR.zh-CN.md)提供配置与信号处理。

| 选项 | 默认值 | 含义 |
|---|---:|---|
| max_frame_bytes | 16 MiB | 分配前检查协议帧体长度 |
| max_transaction_bytes | 64 MiB | 每事务消息字节总和；v2 也限制开放缓冲总量 |
| max_transaction_events | 100000 | 含元数据的 Change；v2 也限制开放缓冲总量 |
| max_pending_transactions | 16 | 未完成已提交事务；v2 也限制开放 streaming 事务数 |
| backpressure | false | 队列满时等待持久确认 |
| max_metadata_entries | 4096 | relation 和 type 分别的最大数 |
| heartbeat_ms | 1000 | feedback 周期，须明显小于服务端 wal_sender_timeout |
| max_reconnects | 3 | 每订阅总重连尝试 |
| reconnect_delay_ms | 250 | 尝试间隔 |

限制针对保留的协议数据，不是精确 heap/RSS；解码对象和字符串另有开销。
reader 可能在发现队列超限前多组装一个有界事务。每个元数据条目还受帧长限制。
默认队列满失败，backpressure 则等待连续持久前缀释放空间，绝不丢最旧/最新数据。
提高上限后仍从同一 checkpoint resume。

诊断含重连次数、最后保留完整事务位置、持久位置、server WAL end、队列数、总 outstanding
及估算 WAL 字节 lag。lag 包括数据库内与该 publication 无关 WAL，不是事件数。

<a id="platform-and-authentication"></a>

## 平台与认证

native 后端、POSIX 本地文件系统（macOS/Linux）；不宣称 Windows、Wasm 或 JS 支持。
支持 SCRAM-SHA-256 和服务端 trust，不支持 MD5/明文密码认证、SCRAM-PLUS/客户端证书
及非 ASCII SASLprep 密码。`LocalPlaintext` 只接受 127.0.0.1 或 ::1。
`VerifyFull` 使用系统 roots 并验证 hostname，`VerifyCAFile(path)` 验证指定 CA 和 hostname。
不存在 TLS 降级或证书验证绕过；TLS 使用 async 依赖加载的 OpenSSL 运行时。
具体环境的验收报告与本页预期支持契约分别记录。
