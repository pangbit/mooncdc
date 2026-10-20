# 独立 replicator

`moon build --target native --release` 后运行
`_build/native/release/build/cmd/replicator/replicator.exe --config config.json`。
增加 `--check` 只做配置与库参数校验，不连接服务，也不确认 CA 文件可用性。
启动失败会在 stderr 显示配置字段或错误原因，并返回 1；PostgreSQL 错误保留 SQLSTATE，
目标端错误保留分类及状态码。配置检查不会回显密码或无效输入值。

当前采用 MoonCDC 自己的配置字段，支持 JSON/YAML，尚不能直接加载 Supabase ETL 的完整配置。
支持 ClickHouse 和 PostgreSQL 状态/schema 存储；未知字段和未实现的目标端明确拒绝。

```json
{
  "id": "orders",
  "publication": "orders_publication",
  "run_source_migrations": true,
  "replication_slot": { "failover": false },
  "invalidated_slot_behavior": "error",
  "memory_refresh_interval_ms": 100,
  "memory_backpressure": { "activate_threshold": 0.85, "resume_threshold": 0.75 },
  "batch": { "memory_budget_ratio": 0.2, "max_bytes": 33554432, "max_fill_ms": 10000 },
  "source": {
    "host": "postgres.example.com",
    "user": "replicator",
    "database": "orders",
    "security": "verify_full"
  },
  "state": { "schema": "etl", "table": "mooncdc_state" },
  "destination": {
    "type": "clickhouse",
    "url": "https://clickhouse.example.com:8443",
    "user": "replicator",
    "database": "orders",
    "engine": "replacing_merge_tree"
  },
  "health": { "port": 9001, "stall_timeout_ms": 300000 },
  "metrics": { "port": 9000 },
  "copy_concurrency": 4,
  "copy_connections_per_table": 4,
  "table_sync_monitor_refresh_interval_ms": 10000,
  "batch_rows": 256,
  "table_retry_delay_ms": 10000,
  "max_table_retries": 5,
  "table_sync_copy": { "type": "include_all_tables" }
}
```

密码可由 `APP_SOURCE__PASSWORD` 和 `APP_DESTINATION__PASSWORD` 注入。所有 `APP_`
配置覆盖都保留字符串原值，整数参数另行严格解析；`true`、`00123` 等密码不会被转型。
嵌套字段使用 `__` 分隔、键忽略大小写。数组选择列表从 JSON/YAML 提供。

其他目标不在本模块或 CLI 的交付范围内。核心 CLI 在读取目标凭据前拒绝 BigQuery、Snowflake、Iceberg 和 DuckLake。

`copy_concurrency` 控制同时复制的表数，默认 4，范围 1–256。
`copy_connections_per_table` 控制单表读取连接，默认 4，范围 1–256；两者乘积不得超过
256。环境覆盖为 `APP_COPY_CONNECTIONS_PER_TABLE`。启用并行读取时，各连接导入同一
快照，以 CTID 范围分片；每表另外保留导出与元数据连接，配置值不是数据库总连接数。
目标写入仍串行，空表和跳过复制的表仍完成 schema/目标初始化与持久屏障。行顺序无保证。
这两个 CLI 默认值与固定 ETL 一致；库调用继续保留原默认 2/1，调用方可显式覆盖。
复制期独立槽监测周期 `table_sync_monitor_refresh_interval_ms` 默认 10000ms，须为正 Int，
可用 `APP_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS` 覆盖。每个活动复制最多一个独立采样
连接，不在快照 exporter 上执行 SQL。槽消失/失效会取消复制，再按表重试规则处理；
一般采样连接/查询失败只计数，后续继续采样。停止会取消并等待未完成的采样操作。
同一周期也用于主 apply 和私有 WAL 追赶的独立当前源 LSN 采样；暂停源读取或阻塞目标
write/flush 不会暂停采样。样本只更新 lag 和年龄指标，不推进确认或刷新业务活动。
每个活动 WAL worker 额外至多一个查询连接，采样完成后延迟重试，不积压 tick。

省略 `--config` 时，在 `${APP_CONFIG_DIR:-configuration}` 中依次选择 `base` 和
`${APP_ENVIRONMENT:-prod}` 文件，各自按 `.yaml`、`.yml`、`.json` 顺序选择首个存在的
普通文件，递归合并对象，再应用环境变量。环境仅允许 `dev`、`staging`、`prod`；
两份文件均必需。显式 `--config` 跳过分层文件，但仍应用环境覆盖；`.yaml`/`.yml`
使用 YAML 解析，其余路径按 JSON 解析。文件最多 1 MiB。

YAML 支持注释、块/流集合、多行字符串、锚点与别名；根必须为单个映射，空文件代表
空配置。重复键、多文档及无效别名拒绝。隐式布尔值只有小写 `true`/`false`；
密码中的数字和布尔字面量应加引号。`<<` 保留为普通键，不执行合并。
配置嵌套最多 64 层，展开后的节点数与字符串/键长度合计最多 1,048,576；别名按完整
展开量计入预算。非有限数值、复杂映射键拒绝。错误不回显 YAML 原文或值。

连接默认端口 5432、数据库 postgres、超时 5000ms、空密码。`security` 可取
`verify_full`、`ca_file`（同时提供 `ca_file` 路径）、`local_plaintext`（仅数字回环）。
连接支持可选 `hostaddr`（数值 IPv4/IPv6），拨号地址与 TLS `host` 分开；明文连接的
实际地址也必须为回环。`keepalive` 对象支持 `idle_secs`、`interval_secs`、`retries`，
默认 30、10、3，均须为正数。环境覆盖如 `APP_SOURCE__HOSTADDR` 和
`APP_SOURCE__KEEPALIVE__IDLE_SECS` 同样适用于分层配置。
`state.connection` 可提供独立连接；缺省复用源连接。状态表所在 schema 必须预先存在。
**状态表必须排除在源 publication 外**；`FOR ALL TABLES` publication 需要另一个状态数据库。
`run_source_migrations` 默认 true，在获取状态存储前安装固定上游源 DDL helpers/trigger。
需要创建 event trigger 的 superuser 权限；低权限运行应设为 false，并由管理员预先安装。
物理 standby 跳过安装，需先在 primary 安装并等待回放。详见 [API](API.md)。

`replication_slot.failover` 默认 false；环境覆盖为 `APP_REPLICATION_SLOT__FAILOVER`。
启用需 PostgreSQL 17+，主槽与逐表复制槽均启用 failover，已有主槽按需升级；
关闭选项不降级已有主槽。逐表槽改为持久槽，并由已保存的表状态负责退出和重启清理。
standby 的槽同步需另行配置。隔离旧主库后，以原状态存储和新源连接启动新进程，可恢复
经过 ancestry/fork 校验的晋升：checkpoint、快照、表交接点及持久写入上界都须在祖先
时间线分叉范围内，主槽须为现有且有效的持久 failover 槽。旧状态缺少写入上界时拒绝
自动改绑。运行中主worker重连观察到同源timeline变化后，关闭旧worker并flush，重新做
相同证明；健康的独立状态owner保留，owner连接丢失则失败退出，须用新进程获取新owner。
入口新进程及同PID切换已在PG17/18、JSON/YAML、两种CH引擎验证，见
启动晋升[验收版本绑定](VALIDATION.md)、活跃晋升[验收版本绑定](VALIDATION.md)
和[API](API.md)。活跃COPY/其他状态后端、组合故障与自动HA路由仍待补。

`invalidated_slot_behavior` 默认 `error`，可显式选择 `recreate`；环境覆盖为
`APP_INVALIDATED_SLOT_BEHAVIOR`。只允许重建现有、身份匹配的失效槽，不绕过缺槽、源/目标
变更、损坏 checkpoint 或空 publication。重建先保存全表 Pending 意图，再创建主槽和新
checkpoint，随后按复制选择重新初始化目标；被跳过的表无法补回已经丢失的 WAL。
中断期间状态版本为 2，须用支持此版本的程序及显式 recreate 配置继续；完成后为版本 1。
运行中失效会排空旧目标写入并重启源 worker，重试次数/等待使用现有 retry 配置。
恢复等待为 initializing/unready，信号可立即结束等待。详见 [API](API.md)。

内存压力控制默认开启：每 100ms 读取主机或 cgroup，使用量达到 0.85 时暂停新的源读取，
严格低于 0.75 时恢复。`memory_backpressure: null` 关闭压力暂停；省略该字段保留默认开启。
显式对象须同时包含 `activate_threshold` 和 `resume_threshold`，范围分别为 `(0,1]`
和 `[0,1)`，后者必须小于前者。`memory_refresh_interval_ms` 始终须为正整数。
环境覆盖为 `APP_MEMORY_REFRESH_INTERVAL_MS`、
`APP_MEMORY_BACKPRESSURE__ACTIVATE_THRESHOLD` 和 `APP_MEMORY_BACKPRESSURE__RESUME_THRESHOLD`；
从未提供对象的配置覆盖阈值时须同时提供两个阈值。数字字符串支持十进制与科学记数法。
暂停不阻止 Accepted 写入排空、checkpoint 和心跳；此选项不能视为硬内存上限。
库 API 默认仍为显式启用，见 [API](API.md)。

`batch` 省略或 `{}` 默认启用，与固定 ETL 一致；显式 `null` 保留已有关闭方式。默认使用库的
默认预算。可在 JSON/YAML 中分别覆盖以下字段，未提供的字段使用库默认值：

```json
{ "batch": { "memory_budget_ratio": 0.2, "max_bytes": 33554432, "max_fill_ms": 10000 } }
```

比例须在 `(0,1]`，字节数为正 UInt64，填充间隔为非负 Int 毫秒；零间隔每项刷新。
比例支持数字及科学记数法字符串，填充间隔支持整数及整数字符串。`max_bytes` 支持
JSON 十进制整数字面量和十进制数字字符串，不接受字符串的符号、小数或指数形式。
YAML 的大字节整数应加引号；没有精确整数表示的数值仅接受不超过 `2^53-1` 的整数，
避免浮点舍入。环境覆盖为 `APP_BATCH__MEMORY_BUDGET_RATIO`、`APP_BATCH__MAX_BYTES`、
`APP_BATCH__MAX_FILL_MS`；嵌套覆盖可从省略或 null 的配置启用 batch，其余字段仍用默认值。

批次与压力暂停共用 `memory_refresh_interval_ms` 采样周期。关闭压力暂停仍可启用
批次容量刷新。快照按估算字节拆批并排空；启用 batch 的 ClickHouse 主 WAL 和私有
WAL 逐事件解码、分批排空，保留原始事件 ID。预算是启发式，单个事件可以超出，
不是硬内存上限。目标可以在接收 Commit 帧前看到部分事务，但 checkpoint 与表交接
仍须等有效 Commit 和持久屏障。显式 `batch: null` 保留整事务路径。
配置验证见 批次验收[验收版本绑定](VALIDATION.md)，逐事件实现与真库证据见
WAL 事件验收[验收版本绑定](VALIDATION.md)。这不包含 pgoutput v2 的未提交事务 streaming。

`id` 在同一状态表内必须唯一且重启保持不变；程序分别使用 `<id>/state` 和
`<id>/schemas` 两个独占 namespace。改变源/目标身份会拒绝旧状态恢复。
退出保留持久槽与状态；删除状态或槽不是正常重启步骤。

健康监听默认关闭；配置后绑定所有接口，暴露 `/livez` 与 `/readyz`。
SIGINT/SIGTERM 请求停止并等待流水线和目标端排空。所有异步作用域退出后，入口明确
返回成功退出码 0；排空期间发生错误则输出错误并返回 1，不会被信号覆盖为成功。
停止期间健康状态为 live/unready。排空受各操作自身超时约束，没有独立总截止时间。

metrics 默认在独立的 9000 端口监听所有接口；`metrics: null` 或
`metrics: { enabled: false }` 关闭，`metrics.port` 范围 1–65535，必须与启用的健康
端口不同。`metrics.listen_address` 可显式指定数字 IPv4 或未加方括号的 IPv6
地址，例如 `metrics: { port: 9000, listen_address: "127.0.0.1" }`。
省略地址保持原来的所有接口行为；显式地址不做 DNS 解析或 IPv4 回退。
地址中不能包含端口、空白或 IPv6 scope；非法地址即使在禁用 metrics 时也拒绝。
环境覆盖为 `APP_METRICS__PORT`、`APP_METRICS__ENABLED`、
`APP_METRICS__LISTEN_ADDRESS`。
`/health` 返回 `OK`，其他路径（包括 `/metrics`）提供 Prometheus 文本，HEAD 不发送正文。
其生命周期与 CLI 相同：先绑定，再启动流水线；正常、失败及取消退出均关闭监听和连接。
健康与 metrics 端口绑定失败会阻止启动，不会触发源库迁移。指标没有身份认证；部署时
监听可达范围由运行环境控制。字段含义和固定参照映射见 [metrics](METRICS.md)。
CLI将同一个control传给源迁移、流水线、schema和状态库，显式SQL按两个固定pool类别
计时。连接建立/认证和状态mutex等待不在查询计时中；指标不改变状态owner或durability。

可选错误报告默认关闭，配置示例：

```json
{
  "sentry": {"dsn": "https://public-key@sentry.example/42"},
  "supabase": {
    "project_ref": "project-ref",
    "api_url": "https://api.example",
    "api_key": "replace-through-environment"
  }
}
```

`APP_SENTRY__DSN`、`APP_SUPABASE__API_KEY` 可注入秘密。Supabase未提供api_url时只提供
project标签，不发通知；提供时必须有非空api_key。终止表错误先持久化再报告，TimedRetry
和未变化的终止状态不报告；CLI最终失败也报告，仍保留原退出码。每个请求10s、响应64KiB，
失败仅输出固定诊断文字，不替换状态写入结果。报告在当前作用域内等待，启用两端时每个
错误可增加最多约20s网络等待。接口含 `/emails`，启用真实端点会向其配置的收件人发通知。

已知连接/目标/报告秘密的字面值在错误正文中替换；正文截到16KiB，并非任意秘密发现器。
通知dedup使用redacted正文的SHA-256，不能与Rust DefaultHasher的key字节等同。DSN/URL
接受ASCII DNS/IPv4、端口及路径前缀，无query/fragment；IPv6/IDNA尚未支持。
`APP_ENVIRONMENT` 为dev/staging/prod，`APP_VERSION` 为可选报告metadata，二者是保留环境
字段。panic/backtrace、限流缓存和真实报告服务验收仍未完成。
验证和剩余范围见报告[验收版本绑定](VALIDATION.md)。

`supabase.configcat_sdk_key` 可选，缺省/null关闭；设置后持有有界的ConfigCat轮询适配器，
默认全球CDN、60s自动轮询、整轮30s超时。支持SDK key的两种固定格式，custom base URL
也允许非空`configcat-proxy/` key；按UTF-8字节校验长度，错误不回显key。`project_ref`
保留为默认user身份，不传入CDN请求。`--check`只校验配置，不启动轮询。

MoonCDC额外提供`supabase.configcat_base_url`、`configcat_poll_interval_ms`和
`configcat_http_timeout_ms`，可映射自有代理和受控测试；它们不是固定ETL配置字段，
须同时配置SDK key，周期与超时须为正Int。对应`APP_SUPABASE__CONFIGCAT_*`覆盖同样校验。
GET使用V6配置路径与ETag，200缓存、304保留原ETag，网络/状态/解析失败保留旧JSON，
403/404只更新已有缓存的刷新时间。V6偏好重定向最多三次，每轮响应最多1MiB。
轮询不积压，不改变复制配置或确认；正常、失败和取消退出均等待作用域关闭HTTP连接。

固定ETL入口仅初始化并持有客户端，未实际求值开关；当前适配器校验V6根结构和偏好，
保留原JSON。完整SDK规则模型/求值/外部缓存、HTTP3xx跳转、IPv6/IDNA端点和真实CDN
验收尚未实现；整轮超时比上游逐请求超时更紧。受控证据和固定源码链接见
ConfigCat验收[验收版本绑定](VALIDATION.md)，不能以此宣称完整C8验收。

尚未实现上游完整配置格式、多目标端及统一停止超时。
固定配置逐字段映射、库兼容默认与本地限制见 [配置映射](PIPELINE_CONFIG_MAPPING.md)。
验证命令和当前证据见 验收记录[验收版本绑定](VALIDATION.md)。
YAML 固定版本差分、限制和失败输入见 YAML 验证[验收版本绑定](VALIDATION.md)。
