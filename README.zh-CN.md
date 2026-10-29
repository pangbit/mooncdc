<a id="mooncdc"></a>

# MoonCDC

<!-- language-switch -->
[English](README.mbt.md) | [简体中文](README.zh-CN.md)

英文版本为权威版本。


MoonBit 编写的 PostgreSQL → ClickHouse CDC 库与独立同步工具。库与 CLI 同等支持，
共用 `run_pipeline` 核心；首批验收覆盖 PG17/18，边界和判据见
[当前交付范围](docs/RELEASE_SCOPE.zh-CN.md)。底层订阅使用原生 `pgoutput` 协议 v1，按完整的
已提交事务交付数据，提供显式确认、持久检查点和有限次数断线重连。

**0.1.1：Native 后端的 PostgreSQL → ClickHouse 库与 CLI。**
修复 ClickHouse 异常响应、快照取消顺序及相邻事务重放边界，见[变更记录](CHANGELOG.zh-CN.md)。
0.1.1 已于 2026-10-29 发布。验收方法及绑定源码的证据见[测试说明](docs/TESTING.zh-CN.md)。
采用至少一次投递，业务端必须处理重放。

2026-10-20 的固定核心提交已通过 Linux PG17/18 库4格和 CLI8格验收。
编译窗口的256 tasks 与数据库矩阵不同：本次矩阵内核峰值264，诊断临时采用
512上限、384停止水位，结束已恢复256。这是测试夹具预算，不是产品最低需求，
也不代表512已获永久授权或最终发布提交的远端CI已通过；版本绑定见测试说明。

<a id="installation-and-usage"></a>

## 安装与使用

先用 `moon view pangbit/mooncdc@0.1.1` 查询已发布版本，再通过
`moon add pangbit/mooncdc@0.1.1` 安装，并核对实际解析版本。
仓库中的可运行程序见[示例](examples/README.zh-CN.md)，源码包及注册库消费者检查见
[发布流程](docs/RELEASING.zh-CN.md)。

在 `moon.mod` 中设置 `preferred_target = "native"`，在使用方 `moon.pkg` 导入。
以下示例统一使用默认别名 `@mooncdc`：

```moonbit nocheck
///|
import {
  "pangbit/mooncdc",
}
```

```mbt check
///|
test "parse a PostgreSQL WAL position" {
  let position = @mooncdc.Lsn::parse("16/B374D848")
  assert_eq(position.to_string(), "16/B374D848")
  assert_true(position > @mooncdc.Lsn::parse("0/FFFFFFFF"))
}
```

嵌入 `run_pipeline` 的宿主可读取 worker 活动与健康状态；`ready` 表示正在运行且
未检测到卡住，不表示全量复制完成或已经追平 WAL。

```mbt check
///|
test "pipeline health before startup and during shutdown" {
  let control = @mooncdc.ApplyControl()
  assert_true(control.health().live)
  assert_false(control.health().ready)
  assert_eq(control.activities().length(), 0)
  control.stop()
  assert_eq(control.health().status, "stopping")
}
```

需要 HTTP 探针时，用 `with_health_server(control, () => { ... })` 包裹 pipeline
初始化及运行；默认端口 9001，提供 `/livez` 和 `/readyz`，未调用则不启动监听器。
宿主负责调用 `control.stop()` 并等待 pipeline 排空，详见 [健康接口](docs/API.zh-CN.md#limits-and-diagnostics)。

`run_pipeline` 默认复制全部初始行；可用 `table_sync_copy=SkipAllTables`，或
`IncludeTables([oid, ...])` / `SkipTables([oid, ...])` 按源表 OID 选择初始行。
跳过的表仍会重置目标、建立 schema 并接入后续 WAL；这不是流式过滤，也不会在重启时
补齐已完成表的历史行，详见 [pipeline 契约](docs/API.zh-CN.md#persistent-table-pipeline)。

`with_state_schema_store` 可把 schema 历史保存在文件或 PostgreSQL StateStore。
为 schema 与 pipeline 使用两个独立 namespace，并将同一个 `schemas` 传给目标端和
`run_pipeline`，即可在 PostgreSQL 中持久保存恢复所需的 schema，详见
[状态存储](docs/API.zh-CN.md#external-state-stores)。

典型消费流程如下；完整可运行程序见 [examples](examples/README.zh-CN.md)。

```mbt nocheck
let config = @mooncdc.ConnectionConfig::new(
  host="127.0.0.1", port=55418, user="cdc",
  password=password_from_environment, database="postgres",
  security=@mooncdc.LocalPlaintext,
)
@mooncdc.subscribe(
  config, slot="my_slot", publication="my_publication",
  start=@mooncdc.Resume, checkpoint_path="./state/checkpoint",
  sub => {
    for ;; {
      let tx = sub.next()
      persist_transaction_idempotently(tx) // 业务内容与 tx.id 一起持久化
      sub.ack(tx)                          // 之后才允许推进源端反馈
    }
  },
)
```

首次运行使用复制槽创建时返回的明确 LSN：`At(Lsn::parse("0/…"))`。
需要全量初始化时，使用 `copy_snapshot` 创建槽并读取一致性快照；逐批持久化成功后，
将返回值传给 `AfterSnapshot(position)` 衔接增量，详见 [快照接口](docs/API.zh-CN.md#initial-snapshot)。
也可用 `position.save_checkpoint(path)` 保存已持久化快照的衔接位置，再由新进程 `Resume`。
`Resume` 要求检查点已经存在；损坏、来源不符、槽失效或位置已不可恢复时明确失败。
不存在自动从最新位置继续的降级。

<a id="guarantees-and-boundaries"></a>

## 保证与边界

- 仅输出已提交事务，保留事件顺序、表信息、类型 OID、事务 ID 和 WAL 位置。
- `Null`、`Text`、`UnchangedToast` 分别表示 NULL、文本值和未变化的外部字段。
  可用 `subscribe(..., binary=true)` 请求二进制传输；`Binary(Bytes)` 保留原始类型编码，
  调用方结合列 OID 解码。默认仍为文本，二进制模式也可能收到回退文本。
- 独立订阅可显式选择 `protocol=V2Streaming`，缓冲未提交分段并在提交后交付；
  完整资源与回滚契约见 [API](docs/API.zh-CN.md)。流水线继续使用协议v1。
- `ack` 只越过连续完成的事务；检查点完成文件同步、原子替换及父目录同步后，
  才更新可反馈位置。接收位置不能代替业务持久位置。
- `tx.id` 标识事务，`tx.event_id(index)` 标识数据变更；重复元数据不改变数据事件 ID。
- 可用 `messages=true` 订阅逻辑消息：事务消息随提交交付，非事务消息需提供独立回调。
  消息内容保留原始字节；`schema_snapshot` 可解释 Supabase ETL DDL payload。
  `run_pipeline(..., schema_store=store)` 可持久保存初始 schema 与 DDL 版本，详见 API。
- 自定义 `Destination` 可通过 `copy_snapshot_to` 和 `sub.apply_to` 接入复制流程；
  `Accepted` 不触发确认，持久化屏障成功才推进检查点，支持空闲刷新及正常停止。
- 后台处理心跳；帧、事务、元数据及未确认队列有上限。超限关闭连接并保留检查点。
- SCRAM-SHA-256；远端连接使用验证证书和主机名的 TLS。明文仅允许数字回环地址。
  Unicode 密码采用兼容 PostgreSQL 的 SASLprep，包含完整 NFKC 规范化；准备失败时回退到原始密码。
- `run_pipeline` 提供持久逐表状态、并行独立快照、重启后失败表重建和增量追赶；
  完成的表保留快照进度，队列满时背压。运行中新发布表在首次 WAL 事件时发现并复制。
- `Value.decode_text(oid)` 提供类型化 Cell，支持精确数值、日期时间、JSON、UUID、bytea 和
  一维数组；SQL NULL、JSON null 与未变化 TOAST 分开表示。未知标量保留文本。
- `StateStore` 支持文件目录及 PostgreSQL 持久后端，接入流水线/订阅 checkpoint 和
  目标端元数据；逐表复制错误可持久化并定时或人工重试。详见 [API](docs/API.zh-CN.md)。
- `ClickHouseDestination` 支持 ReplacingMergeTree 当前值视图和 MergeTree 操作历史，
  接入全量/增量、列 DDL 及持久恢复。支持范围见 [API](docs/API.zh-CN.md#clickhouse-destination)。
- 本模块仅提供 PostgreSQL → ClickHouse，不包含 BigQuery、Snowflake 或 Iceberg。
  DuckLake 是路线图，不能视为已实现。
- 不提供自动切主或两阶段事务；各目标端的 schema 支持范围单独列明。
  不承诺跨系统恰好一次。

`replica identity` 决定旧值是否存在。`FULL` 能提供较完整旧行；默认主键身份可能仅有
键或没有旧元组。`UnchangedToast` 不能作为 NULL 覆盖已有业务值。详见
[API 与支持范围](docs/API.zh-CN.md)。

<a id="local-validation"></a>

## 本地验证

已知验证局限：合并源码的一轮 macOS release 普通测试中，copy-exit cleanup 与
reconnect retry 两个场景报 `TimeoutError`；同一源码、工具链、命令和时限下仅重试
该 job 一次后通过。原因尚未确认，不能视为已修复或据此认定产品缺陷。
具体时限、运行结果及版本绑定见[验证记录](docs/VALIDATION.zh-CN.md#github-ci-at-the-reviewed-source)。

使用专用 Docker Compose 项目，数据库端口只绑定 `127.0.0.1`：

```sh
moon update
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait
moon run tools/setup-db.mbtx pg18
moon run tools/setup-db.mbtx pg17
moon check --target native --deny-warn
moon test --target native
moon run tools/setup-tls.mbtx pg18
moon run tools/setup-tls.mbtx pg17
MOONCDC_TEST_PORT=55418 MOONCDC_TEST_CA=.test-artifacts/tls-pg18/server.crt moon run tools/live-test.mbtx
MOONCDC_TEST_PORT=55417 MOONCDC_TEST_CA=.test-artifacts/tls-pg17/server.crt moon run tools/live-test.mbtx
moon run examples/index
moon run examples/cache
moon run tools/recovery-test.mbtx
moon run tools/checkpoint-crash-test.mbtx
```

未设置 `MOONCDC_TEST_PORT` 时真实数据库测试体不会运行；普通测试通过不代表实测通过。
初始化脚本用于新建的隔离实例，不覆盖已有业务库。故障测试会终止本项目的 walsender
或示例子进程。详细命令、TLS 与清理说明见 [TESTING](docs/TESTING.zh-CN.md)。

<a id="documentation-and-maintenance"></a>

## 文档与维护

可运行入口与配置见 [独立 replicator](docs/REPLICATOR.zh-CN.md)。

- [示例](examples/README.zh-CN.md) · [API](docs/API.zh-CN.md) · [测试](docs/TESTING.zh-CN.md)
- [运行与 WAL 监控](docs/OPERATIONS.zh-CN.md) · [发布](docs/RELEASING.zh-CN.md)
- [开发原则](docs/DEVELOPMENT.zh-CN.md) · [贡献](CONTRIBUTING.zh-CN.md) · [变更记录](CHANGELOG.zh-CN.md)
- [依赖与协议来源](THIRD_PARTY.zh-CN.md) · [安全](SECURITY.zh-CN.md)

Apache-2.0，见 [LICENSE](LICENSE)。
