# MoonCDC

MoonBit 编写的 PostgreSQL 变更订阅库。使用原生 `pgoutput` 协议 v1，按完整的
已提交事务交付数据，提供显式确认、持久检查点和有限次数断线重连。

**0.1.0 发布候选，尚未发布到 Mooncakes。** Native 后端；实测版本及环境见
[测试报告](docs/reports/VALIDATION_FOLLOWUP.md)。采用至少一次投递，业务端必须处理重放。

## 安装与使用

发布后通过 `moon add pangbit/mooncdc` 安装。发布前可在本仓库运行示例，或按照
[发布检查](docs/RELEASING.md) 使用打包源码做独立消费者验证。

在 `moon.mod` 中设置 `preferred_target = "native"`，在使用方 `moon.pkg` 导入：

```moonbit nocheck
///|
import {
  "pangbit/mooncdc" @cdc,
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

典型消费流程如下；完整可运行程序见 [examples](examples/README.md)。

```mbt nocheck
let config : @cdc.ConnectionConfig = {
  host: "127.0.0.1", port: 55418, user: "cdc",
  password: password_from_environment, database: "postgres",
  security: @cdc.LocalPlaintext, timeout_ms: 5000,
}
@cdc.subscribe(
  config, slot="my_slot", publication="my_publication",
  start=@cdc.Resume, checkpoint_path="./state/checkpoint",
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
将返回值传给 `AfterSnapshot(position)` 衔接增量，详见 [快照接口](docs/API.md#initial-snapshot)。
也可用 `position.save_checkpoint(path)` 保存已持久化快照的衔接位置，再由新进程 `Resume`。
`Resume` 要求检查点已经存在；损坏、来源不符、槽失效或位置已不可恢复时明确失败。
不存在自动从最新位置继续的降级。

## 保证与边界

- 仅输出已提交事务，保留事件顺序、表信息、类型 OID、事务 ID 和 WAL 位置。
- `Null`、`Text`、`UnchangedToast` 分别表示 NULL、文本值和未变化的外部字段。
  可用 `subscribe(..., binary=true)` 请求二进制传输；`Binary(Bytes)` 保留原始类型编码，
  调用方结合列 OID 解码。默认仍为文本，二进制模式也可能收到回退文本。
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
  首版密码限定可打印 ASCII，明确拒绝需要 SASLprep 的非 ASCII 密码。
- `run_pipeline` 提供持久逐表状态、并行独立快照、重启后失败表重建和增量追赶；
  完成的表保留快照进度，队列满时背压。运行中新发布表在首次 WAL 事件时发现并复制。
- `Value.decode_text(oid)` 提供类型化 Cell，支持精确数值、日期时间、JSON、UUID、bytea 和
  一维数组；SQL NULL、JSON null 与未变化 TOAST 分开表示。未知标量保留文本。
- `StateStore` 支持文件目录及 PostgreSQL 持久后端，接入流水线/订阅 checkpoint 和
  目标端元数据；逐表复制错误可持久化并定时或人工重试。详见 [API](docs/API.md)。
- `ClickHouseDestination` 支持 ReplacingMergeTree 当前值视图和 MergeTree 操作历史，
  接入全量/增量、列 DDL 及持久恢复。支持范围见 [API](docs/API.md#clickhouse-destination)。
- 不提供自动切主或两阶段事务；各目标端的 schema 支持范围单独列明。
  不承诺跨系统恰好一次。

`replica identity` 决定旧值是否存在。`FULL` 能提供较完整旧行；默认主键身份可能仅有
键或没有旧元组。`UnchangedToast` 不能作为 NULL 覆盖已有业务值。详见
[API 与支持范围](docs/API.md)。

## 本地验证

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
或示例子进程。详细命令、TLS 与清理说明见 [TESTING](docs/TESTING.md)。

## 文档与维护

- [示例](examples/README.md) · [API](docs/API.md) · [测试](docs/TESTING.md)
- [运行与 WAL 监控](docs/OPERATIONS.md) · [发布](docs/RELEASING.md)
- [开发原则](docs/DEVELOPMENT.md) · [贡献](CONTRIBUTING.md) · [变更记录](CHANGELOG.md)
- [依赖与协议来源](THIRD_PARTY.md) · [安全](SECURITY.md)

Apache-2.0，见 [LICENSE](LICENSE)。
