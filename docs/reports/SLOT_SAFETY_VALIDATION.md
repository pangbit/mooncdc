# PostgreSQL 槽安全与低流量 WAL 验证

日期：2026-10-02。基线提交：`b753b5d5c1e1b1f2fb6e9d85f55b30fd9eb50298`，
加当前工作区中的恢复拒绝测试、slot-safety 测试及配置。生产代码与该提交一致。

PostgreSQL 18.6 和 17.11 均通过外部推进槽、真实 WAL 失效、低流量 publication 三个场景。
前两个场景明确拒绝 Resume，检查点全部字节保持不变，独占锁正常释放。
第三个场景确认：即使心跳正常且没有未确认事务，无关表写入仍可增加 WAL 保留跨度。

## 环境与边界

- 本地 macOS 客户端，MoonBit native；`moon 0.1.20260920 (914d7da)`。
- 专用 Compose 项目 `mooncdc-slot-test`，数据库 `mooncdc_slot_safety`，只绑定
  `127.0.0.1:55428/55427`。未使用普通 `mooncdc-test`、共享库或生产库。
- PostgreSQL 18.6 / 17.11 Debian 镜像，服务端架构 aarch64，SCRAM 认证、回环明文。
- 启动参数与运行中 `pg_settings` 一致：logical WAL、`max_slot_wal_keep_size=0`、
  `wal_keep_size=0`、`wal_sender_timeout=5s`、`checkpoint_timeout=1h`、
  `min_wal_size=32MB`、`max_wal_size=64MB`；WAL 段 16 MiB。
- 整个测试体超时 60 秒；失效最多尝试 8 次 WAL 切换，每次先写一行再切换并 CHECKPOINT。
  128 MiB 是切换跨度上限，不是数据库全部 I/O 或磁盘占用上限。

镜像 RepoDigest：

| 版本 | SHA256 |
|---|---|
| 18.6 | `5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722` |
| 17.11 | `d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f` |

## 实测结果

| 场景 | PG18 | PG17 | 验收 |
|---|---|---|---|
| 外部 `pg_replication_slot_advance` | checkpoint `0/1BDEBE0`，槽推进到 `0/1BDECF0` | checkpoint `0/194DA18`，槽推进到 `0/194DB60` | 返回 `UnsafeResume`，原因是槽超过 durable；checkpoint 字节不变 |
| WAL 被清理 | 第 1 次切换后 `lost / wal_removed`，restart LSN 为空 | 第 1 次切换后 `lost / wal_removed`，restart LSN 为空 | 返回 `UnsafeResume`，原因是槽失效；checkpoint 字节不变 |
| 低流量 publication | 三次采样，零重连、零 outstanding | 三次采样，零重连、零 outstanding | 槽 active，received、durable、confirmed 与 restart 不动，server end 增长 |

低流量场景每次向未发布的 `cdc_noise` 插入 1000 行约 1 KiB 文本，等待 2200 ms 后采样，
共三次；累计等待 6.6 秒，超过服务器的 5 秒发送超时。发布的 `cdc_safety` 表期间无写入。

| 采样 | PG18 retained WAL bytes | PG17 retained WAL bytes |
|---|---:|---:|
| 1 | 1,107,792 | 1,091,320 |
| 2 | 2,199,032 | 2,182,560 |
| 3 | 3,290,296 | 3,273,800 |

数值为 `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`，表示 LSN 跨度，
不等于物理目录大小。PG18 的 restart/confirmed 分别固定在 `0/1BDEDC0 / 0/1BDEDF8`，
PG17 固定在 `0/194DC30 / 0/194DC68`。每个版本的持久检查点等于自己的 confirmed 位置。
这证明当前短时行为，不能外推长期磁盘增长速率、峰值吞吐或自动恢复能力。

有限槽保留在 checkpoint 时可能使落后槽失去 WAL，符合
[PostgreSQL 配置说明](https://www.postgresql.org/docs/18/runtime-config-replication.html)
及 [槽状态定义](https://www.postgresql.org/docs/18/view-pg-replication-slots.html)。
MoonCDC 保留 durable-only 反馈与严格恢复策略；本次未引入 server-end 自动推进。

## 验证与复现

完整命令见 [TESTING](../TESTING.md#isolated-slot-safety-tests)。本轮两个版本分别运行
`MOONCDC_SLOT_TEST_PORT=<55428 或 55427> moon test --target native --filter 'slot safety live*'`，
各 1/1 通过；这是一个顺序执行三个场景的测试体，不是三个独立 runner entries。
初次两版本均通过后，为避免客户端关闭与服务端释放槽的时序竞争，增加了有超时的
inactive 状态等待，并用全新容器重跑两版本通过；上表记录最终这次运行。

普通 native 套件 25/25 runner entries 通过：18 个执行体活跃，5 个普通 live 体、
1 个进程 worker、1 个专用 slot-safety 体未启用。`moon check --target native --deny-warn`、
`moon info --target native`、`moon fmt`、`moon fmt --check` 和 `git diff --check` 均通过，
生成的 `.mbti` 无变化。

格式化后的测试与配置 SHA256：

- `slot_safety_wbtest.mbt`：`8e79f1572cc027b3c9ad8ad0cd8a79ec79a5876bad4ed0c1aae45e527e1ada92`
- `integration/slot-safety.compose.yaml`：`616a003fb5ed6f29338284d06595b599a565c723d75ce33441f78cbf60c4e48f`

原始日志保留在忽略目录 `.test-artifacts/validation/`：`slot-safety-pg18.log`、
`slot-safety-pg17.log`、`slot-safety-containers.log`。成功结束时 SQL readback 确认两个库均无
剩余复制槽，测试表和 publication 已移除；随后仅清理本轮 Compose 项目及其卷。

CI 已增加独立 fixture、独立进程运行此用例及 always 清理步骤；工作流尚未推送或远端执行。
本轮未重跑其他真实库场景、TLS、SIGKILL 或性能测试。客户端重连拒绝的 8 个合成场景
仍是独立证据层，见 [参考核查](REFERENCE_REVIEW.md#恢复拒绝测试跟进)。
