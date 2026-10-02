# MoonCDC 跨语言参考实现核查

日期：2026-10-02。MoonCDC 核查基线：`d0172f86c8398cbad3cc239a9504db1e5b6a64b1`。

本次按协议、恢复架构、异常场景三个层次对照 pglogrepl、Supabase ETL 和
Debezium。已核查路径中未发现需要修改生产代码的明确缺陷；补充了直接读取反馈报文的
回归测试。MoonCDC 继续保持完整已提交事务、连续 ack、检查点持久化后反馈的语义。

这是针对上述路径的源码与测试核查，不是完整安全审计、上游互操作实测或性能比较。
没有引入依赖、移植上游代码或扩展公共 API。CodeGraph 不支持 MoonBit，本次直接读取文件。

## 参考来源

上游 Git HEAD 在本次读取时固定为以下提交；后续复核应使用这些固定链接。
Debezium stable 文档及 PostgreSQL 18 文档按本次访问内容引用，可能继续更新。

| 来源 | 固定版本和阅读入口 | 用途 |
|---|---|---|
| pglogrepl Go | `4ae5c490f7ce97fb0b918eed3023b35c16990f5a`：[复制反馈](https://github.com/jackc/pglogrepl/blob/4ae5c490f7ce97fb0b918eed3023b35c16990f5a/pglogrepl.go#L703-L790)、[逻辑消息](https://github.com/jackc/pglogrepl/blob/4ae5c490f7ce97fb0b918eed3023b35c16990f5a/message.go) | 对照字段、编码、消息边界 |
| Supabase ETL Rust | `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`：[架构](https://github.com/supabase/etl/blob/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4/site/content/docs/explanation/architecture.mdx)、[Destination](https://github.com/supabase/etl/blob/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4/crates/etl/src/destination/base.rs)、[StateStore](https://github.com/supabase/etl/blob/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4/crates/etl/src/store/state/base.rs) | 对照持久化契约和全量增量协调 |
| Debezium Java | [PostgreSQL connector 文档](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) | 提取槽与 offset 不一致、TOAST、事务元数据、低流量 WAL 场景 |
| PostgreSQL 18 | [复制协议](https://www.postgresql.org/docs/18/protocol-replication.html)、[逻辑消息格式](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html) | 协议裁决依据 |

## 协议对照结果

pglogrepl 的 `SendStandbyStatusUpdate` 在 flush 或 apply 为零时，将它们设为 write
位置。该行为属于底层 API 的默认值，不能证明调用方已经完成业务持久化。
MoonCDC 将三个字段全部设为 `durable`，保留这个更保守的选择。

| 核查项 | MoonCDC 实现 | 结论与证据 |
|---|---|---|
| 反馈封装 | [replication.mbt](../../replication.mbt) 的 `Connection::feedback` | CopyData 内为 `r`、三个 64 位位置、PostgreSQL 纪元微秒时间、reply 字节，载荷 34 字节；新增测试读取实际报文 |
| 接收与服务器进度 | [subscription.mbt](../../subscription.mbt) 的 `read_stream` | XLogData/keepalive 更新 `server_end`；仅完整 Commit 更新 `received`；二者都不直接推进 `durable` |
| 提交边界 | [decoder.mbt](../../decoder.mbt) 的 `Decoder::decode` | 分别保留 commit LSN 和 end LSN，核对 Begin/Commit 的 LSN 与时间；到 Commit 才交付；checkpoint 使用 end LSN |
| NULL 与 TOAST | [decoder.mbt](../../decoder.mbt) 的 `Cursor::tuple` | `n/u/t` 分别成为 Null/UnchangedToast/Text；拒绝 binary，符合首版范围；已有解码与真实库用例 |
| Relation 和 Type | [decoder.mbt](../../decoder.mbt)、[events.mbt](../../events.mbt) | 每个数据事件带当时的不可变 Relation，避免后续元数据覆盖旧事件；重复元数据不占用数据事件 ID 序号 |
| 截断与未知消息 | [wire.mbt](../../wire.mbt)、[decoder_wbtest.mbt](../../decoder_wbtest.mbt) | 长度先检查再读取；拒绝尾随字节和不支持的消息。上游支持更多消息不是放宽 v1 边界的理由 |

新增 [feedback_wbtest.mbt](../../feedback_wbtest.mbt) 用本地 TCP peer 发送两个事务，
并逐个检查真实反馈包中的 write、flush、apply：

1. 收到且交付 A/B，但均未 ack：三个字段仍为起点 `0/10`。
2. 先 ack B，A 留有缺口：仍为 `0/10`。
3. ack A 时制造检查点临时路径冲突：旧文件及反馈仍为 `0/10`。
4. 清除冲突、重试 ack A：文件与三个字段一起推进到 B 的 end LSN `0/C9`。
5. 服务器报告 `0/2000` 并请求立即回包：反馈仍为 `0/C9`，不采用服务器进度。

前四次显式调用订阅的反馈路径，第五次由实际 keepalive 触发。测试有总超时且不依赖
周期性 sleep；周期定时心跳的真实 PostgreSQL 证据仍由既有 live 测试负责。

## 恢复架构取舍

Supabase ETL 的 Destination 区分接收工作与持久完成；StateStore 要求持久写成功后才
更新缓存。MoonCDC 对应的是业务持久化后调用 `ack`，随后依次写文件、文件同步、原子
替换、目录同步，最后公布内存中的 durable 位置。乱序 ack 只能推进连续完成的前缀。
保留 [subscription.mbt](../../subscription.mbt) 与 [checkpoint.mbt](../../checkpoint.mbt)
现有顺序，不为单一文件后端引入通用 Store/Destination 抽象。

ETL 的初始同步以表为单位执行快照复制和增量追赶，初始同步期间不保证跨表完整事务边界。
如果 MoonCDC 以后增加全量初始化，需要先明确快照水位、表间协调、失败重建及交付边界，
不能把这些 worker 直接接到当前 `Transaction` API。该功能本次不实现。

两个实现都需要业务端幂等。MoonCDC 用源身份、commit LSN 与数据事件序号组成稳定 ID，
并严格校验恢复源；收到完整事务后的进程内重连保留 pending，进程重启则从持久位置重放。
相关证据见 [reconnect_wbtest.mbt](../../reconnect_wbtest.mbt)、
[checkpoint_wbtest.mbt](../../checkpoint_wbtest.mbt) 和既有
[进程中断报告](VALIDATION_FOLLOWUP.md)。历史报告结果不计入本次执行。

## 异常场景覆盖与缺口

Debezium 文档提供的检查线索包括 offset 与槽进度不一致、不可用 TOAST 值、事务边界，
以及没有受监控事件时的 WAL 保留。下表状态来自 MoonCDC 实现和测试体检查；
“已有 live 用例”不表示本次执行了数据库测试。

| 场景 | 当前证据 | 可观察验收或后续动作 |
|---|---|---|
| 多表事务、回滚、事件顺序 | 已有 `integration_wbtest.mbt` live 用例 | 一次交付包含完整已提交事务；回滚不交付 |
| 乱序 ack、检查点写失败、立即心跳 | 本次新增 TCP 用例通过 | 五阶段反馈位置与上文完全一致 |
| 断线及重连各阶段失败 | 已有 `reconnect_wbtest.mbt`，本次普通套件执行 | 重试有界，源改变立即失败，耗尽返回最后错误 |
| 缺槽、错误 publication、检查点损坏 | 已有 live 和普通用例 | 明确失败；不自动创建槽或跳到最新位置 |
| 槽 confirmed/restart LSN 超前或已失效 | 后续 `resume_wbtest.mbt` 覆盖客户端分支；[真实槽验证](SLOT_SAFETY_VALIDATION.md) 覆盖 PG17/18 外部推进与 WAL 失效 | 客户端 Resume/重连共 8 个场景；真实库恢复拒绝且 checkpoint 字节不变 |
| NULL、TOAST、主键变化、Relation 更新、truncate | 已有 `decoder_wbtest.mbt` 和 `advanced_wbtest.mbt` | 普通解码本次执行；真实 PG17/18 场景保留在 live 套件 |
| 帧、事务和未确认队列超限 | 已有单元与 live 用例 | 明确失败且 checkpoint 不前进；提高限制后完整重放 |
| 业务写入与检查点之间进程终止 | 已有独立 SIGKILL 工具及历史报告 | 本次未重跑；验收需新进程恢复、稳定 ID 和重复边界，而非仅异常注入 |
| 低流量 publication，但其他表持续产生 WAL | [PG17/18 三次采样](SLOT_SAFETY_VALIDATION.md)确认心跳存活但 retained WAL 增长 | 已有短时正确性证据；不外推为持续运行容量或长期磁盘增长速率 |
| 恢复期间同名 publication 改过滤条件或列清单 | 仅绑定名称；[OPERATIONS](../OPERATIONS.md) 已要求保持定义稳定 | 属于运维前提；若要支持在线修改，先设计定义绑定及重新初始化语义 |
| 分区根发布、行过滤、列投影和最小权限角色 | 当前专门兼容性证据不足 | **后续矩阵**：明确配置后在 PG17/18 验证事件表身份、列数量和权限错误 |
| 一致性快照、自动切主、两阶段事务 | 首版明确不支持 | 属于新功能范围，不能用普通套件通过代替支持声明 |

低流量 WAL 释放需要单独设计：MoonCDC 当前不能仅凭 keepalive 中的 server end LSN
推进业务检查点；直接借用上游的空闲推进策略会改变恢复校验前提。本次保留现有保证，
将持续 WAL 增长作为可观察的运维限制。

## 本次验证

工具链：`moon 0.1.20260920 (914d7da 2026-09-20)`，native 后端。
测试变更基于上面的 MoonCDC 基线；仅增加测试、扩展测试 fixture 参数及文档。

| 命令 | 结果 |
|---|---|
| 修改前 `moon test --target native` | 21/21 runner entries 通过 |
| `moon test --target native --filter 'feedback*'` | 新增用例 1/1 通过 |
| `moon check --target native --deny-warn` | 通过 |
| 清除 live/worker 环境开关后 `moon test --target native` | 22/22 runner entries 通过；16 个执行体活跃，5 个 live 体和 1 个进程 worker 未启用 |
| `moon info --target native`、`moon fmt`、`moon fmt --check` | 通过；生成的 `.mbti` 无变化 |
| `git diff --check` | 通过 |

本次没有启动 PostgreSQL、运行上游项目、执行性能测试或重跑远端 CI。
已有 PG17/18 与 SIGKILL 证据的版本和边界以 [VALIDATION_FOLLOWUP](VALIDATION_FOLLOWUP.md)
为准，不能视作本次新增用例的数据库兼容性证明。

## 恢复拒绝测试跟进

2026-10-02，在已提交的参考核查 `b753b5d5c1e1b1f2fb6e9d85f55b30fd9eb50298`
基础上，新增 [resume_wbtest.mbt](../../resume_wbtest.mbt)。这次继续推进只修改测试和文档，
生产逻辑及公共接口不变。

| peer 返回的异常 | 首次 Resume | 保留未确认事务后重连 |
|---|---|---|
| restart LSN `0/20` 高于 durable `0/10` | 拒绝，通过 | 拒绝，通过 |
| confirmed LSN `0/20` 高于 durable `0/10` | 拒绝，通过 | 拒绝，通过 |
| `wal_status=lost` | 拒绝，通过 | 拒绝，通过 |
| 非空 `invalidation_reason=wal_removed` | 拒绝，通过 | 拒绝，通过 |

测试逐一隔离四个拒绝条件，合成 catalog 行不代表 PostgreSQL 一定会产生该字段组合。
重连前已交付 end LSN 为 `0/65` 的事务，但没有 ack；即使槽位置 `0/20` 低于 received，
仍必须相对于 durable `0/10` 拒绝恢复。每个场景同时验证：

- 返回预期的 `UnsafeResume` 原因，未进入成功消费路径。
- peer 收到连接关闭，而非后续查询、START_REPLICATION 或反馈报文。
- 重连计数为 1，即使允许 3 次重试也立即终止；未确认事务仍未被 ack。
- 检查点大小和全部字节保持不变，解析所得 LSN 不变，退出后可重新取得独占锁。

`moon test --target native --filter '*slots*'`：2/2 测试通过，覆盖上述 8 个场景。
清除 live/worker 环境开关后，完整普通套件 24/24 runner entries 通过：18 个执行体
活跃，5 个 live 体和 1 个进程 worker 未启用。`moon check --target native --deny-warn`、
`moon info --target native`、`moon fmt`、`moon fmt --check`、`git diff --check` 均通过，
生成的 `.mbti` 无变化。

该轮关闭了客户端协议测试缺口，没有操作数据库、运行进程故障测试或执行远端 CI。
随后完成的隔离 PostgreSQL 槽推进、WAL 失效及低流量 WAL 保留实测，单独记录于
[真实槽验证报告](SLOT_SAFETY_VALIDATION.md)，不改变上面各轮历史结果的范围。
