# MoonCDC 功能对齐计划

启动日期：2026-10-02。执行基线：`5089e3a`。
目标从原首期 CDC 子集扩展为参照实现的功能对齐；旧需求的排除项是历史阶段边界，
不能据此把本计划中的功能判作完成或永久排除。

## 对齐标准

用户已选择 Supabase ETL 为主参照，pglogrepl 和 Debezium 补充协议与异常语义。
快照协调、持久状态、目标端及运行框架以固定 Supabase ETL 提交的实际能力为基准。
对齐指可观察功能与故障语义达到逐项验收，不要求跨语言 API 名称相同。
不以“设计相近”“单元测试通过”替代真实库验证，也不把上游尚未实测的功能声明为对等。

固定阅读基线：

- [pglogrepl](https://github.com/jackc/pglogrepl/tree/4ae5c490f7ce97fb0b918eed3023b35c16990f5a)
- [Supabase ETL](https://github.com/supabase/etl/tree/c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4)
- [Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
  的具体发布基线在选为主参照后冻结。

## 阶段及验收

| 阶段 | 工作 | 可观察验收 | 状态 |
|---|---|---|---|
| P0 | 固化已有协议、恢复和槽安全证据 | native 普通套件、PG17/18 槽推进/失效/低流量 WAL 实测；提交可追溯 | 已提交 `5089e3a` |
| P1 | 字段 binary 传输、逻辑消息；v2 单列协议补充 | 原始字节不损坏；NULL/TOAST 保留；重连维持协商；逻辑消息提交/回滚及异常边界；PG17/18 实测与参照差分 | binary 与逻辑消息本地验证；差分待执行 |
| P2 | 一致性快照与增量衔接 | 导出快照、并发写入期间全量复制与增量追赶后最终状态相同；中断重建不静默遗漏；批次内存有界 | 逐表持久恢复、并行复制及 WAL 追赶本地验证；完整 worker handover 待执行 |
| P3 | 持久状态与目标端契约 | 区分接收和持久完成；失败不推进 checkpoint；启动恢复、优雅退出、旧任务隔离和幂等批次可测 | 目标端契约、按表文件状态本地验证；schema store 和外部 state store 待执行 |
| P4 | 类型与 schema 演进 | 类型矩阵、Relation 版本、列增删改、publication 过滤/投影/分区变更逐项对照；未知类型明确处理 | 待执行 |
| P5 | 目标端及运行方式 | 自定义目标端、独立运行程序及上游内置目标端逐项映射；本地可测逐一验证，需要外部服务的能力单列验证条件 | 待执行，内置目标端先核查稳定性 |
| P6 | 故障及一致性验收 | 两端同输入输出归一化比对；进程中断、重试、空闲 WAL、资源上限和长期运行证据；文档/示例/独立消费者与接口同步 | 随每阶段推进，最终汇总 |

Supabase ETL 的按表初始同步与现有完整事务交付需要分开的接口契约。
pglogrepl 用于协议字段和消息对照；Debezium 用于异常场景补充，Kafka Connect 不是本次主参照范围。
自动切主、两阶段事务等须对照固定 ETL 版本的实际能力记录，不能推测。

## 固定基线的具体差距

主参照 `crates/etl/src/postgres/client/raw.rs` 的 `start_logical_replication` 使用
`proto_version=1, messages=true`。协议 v2 streaming 属 pglogrepl 补充项，不能用它替代
主参照的按表复制、持久状态和目标端功能验收；仍保留为后续独立协议工作。

| ETL 能力 | MoonCDC 状态 / 下一项 |
|---|---|
| 初始复制与 WAL 追赶 | 独立逐表快照、持久 cutoff、并行复制、重启只重建失败表已实测；完整 SyncDone worker handover 待实现 |
| StateStore / SchemaStore | 源绑定 checkpoint、逐表文件状态、公开 SchemaStore、schema 原子文件版本及显式清理已实现；外部 StateStore、自动清理协调、目标端创建状态待实现 |
| Destination accepted/durable | 单实例有序写入、累计屏障、空闲刷新、正常停止和批次 ID 已实测；并发表复制、目标端持久元数据及具体后端隔离待实现 |
| 逻辑消息与 DDL | 已解码并存储 `supabase_etl_ddl`、版本顺序/作用域/原始上游触发器已实测；完整 decoding masks、目标端列演进待实现 |
| 类型转换 | 已有类型化文本 Cell、精确 numeric、日期/时间/JSON/UUID/bytea/可空一维数组；未知标量保留文本；PG17/18 快照及 WAL 矩阵已实测，上游 codec 进程差分待执行 |
| 独立 replicator | 当前为嵌入式库及示例；配置、持久运行状态、健康/指标、优雅退出待实现 |
| ClickHouse | 上游 private alpha；本地优先实现 ReplacingMergeTree / MergeTree、主键变化墓碑、truncate 与 schema 契约 |
| BigQuery | 上游 stable；Storage Write API、CDC、目标端持久偏移、布局配置及真云验证待实现 |
| DuckLake | 上游 private alpha；DuckDB/catalog、排序和恢复语义待实现 |
| Snowflake | 上游 private alpha；Snowpipe Streaming、key-pair auth、通道恢复及 schema 子集待实现 |
| Iceberg | 上游 deprecated；保留在差距清单，兼容实现/验证未完成，不声明已对齐 |

目标端状态取自固定提交的 `site/content/docs/reference/destinations.mdx`，不是动态网页。
云目标端真实验收需要对应账号及隔离资源；本地实现和可模拟验证先推进，缺乏真云证据时
保持“未验证”，不把编译通过当作对等。现阶段没有完整 ETL 对等结论。

## 提交与证据规则

每个可验证功能单独本地提交：实现、单元/故障测试、真实 PG17/18 验证、接口和文档一起交付。
规划及状态变更可单独提交。仅 stage 本阶段明确路径；push、发布与部署不包含在本授权中。
状态只用未开始、实现中、本地验证、差分验证、完成；“完成”必须有对应范围与证据，
剩余主参照功能不能因某个阶段通过而消失。

## 执行记录

- `5089e3a`：提交客户端恢复拒绝与真实槽安全测试、CI 配置及报告；生产 API 未变。
- P1 binary：本地验证。显式 `binary=true`、原始 Binary 字段、重连保留协商；PG17.11/18.6
  验证 int4/bytea/bool 精确字节、默认文本及 ack，畸形长度与非 UTF-8 字节有单元测试。
  添加公开 enum 分支会影响调用方的穷举匹配；不代表已实现原生类型转换。
- `ea502b3`：提交上述 binary 实现和验证。
- `b09f3f2`：提交 P2 一致性快照基础层。
- P2 基础快照：实现 `copy_snapshot`、publication 投影/过滤、有界 FETCH、空表通知及
  源身份绑定的 `AfterSnapshot`。并发 I/U/D + 回滚、分区 root/leaf、普通继承去重、
  回调失败、行/批次超限、取消释放与拒绝复用槽有 PG17/18 实测。
  尚未实现 ETL 的独立按表状态机、持久同步状态、并行复制或失败自动重建。
- P1 逻辑消息：PG17/18 验证事务提交/回滚、非事务消息及原始字节；单元验证畸形字段、
  稳定消息 ID、重连协商，故障测试验证回调错误不推进 checkpoint、不重连。
- `558f4b3`：提交逻辑消息支持及固定 ETL 能力矩阵。
- P3 基础契约：`Destination` 的 Accepted/Durable、累计 flush、空闲刷新和停止排空，
  快照轮次/批次 ID、reset 隔离契约及 `SnapshotPosition.save_checkpoint`。
  故障注入覆盖 flush 失败/取消不确认，PG17/18 覆盖快照持久屏障、重复复制重置、
  保存 checkpoint 后 Resume 到目标端。自定义 trait 有外部调用方编译测试。
- `7b1f4cf`：提交目标端基础契约与快照衔接 checkpoint。
- `a883181`：修复 snapshot 对主键 INCLUDE 列的身份键误判；PG18 先复现，PG17/18 验证修复。
- 首批累计验证：native 套件 43/43；PG17.11、18.6 的 `live*` 各 11/11；
  release 构建、文档与打包后独立消费者通过。见 [阶段报告](reports/FEATURE_PARITY_BATCH1.md)。
  下一项是 P2/P3 的逐表持久同步状态及失败重建，之后推进 schema/type 和目标端。
- P2/P3 逐表阶段：`run_pipeline` 保存建槽意图及每表 Pending/Copying/Catchup/Ready，
  保留完成表的 cutoff，重启重新 reset 未完成表；临时 copy 槽自动释放，主槽保留恢复。
  全量期间并发事务、跨表 cutoff、运行中新表、目标身份拒绝及三表独立快照重叠已在
  PG17.11/18.6 验证。投影后 row/message ID 保持原序号；有界队列背压通过 wire 测试。
  本轮完整 live 场景逐进程隔离运行，PG17/18 各 13/13；常规 native 50/50。
  完整回归暴露并修复目录 regclass 名称解析被其他 publication 并发 DROP 干扰的问题。
  仍需完整 worker handover、空表动态目录发现、schema/type、目标端及上游差分。
- P3/P4 schema：公开版本化 SchemaStore 与原子文件实现，版本按 commit/message LSN 排序，
  保留边界基准版本及未来版本；失败写盘不发布缓存，同版本重放冲突拒绝。
  逐表快照在导出视图内保存全列 schema，pipeline 在目标事务写入前持久化适用 DDL。
  使用固定上游的两份未修改 SQL 触发器验证真实 payload，覆盖同事务多次 DDL、
  回滚、publication 作用域、默认值/nullable/复合主键顺序及 INCLUDE 排除。
  目标端仍需按消息顺序实现自己的列演进；这不是完整 schema 规划/worker handover 对等。
  native 56/56；schema 集成完整 live 套件 PG17/18 各 14/14。随后增加的调试字段裁剪与
  缺失 schema 恢复拒绝，重新通过普通套件和 PG17/18 的上游 DDL 专项；release/doc 通过。
- P6 逐表进程恢复：PG17/18 均在 B 表首批业务数据持久化、状态仍为 Copying 时真实
  SIGKILL worker。新进程验证 A 表不重置、B 表重置一次、主 checkpoint 未提前推进，
  追赶崩溃前后跨表 WAL 后目标内容与 SQL 查询完全一致。工具已纳入 CI；不声称掉电验证。
- P4 类型：`Value.decode_text` 输出类型化 Cell，覆盖有符号宽度/OID、float、精确 numeric
  文本、bytea、UUID、JSON、BC/扩展年份、infinity、24:00:00、UTC 时区归一化及一维可空数组。
  PG17/18 已对照初始快照与 WAL 的相同字段值；连接固定输出 GUC，写入会话的其他设置
  不改变消费者值。未知标量保留文本，自定义数组可提供 element OID。二进制仍为独立原始 API。
  Rust 上游 codec 的逐向量进程差分和目标端类型映射尚未完成，不扩大本阶段结论。
  普通 native 62/62，PG17/18 完整 live 各 15/15；独立源码包消费者通过。
