<a id="changelog"></a>

# 变更记录

<!-- language-switch -->
[English](CHANGELOG.md) | [简体中文](CHANGELOG.zh-CN.md)

英文版本为权威版本。


<a id="010"></a>

## 0.1.0

本节记录为 0.1.0 准备的变更，不提供发布日期，也不作为注册库发布回执。
请用 `moon view pangbit/mooncdc@0.1.0` 查询精确版本；流程见[发布说明](docs/RELEASING.zh-CN.md)。

<a id="validation-note"></a>

### 验证说明

合并源码的一轮 macOS release 测试中，copy-exit cleanup 和 reconnect retry 两个场景报
`TimeoutError`。源码、工具链、命令和超时设置保持不变，仅重试该 job 一次后通过。
原因尚未确认，不能据此声称产品缺陷已被确认或修复。
见[版本绑定记录](docs/VALIDATION.zh-CN.md#github-ci-at-the-reviewed-source)。

<a id="changes"></a>

### 变更内容

- 通过独占 StateStore namespace 持久保存 SchemaStore 历史，支持不依赖本地 schema 文件的
  PostgreSQL 恢复，并拒绝过期 owner 写入。
- 按 OID 选择或跳过初始表行，同时保留 schema 设置、空目标持久屏障、WAL 交接和完成表重启状态。
- 增加可选、受作用域约束的 HTTP 存活/就绪探针，包含双栈绑定、精确 IPv4 回退，
  以及宿主成功、错误和取消退出时的监听清理。
- 通过 ApplyControl 观察流水线生命周期及单调 copy/WAL 活动，提供只读健康判断，
  覆盖空闲 keepalive 和目标 write/flush 阻塞，不把活动误当作持久性。
- 在源调用者取消后，仍排空已接纳的 ClickHouse 事务写入，再重放、重置表或关闭；
  保留每次请求的截止时间，并允许取消尚未获准的等待。
- 持久保存完整的逐表交接解码掩码，源重试后恢复连接本地 schema 血缘。
  通过 Transaction.replicated_schemas 暴露逐事件 schema，ClickHouse 在行写入前消费它们。
- 初始源设置、流恢复和目标失败共用一个流水线重试预算，同时保留致命状态和回调边界。
- 主 apply 从持久 checkpoint 有界重试瞬时目标写入/flush，健康表 worker 继续复制。
- 在持久流水线 checkpoint 后按重放位置、表归属和目标恢复元数据清理旧 schema 版本。
  保留待执行 ClickHouse DDL 的两端版本；自定义目标需显式启用。
- 增加 ClickHouseDestination，支持 RowBinary 快照/CDC、主键 tombstone、FULL TOAST 重建、
  持久 schema 绑定和重启布局校验。验证 PostgreSQL 17/18、ClickHouse 26.4 的两种引擎，
  以及 3,800 个上游默认表达式向量。按 Creating/Applying/Applied 持久状态执行支持的列 DDL，
  拒绝模糊的中间状态和 schema 回退，并在源重命名后保留历史默认值与物理表名。
  分别配置连接、schema、DDL 和插入截止时间及有界服务端设置，覆盖八次真实 SIGKILL 新进程恢复。
- 将可插拔 StateStore 归属与持久记录接入流水线和订阅，支持文件/PostgreSQL 后端及目标元数据会话。
  保留源绑定 checkpoint，拒绝已关闭或过期 owner 的写入。
- 持久保存表复制错误，支持有界定时、手动和禁用重试；健康表继续 apply，失败表等待新快照/重置。
  ApplyControl 支持实时手动重试，同时保留全局持久性失败。
- 保留任意精度 JSON 数字及相等语义，包括下溢、十进制 scale 和指数；识别通用内置 PostgreSQL 数组。
  将官方 ETL codec 差分扩展到 558 个向量。
- 增加 schema 投影/identity 掩码、类型化行解码、规范 schema JSON 往返和带重命名环处理的列 DDL 计划。
  对照固定官方 ETL planner 的 77 个向量核验计划。
- 有界表 worker 复制并重放私有 WAL 时，已就绪表继续 apply；仅在持久后保存交接点。
  即使主 checkpoint 已前进，也能恢复未完成复制；split truncate ID 按表隔离。
- 增加与固定官方 ETL codec 的可复现 320 向量差分，覆盖数值规范化、float4 舍入/溢出、
  OID 符号和闰秒。
- 增加类型化 PostgreSQL 文本 Cell，覆盖精确数值、时间无穷/BC/24h、可空数组和稳定会话输出格式。
- 增加 Supabase ETL DDL 解析、有序 schema 版本、publication 级处理、原子 schema 存储和重放/保留边界检查。
- 增加持久逐表流水线，支持并发独立快照、失败复制重启、表交接点、动态 WAL 发现和可选有界背压。
- 在快照 replica-identity key 标记中排除索引 INCLUDE 列。
- 增加自定义 Destination 契约，含 Accepted/Durable 写入、快照 attempt ID、累计/空闲/停止 flush 和 Resume。
- 增加可选逻辑消息，保留原始字节，事务内随提交交付，非事务消息独立回调，重连时保留协商。
- 增加有界且感知 publication 的一致性快照和源绑定 `AfterSnapshot` 交接，失败 bootstrap 保留槽以便显式恢复。
- 增加可选 pgoutput 二进制传输和原始 Binary 值，重连时保留协商。
- 对重连的启动、身份识别和 COPY BOTH 阶段瞬时失败重试，关闭失败连接，并在预算耗尽时返回最后一次错误。
- 增加协议故障回归，以及内部五个 checkpoint 边界的真实 SIGKILL 测试。
- 对观察到的 backend 退出码缺陷增加严格注册库 dry-run 兼容检查。
- native PostgreSQL pgoutput v1 订阅，支持 SCRAM-SHA-256 和验证 TLS。
- 不可变已提交事务、表/类型元数据，以及重放稳定的数据事件 ID。
- 连续显式确认和原子源绑定 checkpoint。
- 有界帧、事务及未完成交付；心跳和有界重连。
- 索引、缓存和持久事件日志示例；PostgreSQL 18/17 集成测试。
