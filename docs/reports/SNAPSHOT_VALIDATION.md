# 一致性快照基础层验证

日期：2026-10-02。范围：`copy_snapshot` 与 `AfterSnapshot`，不是完整 ETL 功能对等报告。

在隔离 Compose 的 PostgreSQL 17.11 / 18.6 上分别执行：

```sh
MOONCDC_TEST_PORT=55418 moon test --target native --filter 'live snapshot*' -v
MOONCDC_TEST_PORT=55417 moon test --target native --filter 'live snapshot*' -v
```

每个版本 3 个真实数据库场景：

1. 导出槽快照后，在另一连接提交插入、更新、删除并回滚另一次插入。
   快照保持原数据；从绑定位置订阅并应用增量后，目标 Map 与最终 SQL 查询完全相同。
   同时验证 publication 列投影、行过滤、NULL、空表、含引号和中文的标识符、单行批次、
   复制元数据与 pgoutput Relation 一致；错源位置被拒绝且未写 checkpoint。
2. 业务回调抛错时不返回位置；重复使用槽明确报错且不推进槽。
   帧超限、批次累积字节超限明确失败；取消正在等待的回调后能释放连接并删除测试槽。
3. 分区按叶表和按根表发布，两种配置的快照 Relation 均与后续 pgoutput 一致。
   普通继承子表由 publication 单独列出，父表使用 ONLY，初始复制不重复包含子表行。

普通套件另覆盖 SQL 标识符/字面量转义及 NUL 拒绝、公开 API 参数验证。
本次运行的真实测试均通过；CI 的 `live*` 选择器自动包含这些场景。

边界：复制期间不支持 schema/publication 变更；没有按表持久状态与自动重建；
没有两端进程差分或长期资源测试。失败保留槽，需要明确恢复操作，不能当作完成快照。

实现依据：[导出快照与 consistent_point](https://www.postgresql.org/docs/18/protocol-replication.html)、
[快照导入](https://www.postgresql.org/docs/18/sql-set-transaction.html)、
[publication 表/列/过滤元数据](https://www.postgresql.org/docs/18/view-pg-publication-tables.html)。
