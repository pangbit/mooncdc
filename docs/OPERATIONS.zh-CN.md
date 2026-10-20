<a id="operating-a-subscription"></a>

# 订阅运维

<!-- language-switch -->
[English](OPERATIONS.md) | [简体中文](OPERATIONS.zh-CN.md)

英文版本为权威版本。


PostgreSQL 需要 `wal_level=logical`、足够的 replication slots/walsenders、publication
和持久 pgoutput 逻辑槽。使用具有 LOGIN、REPLICATION 及所需数据库/表权限的专用角色。
创建 publication 属于管理员设置操作，MoonCDC 不会静默创建或修改 publication。

```sql
CREATE PUBLICATION app_changes FOR TABLE public.items;
SELECT slot_name, lsn
FROM pg_create_logical_replication_slot('app_slot', 'pgoutput');
```

发送变化前保存返回的 LSN。使用新 checkpoint 路径从该位置 `At` 开始，或使用已有
checkpoint `Resume`。此入口没有初始快照；已有行需要独立初始化方案。
恢复时保持 publication 定义稳定：仅名称不能发现过滤条件或列清单变化。
这些变化需要明确协调重建或新槽。

若需要非 key 的旧值，设置 `REPLICA IDENTITY FULL`。默认 identity 下 update/delete
可能只带身份 key，或没有旧 tuple。缺乏适当 identity 的表可能被 PostgreSQL 拒绝
发布 update/delete。

除客户端诊断外，也应监测 PostgreSQL：

```sql
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status,
       invalidation_reason,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
FROM pg_replication_slots WHERE slot_name = 'app_slot';
```

对持续 WAL 增长、磁盘压力、不活跃消费者和失效告警。明确选择 `max_slot_wal_keep_size`
和存储报警：有限上限可能使落后槽失效，无限槽可能占满磁盘。
客户端心跳应小于 `wal_sender_timeout`，避免长时间 CPU 回调阻塞 MoonBit event loop。

心跳 feedback 只报告业务已持久位置。如果 publication 表无变化、无关表产生 WAL，
健康连接仍可能保留越来越多 WAL；server-end 不推进 checkpoint。
即使重连和未完成事务为零，也要监测保留 WAL。此行为有隔离 PG17/18
[验证记录](VALIDATION.zh-CN.md)。不要在外部推进槽来隐藏增长；槽超过 checkpoint 会拒绝 resume。

达到帧/事务/队列上限时，业务 ack 不前进。应诊断超大负载或慢目标，调整对应上限后恢复。
缺失、失效或被外部推进的槽需要运维介入，通常需要新的初始化/快照计划。
不要删除 checkpoint 掩盖错误，也不要自动从最新 WAL 继续。

退役消费者时，先停止、确认槽不活跃，再明确删除：

```sql
SELECT pg_drop_replication_slot('app_slot');
```

删槽会删除恢复历史，因此库取消/close 有意不执行此操作。
备份恢复应同时保留 checkpoint 和业务去重状态；如果源 WAL 已释放，恢复旧 checkpoint 必须失败。
