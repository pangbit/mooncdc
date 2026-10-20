<a id="executable-examples"></a>

# 可执行示例

<!-- language-switch -->
[English](README.md) | [简体中文](README.zh-CN.md)

英文版本为权威版本。


这些程序仅操作仓库的隔离 Docker 夹具，会创建和移除自己的 `example_*` 表、publication
和槽。先启动服务：

```sh
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait
moon run examples/index
moon run examples/cache
moon run tools/recovery-test.mbtx
```

设置 `MOONCDC_EXAMPLE_SERVICE=pg17` 可在 PostgreSQL 17 上运行相同示例；默认 `pg18`。
从仓库根目录执行。Docker 只用于设置 SQL；复制流量使用公共 MoonCDC 库的 native TCP 客户端。

<a id="index"></a>

## 索引

[`index/main.mbt`](index/main.mbt) 先建立订阅，再提交 insert/update/delete 事务和回滚事务。
它将完整已提交事务应用到 map，按事务 ID 去重，故意应用同一事务两次，并与 SQL 查询比较
最终 map。回滚行绝不能进入索引。

内存索引及去重集合仅存在于进程内。示例始终从新的空表和槽开始；map 丢失后不要复用其
checkpoint。持久应用必须先原子保存索引变化和事务 ID，再确认。

<a id="cache-invalidation"></a>

## 缓存失效

[`cache/main.mbt`](cache/main.mbt) 预置两行数据库数据及三个缓存 key。一次已提交 update/delete
只使对应的两个 key 失效，无关 key 保留。夹具使用 `REPLICA IDENTITY FULL`。
若旧 identity 不可用，示例会解释原因并失败，不会假装能安全使旧 key 失效。

<a id="durable-event-file-and-restart"></a>

## 持久事件文件与重启

[`resume/main.mbt`](resume/main.mbt) 将完整事务和唯一业务 ID 写入 `events.json`，先写临时文件、
fsync、rename，再同步父目录后确认。重放事务 ID 被去重。这个刻意简化的示例会重写日志；
生产目标应采用事务存储，或有界且可崩溃恢复的追加日志。

`tools/recovery-test.mbtx` 构建 native 可执行文件，在每个边界启动独立消费者进程，等待持久
ready 标记，发送 SIGKILL，再启动新的恢复进程。它检查退出码、最终精确业务 ID 和预期重复：

| 终止边界 | 事件文件写入器预期的重放重复数 |
|---|---:|
| 业务持久化前 | 0 |
| 业务持久化后、ack 前 | 1（去重） |
| 持久 ack 后、feedback 前 | 0 |
| 服务端 feedback 已验证后 | 0 |

脚本清理自己的示例对象和临时文件，绝不删除其他槽。公共 API 不把业务文件持久化与源反馈
合成恰好一次事务；第二行重复正是至少一次契约的一部分。
