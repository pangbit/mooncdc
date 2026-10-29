# MoonCDC：2026 年 10 月项目与验收说明

记录日期：2026-10-29。MoonCDC 主要用 MoonBit 实现 PostgreSQL 变更捕获与 ClickHouse
同步，提供共用 `run_pipeline` 核心的库和 replicator CLI。初始一致性快照与后续 WAL
衔接，持久保存逐表进度、schema 和检查点，支持当前值视图与操作历史、JSON/YAML 配置、
健康检查及资源背压。首批源端为 PostgreSQL 17/18，后端为 native；具体边界见
[交付范围](../RELEASE_SCOPE.zh-CN.md)。项目采用 Apache-2.0，参考设计、引入文件及许可证见
[第三方来源](../../THIRD_PARTY.md)。

采用 **at-least-once（至少一次）投递**，业务端需处理重放；不承诺跨系统恰好一次、
源事务在目标端的原子可见性、双向同步或自动选主。

## 本期交付与证据

本月于 10 月 21 日发布 0.1.0，并于 10 月 29 日发布 0.1.1，完成三项正确性修复：

| 已完成修复 | 定向验收结果 |
| --- | --- |
| ClickHouse HTTP 2xx 携带异常头时判定失败，保留重试分类与错误脱敏 | 3 项测试、44 个场景通过 |
| 已接纳 COPY 在调用者取消后维持写入顺序，表重置/关闭等待完成或截止时间；等待接纳仍可取消 | 2 项测试、12 个场景通过，覆盖两种引擎配置 |
| 保留检查点边界的相邻事务，仍过滤旧事务与重复事务 | 1 项测试、At/Resume × Accepted/Durable 四组合通过 |

- [Release 与 tag v0.1.1](https://github.com/pangbit/mooncdc/releases/tag/v0.1.1)：发布源码为 `0a38ed675a3e044f81bfd0c14a3f2e89a0d24cab`。
- [精确 Mooncakes 版本](https://mooncakes.io/api/v0/manifest/pangbit/mooncdc@0.1.1)：`moon add pangbit/mooncdc@0.1.1`；注册库归档与审阅包的 SHA-256 相同。
- [该源码 CI](https://github.com/pangbit/mooncdc/actions/runs/40812266552)：提交主题、参考检查、Linux/macOS native、PG17、PG18 六项 job 全部通过，包含 native debug/release 与归档消费者。
- 仓库外精确版本消费者已在 macOS ARM64 通过严格 native 检查及 release 构建/运行。三项修复的源码绑定、早期 264＋1 普通门禁及发布回执见[验证记录](../VALIDATION.zh-CN.md#011-patch-candidate)。

这些结果覆盖记录的已启用检查，不代表全部故障组合或部署环境。

## 可复现演示

在独立的 `v0.1.1` checkout 中，按[工具链与隔离环境说明](../TESTING.zh-CN.md)准备
MoonBit、Docker 和镜像；Linux 编译栈要求同该说明。仅使用自有隔离夹具，在仓库根目录运行：

```sh
moon update
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait pg18
docker compose -p mooncdc-clickhouse-test -f integration/clickhouse.compose.yaml up -d --wait clickhouse
moon run examples/index
MOONCDC_EXAMPLE_SERVICE=pg18 moon run tools/replicator-test.mbtx
```

[库示例](https://github.com/pangbit/mooncdc/blob/v0.1.1/examples/README.zh-CN.md)演示索引、缓存失效与持久恢复；
[CLI 脚本](https://github.com/pangbit/mooncdc/blob/v0.1.1/tools/replicator-test.mbtx)使用合成数据，核对同步、重放和最终结果。
失败时也应按测试说明清理自有夹具。本页与显式 ClickHouse 启动命令属于发布后的文档补充，
没有修改 `v0.1.1` tag 或已发布包。本页是公开交付证据，不代替正式报名或验收提交回执。
