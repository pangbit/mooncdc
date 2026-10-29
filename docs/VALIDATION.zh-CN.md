<a id="validation-scope-and-version-binding"></a>

# 验证范围与版本绑定

<!-- language-switch -->
[English](VALIDATION.md) | [简体中文](VALIDATION.zh-CN.md)

英文版本为权威版本。


本页记录绑定源码的验证观察，发布事实须与注册库回执相互核验。
先列出 0.1.1 发布记录，下文保留 0.1.0 的历史组装与 CI 记录。

<a id="011-patch-candidate"></a>

## 0.1.1 发布（2026-10-29）

产品源码为 `568fe7c387f04f1cc3f750749a2652cff8b31e0a`，公共接口与依赖版本不变。
Linux 定向检查使用隔离回环模拟端、固定 MoonBit 工具链、64 MiB 编译栈及 8 MiB 运行栈：

| 修复 / 源码 | 实际执行结果 |
| --- | --- |
| ClickHouse 异常头，`fd8c7b7` | 3 项测试 / 44 个场景通过；HTTP 状态及异常头失败保留重试分类与错误脱敏 |
| 已接纳 COPY 的取消顺序，`ce42912` | 2 项测试 / 12 个场景通过，覆盖两种引擎配置、重置/重放、关闭、请求截止时间及等待接纳时取消 |
| 相邻事务边界，`568fe7c` | 1 项测试 / 4 个组合通过：At/Resume × Accepted/Durable，保留相邻事务并过滤旧/重复事务 |

在 `568fe7c` 上，四包普通 debug 回归通过 265 个身份中的 264 项
（root 211、CLI 28、memory 18、YAML 8）。剩余迁移测试因定制验证包漏带两份已跟踪
SQL 夹具而无法读取文件。补齐完全相同的夹具后，仅补测该项，1/1 通过，未改产品代码
或断言。普通门禁由 264 + 1 的结果闭合，不能表述为一次全绿完整运行。
框架总数包含未启用的环境门控 live/reference 测试体，不是新一轮真实 PostgreSQL
或 ClickHouse 验收结果。远端本轮夹具已移除并核验清理。

发布源码 `0a38ed675a3e044f81bfd0c14a3f2e89a0d24cab` 在该产品源码上增加元数据、
文档及包脚本从 manifest 读取版本的处理。该提交的
[CI](https://github.com/pangbit/mooncdc/actions/runs/40812266552)六项全部成功：
提交主题、codec/reference、Linux native、macOS native、PG17 与 PG18。
Native job 包括 debug/release 执行及归档消费者；现有已启用的数据库 job 通过，
不能据此声称覆盖全部故障组合或部署环境。
[`7233238` 的 CI](https://github.com/pangbit/mooncdc/actions/runs/39536560268)保留为补丁前基线，
没有额外手工运行数据库矩阵。

`moon publish` 退出 0，返回 HTTP 200。精确 0.1.1 注册库查询成功，包未撤回，
创建时间为 `2026-10-29T03:49:24.759462+00:00`，归档 SHA-256 为
`d592a730ecf3027783c6d764670b789df11f092992afb52a85fee664434d6b33`，与已审阅包完全相同。
仓库外的新注册库消费者解析到 0.1.1，在 macOS ARM64 通过严格 native 检查及 release
构建/运行，未使用 workspace、本地路径或依赖覆盖，也未连接数据库。
三份修复源码及公共接口与发布源码相同。
[Tag 与 Release `v0.1.1`](https://github.com/pangbit/mooncdc/releases/tag/v0.1.1)指向 `0a38ed6`。
后续发布记录文档不改变或重新测试该源码。

<a id="inputs-and-reuse"></a>

## 输入与复用

- 公开基线：`5a810d6c62e4ff4783c063159008e23760befc6a`。
- 已接受本地产品源码：`b96a741c39119ebd6336b35cdc57f4f4f7c44977`。
  该标识只表示来源，其本地历史未合入本分支。
- 已审阅源码归档 SHA-256：
  `d64d82474a2af6d745cfe399d5fb2e21fa325353249c6494eb96b28605ade529`，
  666,047 字节，290 条目。新树修改文档和测试工具，最终归档需要单独校验值并重新检查。
- 逐字节比较生产源码、native stubs、包元数据和生成接口。
  输入一致才可复用相应产品证据；工具和文档变化必须单独检查。
- 早期离线消费者在 `1ef841a5a4e3960d552338f0a54e6d7d04d7d70c` 使用归档 SHA-256
  `f05c80d99bcbba76b4d1b4e9b05bbde5cd45b50167b1a9d3397e5ed8a5651e41`。
  它使用提取的本地模块，不是普通 Mooncakes 注册库依赖。

<a id="historical-linux-results-not-a-fresh-run-of-this-tree"></a>

## 历史 Linux 结果，不是本树的新运行

2026-10-20 的单次诊断在 03:11:51–03:15:44 UTC 执行。
绑定产品通过四个库单元格（PG17/18 × 两种 ClickHouse 引擎）和八个 CLI 单元格
（PG17/18 × JSON/YAML × 两种引擎），八个最终数据比较均相符。
48 个 CLI 子阶段中 40 个退出 0，八个为有意注入的退出 1；controller、supervisor 和
harness 命令退出 0。服务端是 PostgreSQL 17.11/18.6 与 ClickHouse 26.4.5.143。
该诊断未重跑普通 260-test 或公共 API 套件。

资源观察必须分开：

| 测量 | 含义 |
| --- | --- |
| 256 tasks | 受控编译窗口预算及运行后恢复的父限制 |
| 264 tasks | 数据库诊断中观察到的父域内核总峰值 |
| 512 tasks | 仅为此次诊断批准的临时上限；停止水位 384 |
| 64 MiB / 8 MiB stack | 已接受的 Linux 编译/产品运行条件 |

CPU 保持 4，内存 6 GiB，swap 0。清理读回未发现自有夹具容器、网络、volume 或任务，
父任务限制恢复为 256。这些既不是产品最低需求，也不授权改变宿主策略。
在内核 264 峰值时未采样其精确组成。

先前中断、编译器限制和 harness 失败不能算产品通过。
此前包括 YAML/JSON wrapper 不匹配、supervisor 清理竞态、总任务预算不足和旧 64 MiB ELF
归档上限。该次诊断在执行数据库前解决了自己的准备文件名不符。
更广故障组合或其他环境仍需独立结果。

<a id="public-report-consolidation"></a>

## 公开报告整合

旧报告保留在公开基线历史中；本分支移除当前树副本，不删除历史。

| 原 `docs/reports/` 文件 | 保留价值 / 当前去向 |
| --- | --- |
| `DESTINATION_VALIDATION.md` | 持久性和模拟测试限制 → [API](API.zh-CN.md#destinations-and-durability)、[测试](TESTING.zh-CN.md) |
| `SLOT_SAFETY_VALIDATION.md` | 槽恢复保障 → [API](API.zh-CN.md)、[测试](TESTING.zh-CN.md) |
| `SNAPSHOT_VALIDATION.md` | 快照/WAL 交接契约 → [API](API.zh-CN.md#persistent-table-pipeline) |
| `REFERENCE_REVIEW.md` | 固定参照与独立实现 → [参照测试](REFERENCE_TESTS.zh-CN.md)、[许可证](../THIRD_PARTY.zh-CN.md) |
| `FEATURE_PARITY_BATCH1.md` | 被替代的进度表 → [范围](RELEASE_SCOPE.zh-CN.md)、[功能映射](FEATURE_PARITY.zh-CN.md) |
| `VALIDATION.md` | 2026-10-02、`332083729f00999661d2869c10fca81b3df88642` 的检查；历史数量/环境不认证本版 |
| `VALIDATION_FOLLOWUP.md` | `985f4d0767f9e8fa70c139d75a37be963b890fa1` 的检查；旧 CI/dry-run 不发布本版 |

发布树有意不含原始进程清单、主机路径、环境转储、大日志及历史归档。
本地审阅清单在树外保留校验值和来源。遵循[测试](TESTING.zh-CN.md)生成新的有界本地证据，
仅公开经过审阅和脱敏的摘要。

<a id="initial-local-release-tree-checks-2026-10-20-before-github-ci"></a>

## 初始本地发布树检查（2026-10-20，GitHub CI 前）

初始审阅时，108 个生产/API/元数据条目与已接受候选逐字节相等，208 个
MoonBit/C/interface/package 输入也匹配已接受产品提交。严格 frozen native check 通过。
普通发现仍为 260 个身份（root 206、CLI 28、memory 18、YAML 8）；这是发现，不是
重新执行 260-test。

修改过的 active-promotion 解析器夹具通过 33 个测试；package driver 解析器 7 个，
CLI 配置解析器 3 个，发布响应解析器 1 个，文件系统/分配回归 21 个。
实际命令均退出 0。timeline-promotion 独立脚本在 test 模式编译，但无测试体，
不声称晋升或数据库结果。元数据命令改用可移植 `uname -a`；旧预构建运行模式停用，
其解析器回归改用模拟夹具，不复用旧运行回执。

第一次独立 frozen 尝试因缺少单独 `x` 缓存被拒绝，随后仅复制已有 async/x 缓存到临时
构建目录，没有下载或安装依赖。这是缓存/环境结果，不是产品失败。

初始组装时，新 CI 还未在 GitHub 运行。后续 CI 修正涉及测试工具、夹具和有界服务端
清理观察，生产输入不变。发布 PR 记录精确 source head、新本地检查及真实远端结果。
此次本地整理未启动数据库、远程主机、完整 260-test runtime 或注册库消费者。
最终接口、格式、包检查和完整审阅清单及真实退出码留在跟踪源码之外。

<a id="github-ci-at-the-reviewed-source"></a>

## 已审阅源码的 GitHub CI

已审阅 release head 为 `dc29b03afc0ec15582330c38f92e57b4c6e7e603`，
[PR run](https://github.com/pangbit/mooncdc/actions/runs/39387450645) 和
[push run](https://github.com/pangbit/mooncdc/actions/runs/39387444662) 均成功完成。
普通合并提交 `7eb58ae7235c5c8c2f43cebfb1ee13bfae9dbf3b` 的源码树相同。

[main 首轮](https://github.com/pangbit/mooncdc/actions/runs/39393658718/attempts/1)通过五项 job。
macOS native release 运行步骤约五秒，普通测试通过 258/260；
`slot_failover_wbtest.mbt:73`（copy-exit cleanup）及 `reconnect_wbtest.mbt:200`
（reconnect retry）报 `TimeoutError`。前者每场景外层 3000 ms，连接 100 ms；
后者外层 5000 ms，连接 200 ms。日志未指明具体失败场景或哪个计时器触发，
没有达到 CI 步骤的五分钟上限。

仅失败 job 获得[一次重试](https://github.com/pangbit/mooncdc/actions/runs/39393658718/attempts/2)。
release 运行步骤约两秒，通过 260/260。源码、命令、断言、时限、固定 MoonBit 工具链、
macOS 镜像和 8 MiB 运行栈不变，worker 不同。没有测量调度或主机负载，原因仍未确认。
首轮失败保留；重试既不证明产品缺陷存在，也不代表修复了缺陷。

成功的 main Linux job 与 macOS 重试分别通过 debug 260/260、release 260/260 及完整
65 命令离线包审计，其中两条非零命令是预期负向校验。PostgreSQL 17/18、跨语言参照和
提交标题检查通过；最终六项成功，保留首轮五项成功结果。
已有 Python Path-accessor 复现是唯一文件系统测试 skip。

这些结果只属于上述源码输入。纯文档后续提交可在逐字节核对生产源码、native stubs、API、
元数据、测试和 CI 输入后复用相同产品证据。检查修改的 Markdown 示例、链接与许可证，
再检查和计算新归档校验值。新校验值不代表后续提交重新执行产品/数据库或远端 CI。

<a id="remaining-release-gates"></a>

## 剩余发布门槛

产品输入由 `44a41f8c98b761d127623ba4b65a530bf29c2591` 引入；配置和验证文档为独立提交。
在 PR 记录最终源码提交、清单、归档哈希及实际退出码。上述 GitHub 结果仅适用于命名提交。
namespace 发布权限、任何上传式 dry run、正式发布和干净精确版本注册库消费者仍是独立门槛。
不要声称历史 Linux 结果覆盖已变化的测试工具或未来代码。
