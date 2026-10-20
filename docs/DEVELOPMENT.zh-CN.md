<a id="development"></a>

# 开发

<!-- language-switch -->
[English](DEVELOPMENT.md) | [简体中文](DEVELOPMENT.zh-CN.md)

英文版本为权威版本。


MoonCDC 是独立实现的 native MoonBit PostgreSQL → ClickHouse 库与 CLI，采用 Apache-2.0。
两种入口共用 `run_pipeline` 和相同的持久性/恢复契约。见[范围](RELEASE_SCOPE.zh-CN.md)。

<a id="invariants-and-acceptance"></a>

## 不变量与验收

1. 公共 `Transaction` 交付必须等待 Commit。可选 pgoutput v1 `WalBatch` 可先写出片段；
   只有 Commit 验证和目标屏障完成后才允许确认。源回滚不会进入任一 v1 路径。
   独立 v2 保留临时缓冲，按到达 offset 截断 subabort，交付或 ack 前必须收到 Stream Commit。
2. 持久位置只跨越已确认交付的连续前缀；收到字节不等于业务持久化。
3. 每次 resume 都验证 system identity、database、slot 和 publication，拒绝槽已无法提供的位置。
4. 帧体分配前检查长度；事务和未完成交付有明确上限，超限不确认数据而失败。
5. 稳定身份使用源身份和 commit 位置，不使用进程内序号，也不只使用事务 ID。
6. 晋升恢复要求持久 checkpoint 及所有可能的目标/schema 写入都位于已存时间线祖先上。
   接纳写入前先保存写入上界；未知的旧历史必须保持未知。

<a id="work-sequence"></a>

## 工作顺序

保持源码改动聚焦，并审阅生成接口。运行 `moon info --target native --frozen && moon fmt`，
再按[测试](TESTING.zh-CN.md)执行受影响单测、native 链接和集成门槛。任何测试 driver 改写都需要
自己的检查，历史结果不能认证新工具。

公共 API 在 `pkg.generated.mbti`，CLI 在 `cmd/replicator/`。
`moon.pkg` 列出的 native stubs 必须与源码和声明一起保留。
YAML 与内存包有各自接口和第三方声明。`integration/reference/` 保存固定比较夹具及迁移 SQL。

源码包有意排除原始证据和实验性云端扩展。不要仅为组装发布而合并大批审计历史。
使用明确文件清单，区分产品、测试/工具、文档、生成物和原始执行证据。

可选错误报告及 ConfigCat 的受控 peer 测试使用模拟回环服务，不证明完整外部 SDK/服务兼容。
参照测试可能需要单独准备的上游缓存、Rust 和 Go。源码改动、push、CI 回执和注册库发布
应分别记录。

<a id="commit-subject-checks"></a>

## 提交标题检查

`tools/commit-subject-check.py` 仅使用 Python 标准库和只读 Git 查询。
它遵循 CONTRIBUTING 的基本 Conventional Commit 结构，接受自定义 type、可选 scope，
以及 `feat!:` / `feat(core)!:`。英文措辞、type 和摘要准确性仍需人工审阅；非 ASCII 字母
仅提示。没有新 type 白名单、强制 scope、长度上限或正文模板。

```sh
python3 -B tools/commit-subject-check_test.py
python3 -B tools/commit-subject-check.py --subject "fix(core): preserve durable progress"
python3 -B tools/commit-subject-check.py --base origin/main --head HEAD
```

本地历史检查必须显式指定 base。GitHub CI 中，已有 ref 的 push 检查 `before..after`；
新 ref 检查 `origin/<default-branch>..after`；默认分支首次 push 仅检查其 head。
PR 检查事件的精确 `base.sha..head.sha`，而非 checkout 合成的合并提交。
合并标题不检查，但其新引入的非合并提交仍在范围内。删除 ref 没有标题。
Git 对象缺失时失败并请求 fetch 精确输入；检查器不会自行 fetch 或退回全部历史。

该 job 使用完整历史。手动 workflow dispatch 执行检查器测试，不臆造历史边界。
测试仅在临时夹具仓库创建模拟提交并清理，不改项目提交、ref、index 或 hook。
检查器不会改写消息、上传数据或安装全局 hook。
