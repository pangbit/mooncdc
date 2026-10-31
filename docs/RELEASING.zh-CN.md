> 当前交付：[PostgreSQL → ClickHouse 库与 CLI](RELEASE_SCOPE.zh-CN.md)，包含 PG17/18 验收。C10–C13 和云端验收不在本版范围。历史完整模块证据只绑定原源码；新核心制品需要独立验收。Linux 构建栈：64 MiB。

<a id="release-procedure"></a>

# 发布流程

<!-- language-switch -->
[English](RELEASING.md) | [简体中文](RELEASING.zh-CN.md)

英文版本为权威版本。


遵循[MoonBit 发布指南](https://docs.moonbitlang.com/en/latest/toolchain/moon/package-manage-tour.html)。
CI 不执行发布。0.1.3 为工具链兼容补丁，要求 `moonc v0.10.18+6c83a7f62`，
配套 `moon` / `moonrun 0.1.20261028 (e1e1f15)`。对未变的产品输入，复用
`070df56` 的[六项成功适配 CI](https://github.com/pangbit/mooncdc/actions/runs/41149520495)。
跟踪发布源码和标签的 CI，核对精确注册库归档及文档构建，并运行一次干净的精确版本消费者。
不在其他主机重复功能矩阵。

1. 审阅[测试](TESTING.zh-CN.md)中的单元、PostgreSQL 18/17、TLS、示例和恢复门槛。
   只复用绑定于相同源码、API、依赖和已启用测试体的证据，记录输入变化和缺口。
   仅改文档不会自动使相同产品输入的结果失效。
2. 运行 `moon info --target native && moon fmt`，检查全部生成接口变化。
   运行 `moon fmt --check`、`moon build --target native --release`、`moon doc` 和
   `moon package --list`。检查源码、README、LICENSE、依赖、示例是否齐全，以及是否误含秘密或制品。
3. 需要执行包消费者门槛时，在审阅过的干净 checkout 运行
   `moon run tools/package-test.mbtx --target native`，将归档提取到临时目录。
   用 workspace/本地依赖创建独立 native 消费者，编译运行公开 API，核对包归属和完整性。
4. 审阅元数据：`pangbit/mooncdc`、semver、Apache-2.0、仓库 URL、描述、关键词、README、native 支持。
   **`moon publish --dry-run` 会上传包进行服务端验证，需要明确授权，不是本地打包检查。**
   同时记录服务端响应和 CLI 退出码；dry run 通过不等于发布或预留版本。
   只有获得该授权后，才运行 `moon run tools/publish-check.mbtx`。
   它要求 manifest 中名称/版本对应的精确 HTTP 202 dry-run 成功响应。
   仅对于 backend `mooncake-bin 0.1.20260911 (7344121 2026-09-11)`，
   识别已经观察到的情况：提取包检查通过、精确响应成功，但 wrapper 返回 exit-255。
   未知 backend 的非零码、其他服务端状态、包名/版本不符、额外错误和认证失败均拒绝。
   原始输出/退出码会打印；该兼容逻辑既不真实发布，也不会把失败验证改成绿色。
5. 用 Conventional Commit 提交检查过的明确路径，将报告绑定最终源码提交和环境。
   纯报告提交可引用其父源码提交，不能声称已测试未来代码。
6. 获准后推送精确提交并观察 GitHub CI。本地绿色不代表远端通过。
   发布时重新检查账号认证和版本可用性，不打印 token。
7. 得到明确发布授权后运行 `moon publish`，在干净消费者检查注册库实际包，
   并为匹配源码 revision 打 tag。发布验证后才更新 CHANGELOG 和 README 中的候选状态。

GitHub push、CI 成功和 Mooncakes 发布必须分别报告。CI 不需要注册库凭据，
不得向 pull request 暴露发布 token。

<a id="version-binding-and-the-remaining-registry-gate"></a>

## 版本绑定及剩余注册库门槛

原 0.1.0 内容在公开基线 `5a810d6c62e4ff4783c063159008e23760befc6a` 上的
`release/0.1.0` 组装，不合并 181 个本地审计提交。产品来源及历史 Linux 结果见
[验证范围](VALIDATION.zh-CN.md)。产品相等可以支持复用；测试/工具变化需要自己的检查。
产品输入由 `44a41f8c98b761d127623ba4b65a530bf29c2591` 引入，发布配置和验证文档
有独立审阅提交。0.1.1 发布源码 `0a38ed6` 在公开 `main` 的 `7233238` 上增加修复
`fd8c7b7`、`ce42912` 与 `568fe7c`，保留相同公共 API、依赖及交付范围。
记录最终分支提交与归档哈希，核对该精确提交的 CI。

`git archive HEAD` 只导出已提交文件。候选检查须绑定当前树清单，再在最终干净提交上
执行归档消费者；核对该精确公开提交的 CI 后，才能声称远端验收。

现有源码包消费者使用提取模块和本地 workspace，仅证明记录的源码/归档哈希下离线 API
及 native 链接行为，不证明 namespace 权限、从 Mooncakes 解析所选版本，
也不验证最终上传包。匿名查询可发现已索引版本；缺失不代表所有权或版本预留。
授权上传前确认发布账号对 `pangbit` 的权限并即时复查版本。
未明确变更前，保持现有名称、版本、仓库所有权和许可证。

发布后，在本仓库及 workspace 外创建消费者，在 `moon.mod` 普通依赖中声明
`"pangbit/mooncdc@0.1.3"`，在 `moon.pkg` 导入 root 包 `"pangbit/mooncdc"`。
采用 README/API 公开示例，运行严格 native check/test 及 debug/release build/run，
保留实际解析版本、注册库归档校验值及退出码。
不得用 `moon.work`、本地路径、symlink 或依赖覆盖指向 checkout/提取包。
只有真实注册库版本存在且该消费者通过后，此门槛才完成。

当前 `moon` 将发布委派给独立 backend，并把其非零码报告为 `moon publish failed`，见
[受测 revision 的 adapter](https://github.com/moonbitlang/moon/blob/914d7da08562120ae4d9ef181cdda58c4ff0c191/crates/moon/src/cli/mooncake_adapter.rs#L41-L67)。
兼容处理只在本项目 dry-run 预检中，未修改全局工具。
