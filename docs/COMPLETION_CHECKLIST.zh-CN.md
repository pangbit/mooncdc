<a id="release-review-checklist"></a>

# 发布审阅清单

<!-- language-switch -->
[English](COMPLETION_CHECKLIST.md) | [简体中文](COMPLETION_CHECKLIST.zh-CN.md)

英文版本为权威版本。


版本：0.1.1；记录日期：2026-10-29。本清单列出门槛，不是发布回执。
每个结果须绑定审阅过的源码清单与实际退出码。

- [ ] 对照公开基线审查产品和测试变化，包括 native C 依赖、生成接口及所需许可证。
- [ ] 对照明确允许清单检查包内容，并扫描所有包含的文本；排除原始日志、主机转储、归档和扩展。
- [ ] 修改维护工具或文档后，执行受影响的离线检查。
- [ ] 记录哪些历史结果可按字节相等复用，哪些输入需要重新执行，见[验证记录](VALIDATION.zh-CN.md)。
- [ ] 获得对精确本地提交及分支 push/PR 的明确授权。
- [ ] 核对最终公开源码提交及其自己的 GitHub CI 回执。
- [ ] 确认已认证 Mooncakes 账号对 `pangbit` 的权限；仓库所有权或匿名 namespace 存在不证明此权限。
- [ ] 单独授权上传式 dry run 和/或正式发布。
- [ ] 发布后运行精确版本注册库消费者，不使用本地路径、workspace 成员关系、symlink 或依赖覆盖。

顺序见[发布流程](RELEASING.zh-CN.md)，命令见[测试](TESTING.zh-CN.md)。
不要从未勾选的模板推断门槛已经完成。
