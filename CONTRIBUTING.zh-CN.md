<a id="contributing"></a>

# 贡献指南

<!-- language-switch -->
[English](CONTRIBUTING.md) | [简体中文](CONTRIBUTING.zh-CN.md)

英文版本为权威版本。


本项目目前由一位维护者维护，欢迎范围明确的小型 issue 和 pull request。
修改公共 API 或协议范围前，请先讨论。API 名称和提交标题使用英文。
重要文档以现有英文路径为默认和权威入口，中文使用独立的 `.zh-CN.md` 文件，
双方提供语言链接，并保持支持范围、限制和可执行命令一致。
非关键历史或工具说明可保留原语言；许可证原文和原始证据不翻译。
不要包含凭据、私有数据集、数据库目录或 WAL。

1. 说明观察到的行为，以及预期的事务或持久性不变量。
2. 添加有针对性的回归测试，不夹带无关改动。
3. 运行 `moon check --target native --deny-warn`、`moon test --target native`、
   `moon build --target native --release`、`moon info --target native`、`moon fmt`。
4. 协议或恢复改动需执行[测试说明](docs/TESTING.zh-CN.md)中的 Docker PostgreSQL 18/17 套件。
   模拟证据和真实数据库证据应分别标明。
5. 审阅 `pkg.generated.mbti`。采用 Conventional Commit，例如
   `fix(checkpoint): preserve contiguous acknowledgement after write failure`。

可以在不创建提交的情况下检查标题草稿：
`python3 -B tools/commit-subject-check.py --subject "fix(checkpoint): preserve durable progress"`。
检查器接受自定义 type、可选 scope 和冒号前的 `!`，没有标题长度上限或正文模板要求。
它只检查结构；英文措辞、type 选择和摘要准确性仍需人工审阅。
非 ASCII 字母只产生提示，因为英文标题也可能引用 Unicode 标识符。
结构通过不代表语言一定是英文。

CI 只检查 push 或 PR 新引入的非合并提交，不检查全部历史、PR 标题或 GitHub 合成的
合并标题。范围选择和本地命令见[开发说明](docs/DEVELOPMENT.zh-CN.md#commit-subject-checks)。
不需要安装全局 hook。

贡献与仓库其他内容一样采用 Apache-2.0。不要手工编辑生成接口，也不要在未检查变化时
刷新快照。
