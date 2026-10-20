<a id="configuration-yaml-syntax"></a>

# 配置 YAML 语法

<!-- language-switch -->
[English](README.md) | [简体中文](README.zh-CN.md)

英文版本为权威版本。


八个语法/事件文件来自
[moonbit-community/yaml.mbt](https://github.com/moonbit-community/yaml.mbt/tree/e2f1129249a7cc707aa1c9c39bd8bfe207c93821)，
版本 0.0.7，提交 `e2f1129249a7cc707aa1c9c39bd8bfe207c93821`。
归档 SHA-256：`3a5d8124a04409a0176f976603225f5ffea75c42fe1c0bef50ef68bafe7ee471`。
MIT 许可证及原 yaml-rust 作者声明保留在 `LICENSE`。

本地语法改动：在 `scan_verbatim_tag` 中推进 lexer，避免死循环；flow 嵌套上限从 255
降到 64。不使用原 YAML loader 或 emitter。`config.mbt` 是 MoonCDC 的有界配置加载器，
匹配 ETL 固定 `config 0.14.1` / `yaml-rust2 0.8.1` 的标量规则，包括仅小写隐式布尔、
将 merge key 视为字面 key，以及无效显式标量转换为 null。支持空 mapping key 和整数 key；
在字符串 key 规范化前拒绝重复 key。

加载器限制嵌套为 64，展开后的节点/标量/key 成本为 1,048,576。重复别名计入完整展开
成本和高度，共享节点不能绕过限制。CLI 同时限制文件为 1 MiB。
拒绝非有限数字，以及非字符串/非整数 mapping key；参照实现可能返回非有限值，
或对不支持的 key panic。这些是有意限制的输入域。保留参照的多字节 URI tag escape
行为，中文 escape 向量在两个实现中都被拒绝。

`integration/reference/yaml-cases.json` 和 `yaml-expected.json` 记录固定 Rust 依赖的
34 个结果。运行 `moon run tools/config-reference-test.mbtx` 可在临时项目重新生成参照，
检查已存结果与 MoonCDC 行为；`-- --record` 会有意刷新夹具。
