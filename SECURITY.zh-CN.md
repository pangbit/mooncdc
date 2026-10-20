<a id="security"></a>

# 安全

<!-- language-switch -->
[English](SECURITY.md) | [简体中文](SECURITY.zh-CN.md)

英文版本为权威版本。


报告漏洞时，优先使用仓库提供的私密漏洞报告功能。若不可用，可提交 issue 请求私密
联系方式，但不要附带利用细节或秘密。本项目不承诺固定的响应 SLA。

使用专用复制角色、最小表/publication 权限，以及验证证书的远程 TLS。
`LocalPlaintext` 只接受数字回环地址。不要把密码写入源码或日志。
示例测试密码仅用于隔离的 Docker 夹具。

checkpoint 保存源元数据和位置，不保存凭据。其目录应保持私密，并放在可靠支持文件锁、
rename 和 fsync 语义的本地文件系统上。校验和用于发现意外损坏，不能对抗恶意写入者或
证明文件真实性。
