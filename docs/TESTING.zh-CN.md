<a id="testing"></a>

# 测试

<!-- language-switch -->
[English](TESTING.md) | [简体中文](TESTING.zh-CN.md)

英文版本为权威版本。


使用隔离 checkout 和模拟夹具。native 模块范围是 PostgreSQL → ClickHouse；云端扩展及
历史机器专用审计启动器不在本发布树中。历史结果见[验证范围](VALIDATION.zh-CN.md)，
上游比较见[参照测试](REFERENCE_TESTS.zh-CN.md)。

<a id="toolchain-and-offline-local-gates"></a>

## 工具链与离线本地门槛

已接受的工具链为 `moonc v0.10.14+7d59c7ec9` 和 `moon 0.1.20260920 (914d7da)`。
依赖固定在 `moon.mod`。网络获准时可单独用 `moon update` 准备依赖；以下命令使用已有
缓存，不发布包。

```sh
moon version --all
moon check . cmd/replicator internal/memory internal/yaml internal/saslprep --target native --frozen --deny-warn
moon test . cmd/replicator internal/memory internal/yaml internal/saslprep --target native --frozen
moon build --target native --release --frozen
moon test tools/package-test.mbtx --target native --frozen
moon test tools/replicator-config-test.mbtx --target native --frozen
moon test tools/publish-check.mbtx --target native --frozen
moon test tools/cli-active-promotion-test.mbtx --target native --frozen
moon test tools/active-store-promotion-test.mbtx --target native --frozen
python3 -B tools/package-footprint-sample_test.py
moon info --target native --frozen && moon fmt
moon fmt --check
moon package --list --frozen
moon package --frozen
```

`moon test tools/publish-check.mbtx` 仅执行解析器测试，其 `moon run` 入口会上传注册库
预检，需单独授权。独立脚本测试可能需要已有 async/x 缓存位于
`tools/_build/<script>.mbtx/.mooncakes`；不要把 frozen 缓存失败变成隐式下载。
`tools/fixtures/` 中的 active-promotion 解析器夹具是自包含模拟输入。
旧 runtime-only 模式已停用，正常 full 模式构建当前源码。

本工具链在 Linux 编译时需要已记录的 64 MiB soft stack。在构建子 shell 使用
`ulimit -S -s 65536`，然后在另一个子 shell 以 8 MiB 执行已构建测试。
`moon test ... --build-only` 与 `moon test ...` 应采用相同输入和目标目录，
避免运行时在较小栈上重新编译。仅发现 native 单测身份，不证明环境开关控制的真实测试体已运行。
预期身份必须从本次源码发现，不能把旧 17/21/43/260/439/451 总数固化成新门槛。
active-store driver 要求 root、CLI、memory、YAML 四组，并执行相同明确选择、核对实际发现总数。

<a id="package-consumer"></a>

## 包消费者

在审阅过的干净提交运行 `moon run tools/package-test.mbtx --target native --frozen`，
使用 `git archive`、检查包内容并运行提取模块的 native 消费者。
`-- --phase consumers` 选择消费者部分。dirty 审阅树必须显式打包和列清单，
因为 `git archive` 会导出旧提交。workspace 本地消费者不证明注册库解析或发布权限，
见[发布说明](RELEASING.zh-CN.md)。driver 自身使用 native 后端，使其拥有的临时目录和证据目录
在 Linux/macOS 上统一使用受限 `/tmp` 命名空间。

<a id="isolated-database-tests"></a>

## 隔离数据库测试

以下命令创建并修改模拟 SQL 夹具。先准备隔离 Docker engine 和镜像，不要指向生产服务。
Compose 将测试服务绑定到回环。获取镜像和资源预留属于单独准备，不等于运行本地审阅。

```sh
docker compose -p mooncdc-test -f integration/compose.yaml up -d --wait pg18
moon run tools/setup-db.mbtx pg18
moon run tools/setup-tls.mbtx pg18
MOONCDC_TEST_PORT=55418 MOONCDC_TEST_CA=.test-artifacts/tls-pg18/server.crt moon run tools/live-test.mbtx
docker compose -p mooncdc-clickhouse-test -f integration/clickhouse.compose.yaml up -d --wait clickhouse
MOONCDC_EXAMPLE_SERVICE=pg18 moon run tools/replicator-test.mbtx
MOONCDC_EXAMPLE_SERVICE=pg18 MOONCDC_REPLICATOR_FORMAT=yaml MOONCDC_PIPELINE_FAILOVER=true moon run tools/replicator-test.mbtx
docker compose -p mooncdc-clickhouse-test -f integration/clickhouse.compose.yaml down -v
docker compose -p mooncdc-test -f integration/compose.yaml down -v
```

对 `pg17` 使用 55417 端口和独立 TLS 路径重复。即使断言失败，也应在 always/finally
执行清理。维护中的 CI 含独立 PG17/18、TLS、恢复、standby、迁移和槽安全门槛。
`tools/parallel-copy-test.mbtx`、`tools/core-postgres-test.mbtx`、
`tools/streaming-test.mbtx`、`tools/timeline-promotion-test.mbtx` 提供进一步的自有夹具审计；
先检查镜像和工具链前提。`tools/cli-active-promotion-test.mbtx` 不使用 runtime-only
参数时执行当前源码完整审计；`MOONCDC_ACTIVE_COPY=true` 启用 active COPY，
`MOONCDC_ACTIVE_LOSE_STATE_OWNER=true` 启用独立 owner-loss 矩阵。

本页命令不改变 TasksMax、pids.max 或其他宿主策略。历史 256 编译预算、数据库实测总峰值
264 和单次临时 512 诊断上限是不同观察。新的受限运行需要独立资源预留、真实线程/进程
采样和清理读回，不应把旧测试限制写成产品最低需求。

<a id="evidence-to-retain"></a>

## 应保留的证据

记录源码/dirty 树清单、工具链/依赖哈希、启用的测试体、服务端/镜像版本、精确命令及
实际退出码。区分预期注入失败、非预期失败、跳过和未知步骤。
原始日志留在跟踪源码之外，例如被忽略的临时测试目录。分享摘要前检查凭据、个人/客户
数据、主机路径和无关进程信息。`.gitignore`/`.moonignore` 无法移除已提交文件。
