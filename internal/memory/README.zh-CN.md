<a id="memory-policy-and-sampling"></a>

# 内存策略与采样

<!-- language-switch -->
[English](README.md) | [简体中文](README.zh-CN.md)

英文版本为权威版本。


这是 Supabase ETL 内存 governor 的内部构件。流水线通过可选压力控制和自适应批次配置
使用这些样本；本包导出项是内部组合接口。

主机/cgroup 公式遵循 Supabase ETL `c1eb3f8f746c1c0394c103e7fdf8f17527de5fd4`
锁定的 sysinfo 0.39.6，MIT 许可证保留在 `LICENSE-sysinfo`。
内存域选择遵循 ETL 的 `memory_monitor.rs`。

Linux 依次选择进程 cgroup、根 cgroup 和主机。容量和剩余空间分别对可见祖先取最小值。
样本使用整个 cgroup charge，包括文件缓存，不是 RSS。
刷新先前选中的 cgroup 失败时保留旧快照；成功读到无限制 cgroup 时选择主机域。

macOS 使用 `HW_MEMSIZE` 和 `HOST_VM_INFO64`：已用页为 internal 减 purgeable，
再加 wired 和 compressed。每次调用均释放 Mach host port。

主机读取失败时，MoonCDC 保守保留先前一致样本并标为 stale；此前从未成功则返回容量为零
的未知样本。这与 sysinfo 按字段缓存的失败策略有明确差异。不支持的操作系统同样产生
stale 的未知样本。

`sample` 读取一次。`with_monitor` 独占一个周期任务，以真实样本初始化压力，跳过错过的
tick，并在所有退出路径等待任务。共享 `Monitor` 可在作用域后继续存在，但不会保留任务。
只有新鲜容量变化推进可回绕 revision；使用量和来源变化不会推进。

`Governor::with_slots` 在回调作用域注册 accumulating/in-flight 位置，成功、失败和取消
均释放。回调必须包含目标端排空。目标按容量 revision 和 slot 数刷新，与压力独立；
它们是建议目标，不是预留容量。

`Monitor::wait_for_capacity` 是可取消、有滞回的源读取门控，必须放在新的源读取之前，
不能放在目标完成之前。流水线可选紧急压力控制暂停源读取，等待前排空已接纳快照行。
可选自适应批次注册 17 个主事务/decoder 位置，以及每个表复制 worker 的一个位置。
快照行流入有界目标批次，排空后才再次积累。主 apply 仍持有整事务，不同于 ETL 的两个
逐事件批次位置，可能超出目标一个完整事务；这些提示不强制限制 heap。
