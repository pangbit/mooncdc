# 自定义目标端基础契约验证

日期：2026-10-02。目标端为测试实现，没有声称已经验证任何云目标端。

普通套件的协议 peer 覆盖：Accepted 连续积累后停止刷新、数量阈值刷新、
后续 Durable 覆盖此前 Accepted、空闲刷新、空流停止、flush 失败及取消。
失败/取消后检查 checkpoint 仍为初始 LSN；成功则为最后完成事务的 end LSN。
启动/正常退出顺序、异常不调用成功退出 hook 也有断言。

PG17.11 / PG18.6 各执行：

```sh
MOONCDC_TEST_PORT=55418 moon test --target native --filter 'live destination*' -v
MOONCDC_TEST_PORT=55417 moon test --target native --filter 'live destination*' -v
```

场景包含快照写入 Accepted 后的持久屏障失败不返回位置、每次复制使用新的 attempt、
非空批次带唯一 ID、空批次无 ID、重复复制先 reset、目标端数据不重复累计。
第二次复制保存源绑定 checkpoint，拒绝覆盖同一路径；随后 Resume 并把三个真实增量事务
交付目标端，正常停止时 flush，确认源 checkpoint 前进。

公开 trait 的外部实现通过黑盒测试编译；使用默认 startup/shutdown 实现。
这些结果验证框架在目标端如实报告持久状态时的确认顺序，不能证明任意第三方实现会遵守契约。
逐表持久状态、自动重建、并发复制与真实存储目标端仍在功能对齐计划中。
