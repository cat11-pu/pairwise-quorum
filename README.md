# pairwise-quorum

一个纯内存的 Quorum 读写一致性仿真内核：可注入逻辑时钟 + 消息队列 + 多副本对象，
覆盖多数派读写、版本号推进、读修复、副本故障与恢复、反熵补齐。

- 只用 Python 标准库，不需要安装任何依赖，也不会发起任何网络请求。
- 仿真时间由内核按 tick 自己推进，消息投递时刻等于发送时刻加单程延迟，
  所以任何一次运行的结果都是确定的。

## 目录

- quorum/core.py：内核实现（时钟、消息队列、副本、协调者）
- tests/test_core.py：内核的行为测试

## 怎么跑测试

在项目根目录执行：

    python3 -m unittest discover -s tests -v

Windows 上把 python3 换成你的解释器路径，例如：

    C:/Users/<你>/AppData/Local/Programs/Python/Python313/python.exe -m unittest discover -s tests -v
