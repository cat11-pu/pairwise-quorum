"""quorum：纯内存的 Quorum 读写一致性仿真内核。

对外入口：
    Quorum        协调者：多数派读写、读修复、故障恢复与反熵
    Replica       单个副本
    MessageQueue  仿真消息队列
    Clock         可注入逻辑时钟
    make_version  版本号构造
"""

from .core import Clock, Message, MessageQueue, Quorum, Replica, make_version

__all__ = ["Clock", "Message", "MessageQueue", "Quorum", "Replica", "make_version"]
