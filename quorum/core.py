"""Quorum 读写一致性内核（纯内存仿真）。

组件：
    make_version  版本号构造：计数器 + 协调者标识
    Clock         可注入逻辑时钟，由仿真侧手动推进
    Message       消息队列中的一条消息
    MessageQueue  按投递时刻排序的仿真消息队列
    Replica       单个副本：内存 KV（key -> (版本号, 值)）
    Quorum        协调者：多数派读写、读修复、故障恢复与反熵

仿真约定：
    * 所有时间都是时钟 tick，由 Quorum 在操作过程中推进；
    * 读写请求与响应都经过 MessageQueue，投递时刻 = 发送时刻 + 单程延迟；
    * 单程延迟缺省按副本序号递增（n0 为 1，n1 为 2，依此类推）。
"""


def make_version(counter, coordinator_id):
    """构造版本号：计数器 + 协调者标识；计数器并列时以协调者标识决胜。"""
    return (int(counter), str(coordinator_id))


def _compare_version(a, b):
    """版本号比较：返回正数表示 a 更新，负数表示 a 更旧，0 表示相同。"""
    if a[0] != b[0]:
        return a[0] - b[0]
    if a[1] == b[1]:
        return 0
    return 1 if a[1] > b[1] else -1


class Clock:
    """可注入逻辑时钟。advance 允许负值，用于模拟时钟漂移。"""

    def __init__(self, start=0):
        self._now = int(start)

    def now(self):
        return self._now

    def advance(self, ticks=1):
        self._now += int(ticks)
        return self._now


class Message:
    """消息队列中的一条消息。"""

    __slots__ = ("seq", "src", "dst", "kind", "op_id", "key", "version",
                 "value", "entry", "sent_at", "deliver_at")

    def __init__(self, seq, src, dst, kind, sent_at, deliver_at,
                 op_id=None, key=None, version=None, value=None, entry=None):
        self.seq = seq
        self.src = src
        self.dst = dst
        self.kind = kind
        self.op_id = op_id
        self.key = key
        self.version = version
        self.value = value
        self.entry = entry
        self.sent_at = sent_at
        self.deliver_at = deliver_at

    def __repr__(self):
        return "Message(%s %s->%s key=%r)" % (self.kind, self.src, self.dst, self.key)


class MessageQueue:
    """按投递时刻排序的仿真消息队列。"""

    def __init__(self, clock):
        self.clock = clock
        self._pending = []
        self._seq = 0

    def send(self, src, dst, kind, delay=1, **payload):
        """投递一条消息，返回入队的消息对象。"""
        self._seq += 1
        now = self.clock.now()
        msg = Message(seq=self._seq, src=src, dst=dst, kind=kind,
                      sent_at=now, deliver_at=now + int(delay), **payload)
        self._pending.append(msg)
        return msg

    def due(self, now):
        """取出所有到期的消息，按投递时刻与入队顺序排序。"""
        ready = [m for m in self._pending if m.deliver_at <= now]
        if ready:
            self._pending = [m for m in self._pending if m.deliver_at > now]
            ready.sort(key=lambda m: (m.deliver_at, m.seq))
        return ready

    def cancel(self, predicate):
        """撤回满足条件的在途消息，返回撤回条数。"""
        kept = [m for m in self._pending if not predicate(m)]
        dropped = len(self._pending) - len(kept)
        self._pending = kept
        return dropped

    def pending(self):
        """当前在途消息。"""
        return list(self._pending)


class Replica:
    """单个副本：内存 KV 与仿真参数。"""

    def __init__(self, node_id, latency=1):
        self.node_id = node_id
        self.latency = int(latency)
        self.up = True
        self.data = {}

    def get(self, key):
        """读取本地条目，返回 (版本号, 值)；不存在时返回 None。"""
        return self.data.get(key)

    def put(self, key, version, value):
        """按版本号写入本地，返回是否落盘。"""
        cur = self.data.get(key)
        if cur is not None and _compare_version(version, cur[0]) <= 0:
            return False
        self.data[key] = (version, value)
        return True

    def restore(self, key, entry):
        """把 key 恢复到指定条目；entry 为 None 表示删除。"""
        if entry is None:
            self.data.pop(key, None)
        else:
            self.data[key] = (entry[0], entry[1])

    def lose(self, key):
        """模拟数据丢失：丢弃本地该 key 的数据。"""
        return self.data.pop(key, None)

    def snapshot(self):
        """返回本地数据的副本。"""
        return dict(self.data)


class Quorum:
    """多数派读写的协调者。

    参数：
        clock          可注入逻辑时钟，缺省新建一个
        replicas       副本列表；缺省按 n 新建，单程延迟依次为 1..n
        n              副本数（仅在未给出 replicas 时生效）
        coordinator_id 协调者标识，参与版本号构造
        w              写多数派，缺省 n // 2 + 1
        r              读多数派，缺省 n - w + 1
        timeout        单次操作的仿真超时（tick）
        queue          消息队列；多个协调者共享同一组副本时必须共享队列
    """

    def __init__(self, clock=None, replicas=None, n=3, coordinator_id="c0",
                 w=None, r=None, timeout=8, queue=None):
        self.clock = clock if clock is not None else Clock()
        if replicas is None:
            replicas = [Replica("n%d" % i, latency=i + 1) for i in range(n)]
        self.replicas = list(replicas)
        self.n = len(self.replicas)
        self.coordinator_id = str(coordinator_id)
        self.w = int(w) if w is not None else self.n // 2 + 1
        self.r = int(r) if r is not None else self.n - self.w + 1
        self.timeout = int(timeout)
        self.queue = queue if queue is not None else MessageQueue(self.clock)
        self._validate()
        self._op_id = 0
        self._counter = -1
        self._acks = []
        self._responses = {}

    # ------------------------------------------------------------- 配置与运维

    def _validate(self):
        if self.n < 1:
            raise ValueError("Quorum 至少需要一个副本")
        if not 1 <= self.w <= self.n:
            raise ValueError("W 必须落在 1..N 之间")
        if not 1 <= self.r <= self.n:
            raise ValueError("R 必须落在 1..N 之间")
        if self.r + self.w <= self.n:
            raise ValueError("R + W 必须大于 N，否则读写集合可能不相交")

    def replica(self, node_id):
        """按 id 取副本。"""
        node = self._lookup(node_id)
        if node is None:
            raise KeyError(node_id)
        return node

    def state(self, node_id):
        """取某个副本本地数据的快照。"""
        return self.replica(node_id).snapshot()

    def set_latency(self, node_id, ticks):
        """仿真注入：调整到某个副本的单程延迟。"""
        node = self.replica(node_id)
        node.latency = int(ticks)
        return node

    def fail(self, node_id):
        """副本故障下线，并丢弃与其相关的在途消息。"""
        node = self.replica(node_id)
        node.up = False
        self.queue.cancel(lambda m: m.src == node_id or m.dst == node_id)
        return node

    def recover(self, node_id):
        """副本恢复上线：先与在线副本做反熵补齐，再重新参与读写。"""
        node = self.replica(node_id)
        node.up = True
        self._anti_entropy(node)
        return node

    # ------------------------------------------------------------------ 读写

    def write(self, key, value):
        """多数派写入；返回是否成功。"""
        targets = [node for node in self.replicas if node.up]
        version = self._next_version()
        prev = {node.node_id: node.get(key) for node in targets}
        self._begin_op()
        for node in targets:
            self._send("write", node.node_id, key=key, version=version, value=value)
        deadline = self.clock.now() + self.timeout
        self._pump_until(lambda: len(self._acks) >= self.w, deadline)
        if len(self._acks) < self.w:
            self._abort_write(key, version, prev)
            return False
        return True

    def read(self, key):
        """多数派读取并做读修复；返回 (值, 版本号)，读不到时返回 None。"""
        targets = [node for node in self.replicas if node.up]
        self._begin_op()
        for node in targets:
            self._send("read", node.node_id, key=key)
        deadline = self.clock.now() + self.timeout
        self._pump_until(lambda: len(self._responses) >= len(targets), deadline)
        if len(self._responses) < self.r:
            return None
        best = self._pick_latest(self._responses)
        if best is None:
            return None
        version, value = best
        self._read_repair(key, version, value, self._responses)
        return (value, version)

    # ------------------------------------------------------------------ 内部

    def _next_version(self):
        """生成下一个版本号。"""
        counter = self.clock.now()
        for node in self.replicas:
            for entry in node.data.values():
                if entry[0][0] > counter:
                    counter = entry[0][0]
        if counter <= self._counter:
            counter = self._counter + 1
        self._counter = counter
        return make_version(counter, self.coordinator_id)

    def _begin_op(self):
        self._op_id += 1
        self._acks = []
        self._responses = {}

    def _send(self, kind, dst, **payload):
        delay = self.replica(dst).latency
        return self.queue.send(self.coordinator_id, dst, kind,
                               delay=delay, op_id=self._op_id, **payload)

    def _pump(self):
        """推进一个 tick，并投递到期消息。"""
        self.clock.advance(1)
        for msg in self.queue.due(self.clock.now()):
            self._handle(msg)

    def _pump_until(self, ready, deadline):
        """推进时钟直到条件满足或超时。"""
        while not ready() and self.clock.now() < deadline:
            self._pump()
        return ready()

    def _handle(self, msg):
        if msg.dst == self.coordinator_id:
            if msg.op_id != self._op_id:
                return
            if msg.kind == "ack":
                self._acks.append(msg.src)
            elif msg.kind == "read_ack":
                self._responses[msg.src] = msg.entry
            return
        node = self._lookup(msg.dst)
        if node is None or not node.up:
            return
        if msg.kind in ("write", "repair"):
            node.put(msg.key, msg.version, msg.value)
            self.queue.send(msg.dst, msg.src, "ack",
                            delay=node.latency, op_id=msg.op_id)
        elif msg.kind == "read":
            self.queue.send(msg.dst, msg.src, "read_ack", delay=node.latency,
                            op_id=msg.op_id, entry=node.get(msg.key))

    def _lookup(self, node_id):
        for node in self.replicas:
            if node.node_id == node_id:
                return node
        return None

    def _pick_latest(self, responses):
        """在响应中选出最新的 (版本号, 值)。"""
        best = None
        for node_id in sorted(responses):
            entry = responses[node_id]
            if entry is None:
                continue
            if best is None or _compare_version(entry[0], best[0]) > 0:
                best = entry
        return best

    def _read_repair(self, key, version, value, responses):
        """把最新版本回灌给落后的副本。"""
        stale = []
        for node in self.replicas:
            if not node.up:
                continue
            entry = responses.get(node.node_id)
            if entry is None or _compare_version(entry[0], version) < 0:
                stale.append(node.node_id)
        for node_id in stale:
            self._send("repair", node_id, key=key, version=version, value=value)
        if stale:
            self._pump_until(lambda: len(self._acks) >= len(stale),
                             self.clock.now() + self.timeout)

    def _abort_write(self, key, version, prev):
        """回滚失败的写入：把已确认的副本恢复成写入前的样子。"""
        self.queue.cancel(
            lambda m: m.op_id == self._op_id and m.kind in ("write", "ack"))
        for node in self.replicas:
            entry = node.get(key)
            if entry is not None and _compare_version(entry[0], version) == 0:
                node.restore(key, prev.get(node.node_id))

    def _anti_entropy(self, node):
        """反熵：把本副本的数据与其它在线副本对齐。"""
        latest = {}
        for peer in self.replicas:
            if peer is node or not peer.up:
                continue
            for key, entry in peer.snapshot().items():
                cur = latest.get(key)
                if cur is None or _compare_version(entry[0], cur[0]) > 0:
                    latest[key] = entry
        for key, entry in latest.items():
            cur = node.get(key)
            if cur is None or _compare_version(entry[0], cur[0]) > 0:
                node.put(key, entry[0], entry[1])
