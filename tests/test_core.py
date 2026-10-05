"""quorum.core 的行为测试：多数派读写、读修复、故障恢复与版本推进。"""

import unittest

from quorum.core import Clock, Quorum, make_version


class QuorumCoreTest(unittest.TestCase):
    """覆盖正常路径、边界输入、故障、恢复与版本不变量。"""

    def setUp(self):
        self.clock = Clock()
        self.q = Quorum(clock=self.clock)

    def test_write_then_read_round_trip(self):
        """写入成功之后，读必须返回刚写入的值。"""
        self.assertTrue(self.q.write("k", "v1"))
        got = self.q.read("k")
        self.assertIsNotNone(got)
        self.assertEqual(got[0], "v1")

    def test_read_write_survive_single_failure(self):
        """三副本中一台故障时，写和读都应当继续成功。"""
        self.q.fail("n2")
        self.assertTrue(self.q.write("k", "v1"))
        got = self.q.read("k")
        self.assertIsNotNone(got)
        self.assertEqual(got[0], "v1")

    def test_write_without_majority_is_rejected(self):
        """失去多数派时写必须失败，并且不得留下任何数据。"""
        self.q.fail("n1")
        self.q.fail("n2")
        self.assertFalse(self.q.write("k", "v1"))
        self.q.recover("n1")
        self.q.recover("n2")
        self.assertIsNone(self.q.read("k"))

    def test_failed_write_leaves_no_partial_value(self):
        """确认数不足而失败的写入，不得残留在任何副本上。"""
        clock = Clock()
        q = Quorum(clock=clock, timeout=5)
        self.assertTrue(q.write("k", "v1"))
        q.set_latency("n1", 4)
        q.set_latency("n2", 6)
        self.assertFalse(q.write("k", "v2"))
        q.set_latency("n1", 2)
        q.set_latency("n2", 3)
        got = q.read("k")
        self.assertIsNotNone(got)
        self.assertEqual(got[0], "v1")

    def test_read_repairs_replica_missing_key(self):
        """读修复之后，所有在线副本都应持有该键的最新版本。"""
        self.assertTrue(self.q.write("k", "v1"))
        self.q.replica("n2").lose("k")
        got = self.q.read("k")
        self.assertIsNotNone(got)
        self.assertEqual(got[0], "v1")
        self.assertEqual(self.q.state("n2"), self.q.state("n0"))

    def test_version_tie_keeps_higher_coordinator(self):
        """同版本号的并发写入必须按协调者标识决胜，并让副本收敛。"""
        self.q.replica("n0").put("k", make_version(5, "c0"), "alpha")
        self.q.replica("n1").put("k", make_version(5, "c1"), "beta")
        self.q.replica("n2").put("k", make_version(4, "c0"), "gamma")
        got = self.q.read("k")
        self.assertIsNotNone(got)
        self.assertEqual(got[0], "beta")
        for node_id in ("n0", "n1", "n2"):
            entry = self.q.state(node_id)["k"]
            self.assertEqual(entry[1], "beta")
            self.assertGreaterEqual(tuple(entry[0]), tuple(make_version(4, "c0")))

    def test_recovered_replica_is_caught_up(self):
        """副本恢复之后必须先补齐数据，再重新参与读写。"""
        self.q.fail("n2")
        self.assertTrue(self.q.write("k", "v1"))
        self.q.recover("n2")
        self.assertEqual(self.q.state("n2"), self.q.state("n0"))
        got = self.q.read("k")
        self.assertIsNotNone(got)
        self.assertEqual(got[0], "v1")

    def test_version_does_not_regress_on_clock_rewind(self):
        """逻辑时钟回拨之后，新写入的版本号仍必须大于此前的写入。"""
        self.assertTrue(self.q.write("k", "v1"))
        self.clock.advance(-1000)
        self.assertTrue(self.q.write("k", "v2"))
        got = self.q.read("k")
        self.assertIsNotNone(got)
        self.assertEqual(got[0], "v2")

    def test_quorum_sets_must_overlap(self):
        """R 加 W 不大于 N 的配置必须在构造时被拒绝。"""
        with self.assertRaises(ValueError):
            Quorum(clock=Clock(), n=2, r=1, w=1)
        with self.assertRaises(ValueError):
            Quorum(clock=Clock(), n=3, r=1, w=2)


if __name__ == "__main__":
    unittest.main()
