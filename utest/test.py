import unittest
from walblib import *

class TestSnapshot(unittest.TestCase):
    def test(self):
        self.assertEqual(Snapshot(2, 3), Snapshot(2, 3))
        self.assertTrue(Snapshot(2, 3) != Snapshot(3, 3))
        self.assertTrue(Snapshot(2, 3) != Snapshot(2, 4))

        tbl = [
            ("|2,3|", Snapshot(2, 3)),
            ("|5|", Snapshot(5, 5)),
            ("|7,%d|" % UINT64_MAX, Snapshot(7)),
        ]
        for t in tbl:
            s = create_snapshot_from_str(t[0])
            self.assertEqual(s, t[1])
            self.assertEqual(str(s), t[0])

class TestMetaState(unittest.TestCase):
    def test(self):
        self.assertEqual(MetaState(Snapshot(2, 3)), MetaState(Snapshot(2, 3)))
        self.assertEqual(MetaState(Snapshot(2, 3), Snapshot(3, 4)), MetaState(Snapshot(2, 3), Snapshot(3, 4)))
        self.assertTrue(MetaState(Snapshot(2, 3), Snapshot(4, 5)) != MetaState(Snapshot(2, 3)))
        self.assertTrue(MetaState(Snapshot(2, 3)) != MetaState(Snapshot(2, 3), Snapshot(4, 5)))
        self.assertTrue(MetaState(Snapshot(2, 3)) != MetaState(Snapshot(2, 4)))

class TestDiff(unittest.TestCase):
    def test(self):
        tbl = [
            "|24|-->|25,26| -- 2015-11-16T07:32:08 1",
            "|24,28|-->|30,35| M- 2015-11-16T07:32:09 123",
            "|1,5|-->|25| -C 2015-11-16T07:32:10 4567",
            "|24|-->|25| MC 2015-11-16T07:32:11 89101",
        ]
        for s in tbl:
            d = create_diff_from_str(s)
            ss = str(d)
            self.assertEqual(s, ss)

if __name__ == '__main__':
    unittest.main()
