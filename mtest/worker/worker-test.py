#!/usr/bin/env python
import unittest, yaml
import sys
sys.path.insert(0, '../../python/walb/')
from walb import *
sys.path.insert(0, '../../python/')
from walb_worker import *

class TestParsePERIOD(unittest.TestCase):
    def test(self):
        d = {
            '123':123,
            '10m':10 * 60,
            '100d':100 * 86400,
        }
        for (s, expect) in d.items():
            v = parsePERIOD(s)
            self.assertEqual(v, expect)

class TestParseSIZE_UNIT(unittest.TestCase):
    def test(self):
        d = {
            '123':123,
            '10K':10 * 1024,
            '7M':7 * 1024 * 1024,
            '8G':8 * 1024 * 1024 * 1024,
        }
        for (s, expect) in d.items():
            v = parseSIZE_UNIT(s)
            self.assertEqual(v, expect)

class TestParseCOMPRESS_OPT(unittest.TestCase):
    def test(self):
        d = {
            "none":('none', 0, 0),
            "snappy:3":('snappy', 3, 0),
            "gzip:9:4":('gzip', 9, 4),
            "lzma:0:123":('lzma', 0, 123),
        }
        for (s, expect) in d.items():
            v = parseCOMPRESS_OPT(s)
            self.assertEqual(v, expect)

configStr = """
general:
  addr: 192.168.0.1
  port: 10000
  max_concurrent_tasks: 10
apply:
  keep_days: 14d
merge:
  interval: 10
  max_nr: 10
  max_size: 1M
  threshold_nr: 5
repl_servers:
  repl0:
    addr: 192.168.0.2
    port: 10001
    interval: 3d
    compress: snappy:3:4
    max_merge_size: 5K
    bulk_size: 40
  repl1:
    addr: 192.168.0.3
    port: 10002
    interval: 2h
    compress: gzip
    max_merge_size: 2M
    bulk_size: 400
"""

class TestLoadConfig(unittest.TestCase):
    def test(self):
        d = yaml.load(configStr)
        cfg = Config()
        cfg.set(d)
        general = cfg.general
        self.assertEqual(general.addr, '192.168.0.1')
        self.assertEqual(general.port, 10000)
        self.assertEqual(general.max_concurrent_tasks, 10)
        apply_ = cfg.apply_
        self.assertEqual(apply_.keep_days, 14 * 86400)
        merge = cfg.merge
        self.assertEqual(merge.interval, 10)
        self.assertEqual(merge.max_nr, 10)
        self.assertEqual(merge.max_size, 1024 * 1024)
        self.assertEqual(merge.threshold_nr, 5)
        repl_servers = cfg.repl_servers
        r = repl_servers['repl0']
        self.assertEqual(r.addr, '192.168.0.2')
        self.assertEqual(r.port, 10001)
        self.assertEqual(r.interval, 3 * 86400)
        self.assertEqual(r.compress, ('snappy', 3, 4))
        self.assertEqual(r.max_merge_size, 5 * 1024)
        self.assertEqual(r.bulk_size, 40)

        r = repl_servers['repl1']
        self.assertEqual(r.addr, '192.168.0.3')
        self.assertEqual(r.port, 10002)
        self.assertEqual(r.interval, 2 * 3600)
        self.assertEqual(r.compress, ('gzip', 0, 0))
        self.assertEqual(r.max_merge_size, 2 * 1024 * 1024)
        self.assertEqual(r.bulk_size, 400)

if __name__ == '__main__':
    unittest.main()
