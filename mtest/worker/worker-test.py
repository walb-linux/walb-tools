#!/usr/bin/env python
import unittest
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
            v = parsePERIOD(s)
            self.assertEqual(v, expect)

if __name__ == '__main__':
    unittest.main()

