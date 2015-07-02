#!/usr/bin/env python
import sys
sys.path.insert(0, '../python/walb/')
from walb import *

import yaml

def parseFLAG(s):
    verify_type(s, str)
    if s == '0':
        return False
    if s == '1':
        return True
    raise Exception('parseFLAG:bad s', s)

def parseCOMPRESS_OPT(s):
    """
        MODE:LEVEL:NUM_CPU
        MODE=(none|snappy|gzip|lzma)
        LEVEL=[0-9]
        NUM_CPU=digits
    """
    verify_type(s, str)
    ss = s.split(':')
    if len(ss) > 3:
        raise Exception('parseCOMPRESS_OPT:bad s', s)
    mode = 'none'
    level = 0
    numCpu = 0
    if len(ss) > 0:
        mode = ss[0]
        if mode not in ['none', 'snappy', 'gzip', 'lzma']:
            raise Exception('parseCOMPRESS_OPT:bad MODE', mode, s)
    if len(ss) > 1:
        level = int(ss[1])
        if level < 0 or level > 9:
            raise Exception('parseCOMPRESS_OPT:bad LEVEL', level, s)
    if len(ss) > 2:
        numCpu = int(ss[2])
        if numCpu < 0:
            raise Exception('parseCOMPRESS_OPT:bad NUM_CPU', numCpu, s)
    return (mode, level, numCpu)

def parseSuffix(s, suf, msg=""):
    verify_type(s, str)
    suffix = 1
    c = s[-1]
    if c in suf:
        suffix = suf[c]
        s = s[0:-1]
    n = int(s)
    if n < 0:
        raise Exception(msg + 'parseSuffix:negative value', s)
    return n * suffix

def parsePERIOD(s, msg=""):
    """
        digits suffix
        digits = [0-9]+
        suffix = (m|h|d)
    """
    return parseSuffix(s, {'m':60, 'h':3600, 'd':86400}, msg)

def parseSIZE_UNIT(s, msg=""):
    """
        digits suffix
        suffix = (K|M|G)
    """
    return parseSuffix(s, {'K':1024, 'M':1024 * 1024, 'G':1024 * 1024 * 1024}, msg)

def parsePositive(s, msg=""):
    d = int(s)
    if d < 0:
        raise Exception(msg + 'parsePositive:negative', s)
    return d

class General:
    def __init__(self):
        self.addr = ""
        self.port = 0
        self.max_concurrent_tasks = 0

    def set(self, d):
        verify_type(d, dict)
        self.addr = s['addr']
        verify_type(self.addr, str)
        self.port = parsePositive(s['port'], 'General:port')
        if self.port > 65535:
            raise Exception('General:set:bad port', self.port)
        self.max_concurrent_tasks = parsePositive(s['max_concurrent_tasks'], 'General:set:bad max_concurrent_tasks')

    def __str__(self):
        return "addr=%s, port=%d, max_concurrent_tasks=%d" % (self.addr, self.port, self.max_concurrent_tasks)

class Apply:
    def __init__(self):
        self.keep_days = 0
        self.time_window = (0, 0)
    def set(self, d):
        verify_type(d, dict)
        self.keep_days = parsePERIOD(d['keep_days'])
        s = d['time_window']
        ss = s.split(',')
        if len(ss) != 2:
            raise Exception('Apply:bad time_window', s)
        self.time_window = (int(ss[0]), int(ss[1]))
    def __str__(self):
        return "keep_days=%d, time_window=(%d, %d)" % (self.keep_days, self.time_window[0], self.time_window[1])

class Merge:
    def __init__(self):
        self.interval = 0
        self.max_nr = 0
        self.max_size = 0
        self.threshold_nr = 0
    def set(self, d):
        verify_type(d, dict)
        self.interval = parsePositive(d['interval'], 'Merege:set:interval')
        self.max_nr = parsePositive(d['max_nr'], 'Merge:set:max_nr')
        self.max_size = parseSIZE_UNIT(d['max_size'], 'Merge:set:max_size')
        self.threshold_nr = parsePositive(d['threshold_nr'], 'Merge:set:threshold_nr')
    def __str__(self):
        return "interval=%d, max_nr=%d, max_size=%d, threshold_nr=%d" % (self.interval, self.max_nr, self.max_size, self.threshold_nr)

class ReplServer:
    def __init__(self):
        self.name = ""
        self.addr = ""
        self.port = 0
        self.interval = 0
        self.compress = ()
        self.max_merge_size = 0
        self.bulk_size = 0

class Config:
    def __init__(self):
        self.general = General()
        self.apply_ = Apply()
        self.merge = Merge()
        self.repl_servers = {}

	def set(self, d):
		verify_type(d, dict)
		self.general.set(d['general'])
        self.apply_.set(d['apply'])
        self.merge.set(d['merge'])
        self.repl_servers.set(d['repl_servers'])

    def __str__(self):
        return "%s\n%s\n%s\n%s" % (self.general, self.apply_, self.merge, self.repl_servers)


def loadConfig(configName):
    verify_type(configName, str)
    with open(configName) as f:
        s = f.read().decode('utf8')
        d = yaml.load(s)
        cfg = Config()
        cfg.set(d)
        return d

def usage():
    print "walb-worker [-f configName]"
    exit(1)

def main():
    configName = ""
    i = 1
    argv = sys.argv
    argc = len(argv)
    while i < argc:
        c = argv[i]
        if c == '-f' and i < argc - 1:
            configName = argv[i + 1]
            i += 2
            continue
        else:
            print "option error", argv[i]
            usage()

    if not configName:
        print "set -f option"
        usage()
    cfg = loadConfig(configName)
    print cfg

if __name__ == "__main__":
    main()
