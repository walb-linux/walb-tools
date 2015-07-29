#!/usr/bin/env python
import sys, signal
from walb.walb import *

import yaml

binDir = os.getcwd() + '/binsrc/'
walbcPath = binDir + 'walbc'

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

def parseSuffix(s, suf):
    if type(s) == int:
        n = s
    else:
        verify_type(s, str)
        verify_type(suf, dict)
        suffix = 1
        c = s[-1]
        if c in suf:
            suffix = suf[c]
            s = s[0:-1]
        n = int(s)
        n *= suffix
    if n < 0:
        raise Exception('parseSuffix:negative value', s)
    return n

def parsePERIOD(s):
    """
        digits suffix
        digits = [0-9]+
        suffix = (m|h|d)
    """
    return parseSuffix(s, {'m':60, 'h':3600, 'd':86400})

def parseSIZE_UNIT(s):
    """
        digits suffix
        suffix = (K|M|G)
    """
    return parseSuffix(s, {'K':1024, 'M':1024 * 1024, 'G':1024 * 1024 * 1024})

def parsePositive(d):
    verify_type(d, int)
    if d < 0:
        raise Exception('parsePositive:negative', d)
    return d

def parsePort(d):
    verify_type(d, int)
    if d <= 0 or d > 65535:
        raise Exception('parsePort', d)
    return d

class General:
    def __init__(self):
        self.addr = ""
        self.port = 0
        self.max_concurrent_tasks = 0

    def set(self, d):
        verify_type(d, dict)
        self.addr = d['addr']
        verify_type(self.addr, str)
        self.port = parsePort(d['port'])
        self.max_concurrent_tasks = parsePositive(d['max_concurrent_tasks'])

    def __str__(self):
        return "addr=%s, port=%d, max_concurrent_tasks=%d" % (self.addr, self.port, self.max_concurrent_tasks)

class Apply:
    def __init__(self):
        self.keep_days = 0
        self.time_window = (0, 0)
    def set(self, d):
        verify_type(d, dict)
        self.keep_days = parsePERIOD(d['keep_days'])
    def __str__(self):
        return "keep_days=%d" % self.keep_days

class Merge:
    def __init__(self):
        self.interval = 0
        self.max_nr = 0
        self.max_size = 0
        self.threshold_nr = 0
    def set(self, d):
        verify_type(d, dict)
        self.interval = parsePositive(d['interval'])
        if d.has_key('max_nr'):
            self.max_nr = parsePositive(d['max_nr'])
        if d.has_key('max_size'):
            self.max_size = parseSIZE_UNIT(d['max_size'])
        if d.has_key('threshold_nr'):
            self.threshold_nr = parsePositive(d['threshold_nr'])
    def __str__(self):
        return "interval=%d, max_nr=%d, max_size=%d, threshold_nr=%d" % (self.interval, self.max_nr, self.max_size, self.threshold_nr)

class ReplServer:
    def __init__(self):
        self.addr = ""
        self.port = 0
        self.interval = 0
        self.compress = ('none', 0, 0)
        self.max_merge_size = 0
        self.bulk_size = 0
    def set(self, d):
        verify_type(d, dict)
        self.addr = d['addr']
        verify_type(self.addr, str)
        self.port = parsePort(d['port'])
        self.interval = parsePERIOD(d['interval'])
        if d.has_key('compress'):
            self.compress = parseCOMPRESS_OPT(d['compress'])
        if d.has_key('max_merge_size'):
            self.max_merge_size = parseSIZE_UNIT(d['max_merge_size'])
        if d.has_key('bulk_size'):
            s = d['bulk_size']
            self.bulk_size = parseSIZE_UNIT(d['bulk_size'])
    def __str__(self):
        return "addr=%s, port=%d, interval=%d, compress=(%s, %d, %d), max_merge_size=%d, bulk_size=%d" % (self.addr, self.port, self.interval, self.compress[0], self.compress[1], self.compress[2], self.max_merge_size, self.bulk_size)

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
        ss = d['repl_servers']
        for (name, v) in ss.items():
            rs = ReplServer()
            rs.set(v)
            self.repl_servers[name] = rs

    def __str__(self):
        s = "general\n"
        s += str(self.general) + '\n'
        s += "apply\n"
        s += str(self.apply_) + '\n'
        s += "merge\n"
        s += str(self.merge) + '\n'
        s += 'repl_servers\n'
        for (name, rs) in self.repl_servers.items():
            s += name + ':' + str(rs) + '\n'
        return s

def loadConfig(configName):
    verify_type(configName, str)
    with open(configName) as f:
        s = f.read().decode('utf8')
        d = yaml.load(s)
        cfg = Config()
        cfg.set(d)
        return cfg

def handler(signum, frame):
    print "catch SIGHUP"

def setupSignal():
    signal.signal(signal.SIGHUP, handler)

class Worker:
    def _createSeverLayout(self, cfg):
        binDir = ''
        dirName = ''
        logName = ''
        self.a0 = Server('a0', cfg.general.addr, cfg.general.port, K_ARCHIVE, binDir, dirName, logName, '')
        s0 = Server('s0', '', 0, K_STORAGE, binDir, dirName, logName)
        p0 = Server('p0', '', 0, K_PROXY, binDir, dirName, logName)
        return ServerLayout([s0], [p0], [self.a0])

    def __init__(self, configName):
        verify_type(configName, str)
        setupSignal()
        self.cfg = loadConfig(configName)
        self.serverLayout = self._createSeverLayout(self.cfg)
        isDebug = True
        self.walbc = Controller(walbcPath, self.serverLayout, isDebug)

    def execOne(self):
        ls = self.walbc.get_vol_list(self.a0)
        for s in ls:
            size = self.walbc.get_vol_size_lb(self.a0, s)
            print s, size


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

    w = Worker(configName)
    w.execOne()

if __name__ == "__main__":
    main()
