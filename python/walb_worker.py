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

def parseSuffix(s, suf):
    verify_type(s, str)
    suffix = 1
    c = s[-1]
    if c in suf:
        suffix = suf[c]
        s = s[0:-1]
    n = int(s)
    if n < 0:
        raise Exception('parseSuffix:negative value', s)
    return n * suffix

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

class General:
    def __init__(self):
        self.addr = ""
        self.port = 0
        self.max_concurrent_tasks = 0

class Apply:
    def __init__(self):
        self.keep_days = 0
        self.time_window = (0, 0)

class Merge:
    def __init__(self):
        self.interval = 0
        self.max_nr = 0
        self.max_size = 0
        self.threshold_nr = 0

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


def loadConfig(configName):
    verify_type(configName, str)
    with open(configName) as f:
        s = f.read().decode('utf8')
        data = yaml.load(s)
        return data

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
