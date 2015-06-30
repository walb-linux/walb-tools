#!/usr/bin/env python
import sys
sys.path.insert(0, '../python/walb/')
from walb import *

import yaml

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
