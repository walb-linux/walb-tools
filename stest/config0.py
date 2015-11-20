#!/usr/bin/env python

import sys
sys.path.insert(0, './python')
import os
from walblib import *


isDebug = False
TIMEOUT = 100
wdevSizeMb = 12

workDir = os.getcwd() + '/stest/tmp/'
binDir = os.getcwd() + '/binsrc/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'

def D(name):
    return workDir + name

def L(name):
    return workDir + name + '.log'

s0c = ServerConnectionParam('s0', 'localhost', 10000, K_STORAGE)
s1c = ServerConnectionParam('s1', 'localhost', 10001, K_STORAGE)
s2c = ServerConnectionParam('s2', 'localhost', 10002, K_STORAGE)
p0c = ServerConnectionParam('p0', 'localhost', 10100, K_PROXY)
p1c = ServerConnectionParam('p1', 'localhost', 10101, K_PROXY)
p2c = ServerConnectionParam('p2', 'localhost', 10102, K_PROXY)
a0c = ServerConnectionParam('a0', 'localhost', 10200, K_ARCHIVE)
a1c = ServerConnectionParam('a1', 'localhost', 10201, K_ARCHIVE)
a2c = ServerConnectionParam('a2', 'localhost', 10202, K_ARCHIVE)

s0 = ServerStartupParam(s0c, binDir, D('s0'), L('s0'))
s1 = ServerStartupParam(s1c, binDir, D('s1'), L('s1'))
s2 = ServerStartupParam(s2c, binDir, D('s2'), L('s2'))
p0 = ServerStartupParam(p0c, binDir, D('p0'), L('p0'))
p1 = ServerStartupParam(p1c, binDir, D('p1'), L('p1'))
p2 = ServerStartupParam(p2c, binDir, D('p2'), L('p2'))
a0 = ServerStartupParam(a0c, binDir, D('a0'), L('a0'), 'vg0')
a1 = ServerStartupParam(a1c, binDir, D('a1'), L('a1'), 'vg1')
a2 = ServerStartupParam(a2c, binDir, D('a2'), L('a2'), 'vg2')

VOL = 'vol0'


sLayout = ServerLayout([s0, s1], [p0, p1], [a0, a1])
sLayoutAll = ServerLayout([s0, s1, s2], [p0, p1, p2], [a0, a1, a2])
sLayoutRepeater1 = ServerLayout([s0], [p0], [a0])
sLayoutRepeater2 = ServerLayout([s0], [p0], [a0, a1])

wdev0 = Device('0', '/dev/test/log',  '/dev/test/data',  wdevcPath)
wdev1 = Device('1', '/dev/test/log2', '/dev/test/data2', wdevcPath)
wdev2 = Device('2', '/dev/test/log3', '/dev/test/data3', wdevcPath)
wdevL = [wdev0, wdev1, wdev2]

walbc = Controller(walbcPath, sLayout, isDebug)


def use_thinp():
    a0.tp = 'tp0'
    a1.tp = 'tp0'
    a2.tp = 'tp0'


def get_config():
    return globals()
