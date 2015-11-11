#!/usr/bin/env python

import sys
sys.path.insert(0, './python/walb')
import os
from walb import *

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
p0c = ServerConnectionParam('p0', 'localhost', 10100, K_PROXY)
a0c = ServerConnectionParam('a0', 'localhost', 10200, K_ARCHIVE)

s0 = ServerStartupParam(s0c, binDir, D('s0'), L('s0'))
p0 = ServerStartupParam(p0c, binDir, D('p0'), L('p0'))
a0 = ServerStartupParam(a0c, binDir, D('a0'), L('a0'), 'vg0')

sLayout = ServerLayout([s0], [p0], [a0])
sLayoutAll = ServerLayout([s0], [p0], [a0])

VOL0 = 'vol0'
VOL1 = 'vol1'
VOL2 = 'vol2'

wdev0 = Device('0', '/dev/test/log',  '/dev/test/data',  wdevcPath)
wdev1 = Device('1', '/dev/test/log2', '/dev/test/data2', wdevcPath)
wdev2 = Device('2', '/dev/test/log3', '/dev/test/data3', wdevcPath)
wdevL = [wdev0, wdev1, wdev2]


walbc = Controller(walbcPath, sLayout, isDebug)


def use_thinp():
    a0.tp = 'tp0'


maxFgTasks = 6
maxBgTasks = 3


def get_config():
    return globals()
