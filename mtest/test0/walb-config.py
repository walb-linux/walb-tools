#!/usr/bin/env python

import sys
import os
sys.path.insert(0, os.getcwd() + '/../walb-tools/python')

from walblib import *

binDir = '/home/hoshino/walb-tools/binsrc/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'

def dataPath(s):
  return '/var/walbh/%s/' % s

def logPath(s):
  return '/var/walbh/%s.log' % s

s0 = Server('s0', '10.7.4.12', 20000, K_STORAGE, binDir, dataPath('s0'), logPath('s0'))
p0 = Server('p0', '10.7.4.17', 20100, K_PROXY,   binDir, dataPath('p0'), logPath('p0'))
a0 = Server('a0', '10.7.4.17', 20200, K_ARCHIVE, binDir, dataPath('a0'), logPath('a0'), 'ubuntu')

sLayout = ServerLayout([s0], [p0], [a0])
walbc = Controller(walbcPath, sLayout, isDebug=False)

VOL = 'volh'
