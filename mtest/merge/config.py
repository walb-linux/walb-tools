#!/usr/bin/env python

import sys
sys.path.append('./python')
from walblib import *

binDir = '/home/mitsunari/Program/walb-tools/binsrc/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'

def dataPath(s):
    return '/var/walbm/%s/' % s

def logPath(s):
    return '/var/walbm/%s.log' % s

s0 = Server('s0', 'localhost', 10000, K_STORAGE, binDir, dataPath('s0'), logPath('s0'))
p0 = Server('p0', 'devss-17', 10100, K_PROXY, binDir, dataPath('p0'), logPath('p0'))
a0 = Server('a0', 'devss-17', 10200, K_ARCHIVE, binDir, dataPath('a0'), logPath('a0'), 'ubuntu')

sLayout = ServerLayout([s0], [p0], [a0])
walbc = Controller(walbcPath, sLayout, isDebug=False)

runCommand = walbc.get_run_remote_command(s0)
wdev0 = Device(0, '/dev/ubuntu/logm', '/dev/ubuntu/datam', wdevcPath, runCommand)

VOL = 'volm'
