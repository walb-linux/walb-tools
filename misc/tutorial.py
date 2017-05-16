#!/usr/bin/env python
import sys, os
sys.path.append('./python')
from walblib import *

binDir = os.getcwd() + '/binsrc/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'

def dataPath(s):
  return '/mnt/tutorial/data/%s/' % s

def logPath(s):
  return '/mnt/tutorial/data/%s.log' % s

s0_conn  = ServerConnectionParam('s0', 'localhost', 10000, K_STORAGE)
s0_start = ServerStartupParam(s0_conn, binDir, dataPath('s0'), logPath('s0'))

s0 = s0_start

p0_conn  = ServerConnectionParam('p0', 'localhost', 10100, K_PROXY)
p0_start = ServerStartupParam(p0_conn, binDir, dataPath('p0'), logPath('p0'))

p0 = p0_start

a0_conn  = ServerConnectionParam('a0', 'localhost', 10200, K_ARCHIVE)
a0_start = ServerStartupParam(a0_conn, binDir, dataPath('a0'), logPath('a0'), 'tutorial')

a0 = a0_start

sLayout = ServerLayout([s0], [p0], [a0])
walbc = Controller(walbcPath, sLayout) #, isDebug=False) # <= `isDebug` is False in default.

runCommand = walbc.get_run_remote_command(s0)
wdev = Device('walb-tutorial-device', '/dev/tutorial/wlog', '/dev/tutorial/wdata', wdevcPath, runCommand)

VOL = 'volm'
