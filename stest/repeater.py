#!/usr/bin/env python

from util import *

runRpeater(binDir, server, serverIp, recvIp, cmdIp, rateMbps=0, isDebug=False):
    args = [
        bintDir + 'packet-repeater',
        server, str(serverIp), str(recvIp), str(cmdIp)
    ]
    if rateMbps:
        args += ['-r', str(rateMbps)]
    if isDebug:
        args += ['-v']
