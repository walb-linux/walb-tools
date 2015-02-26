#!/usr/bin/env python

import sys
sys.path.insert(0, './python/walb')
from walb import *

import os, socket, time
from contextlib import closing

from stest_util import *


def send_cmd_to_repeater(port, cmd):
    # you can write a code as the following on Python 3
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with closing(s):
        for i in xrange(3):
            try:
                s.connect(('localhost', port))
                break
            except:
                time.sleep(0.3)
        else:
            raise Exception('send_cmd_to_repeater:retry over', port, cmd)
        s.send(cmd)
        ack = s.recv(1)
        if ack != 'a':
            raise Exception('send_cmd_to_repeater:ack invalid', ack)


def startup_repeater(server, serverPort, recvPort, cmdPort, rateMbps=0, delayMsec=0,
                     logPath=None, outPath=None, isDebug=False):
    args = [
        os.getcwd() + '/binsrc/packet-repeater',
        server, str(serverPort), str(recvPort), str(cmdPort)
    ]
    if rateMbps:
        args += ['-r', str(rateMbps)]
    if delayMsec:
        args += ['-d', str(delayMsec)]
    if logPath:
        args += ['-l', logPath]
    if isDebug:
        args += ['-v']
    print "startup_repeater:args", args
    run_daemon(args, outPath)
