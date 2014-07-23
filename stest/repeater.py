#!/usr/bin/env python
import os, socket, time
from walb import *
from contextlib import closing


def send_cmd_to_repeater(ip, cmd):
    for i in xrange(3):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            break
        except:
            time.sleep(0.3)
    else:
        raise Exception('send_cmd_to_repeater:retry over', ip, cmd)
    with closing(s):
        s.connect(('localhost', ip))
        s.send(cmd)
        s.close()


def quit_repeater(ip):
    try:
        send_cmd_to_repeater(ip, 'quit')
    except:
        pass


class Repeater:
    '''
    packet repeater
    '''
    def __init__(self, server, serverIp, recvIp, cmdIp, rateMbps=0, delayMsec=0, isDebug=False):
        args = [
            os.getcwd() + '/binsrc/packet-repeater',
            server, str(serverIp), str(recvIp), str(cmdIp)
        ]
        if rateMbps:
            args += ['-r', str(rateMbps)]
        if delayMsec:
            args += ['-d', str(delayMsec)]
        if isDebug:
            args += ['-v']
            print "Repeater:args", args
        run_daemon(args)
        self.cmdIp = cmdIp

    def stop(self):
        send_cmd_to_repeater(self.cmdIp, 'stop')

    def start(self):
        send_cmd_to_repeater(self.cmdIp, 'start')

    def quit(self):
        send_cmd_to_repeater(self.cmdIp, 'quit')
