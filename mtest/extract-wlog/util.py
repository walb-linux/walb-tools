#!/usr/bin/env python

import sys
import subprocess


def verify_type(obj, typeValue, elemType=None):
    '''
    obj       - object.
    typeValue - type like int, str, list.
    elemType  - specify type of elements if typeValue is sequence.

    '''
    if not isinstance(obj, typeValue):
        raise Exception('invalid type', type(obj), typeValue)
    if isinstance(obj, list) and elemType:
        if not all(isinstance(e, elemType) for e in obj):
            raise Exception('invalid list type', type(obj), typeValue, elemType)


def to_str(ss):
    return " ".join(ss)


def run_local_command(args, putMsg=False):
    '''
    run a command at localhost.
    args :: [str] - command line arguments.
                    The head item must be full-path executable.
    putMsg :: bool - put debug message.
    return :: str  - standard output of the command.
    '''
    verify_type(args, list, str)
    verify_type(putMsg, bool)

    if putMsg:
        print "run_command:", to_str(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                         stderr=sys.stderr, close_fds=True)
    f = p.stdout
    s = f.read().strip()
    ret = p.wait()
    if ret != 0:
        raise Exception("command error %s %d\n" % (args, ret))
    if putMsg:
        print "run_command_result:", s
    return s
