#!/usr/bin/env python

import collections
import os
import threading
import subprocess
import sys
import time
import socket
import errno
import shutil

Mebi = (1 << 20)  # mebi.
Lbs = (1 << 9)  # logical block size

########################################
# Utility functions.
########################################


def make_dir(pathStr):
    if not os.path.exists(pathStr):
        os.makedirs(pathStr)


def to_str(ss):
    return " ".join(ss)


def run_command(args, putMsg=True):
    '''
    run a command.
    args :: [str] - command line arguments.
                    The head item must be full-path executable.
    putMsg :: bool - put debug message.
    return :: str  - standard output of the command.
    '''
    verify_list_type(args, str)
    verify_type(putMsg, bool)

    if putMsg:
        print "run_command:", to_str(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                         stderr=sys.stderr, close_fds=True)
    f = p.stdout
    s = f.read().strip()
    ret = p.wait()
    if ret != 0:
        raise Exception("command error %d\n" % ret)
    if putMsg:
        print "run_command_result:", s
    return s


def run_daemon(args):
    '''
    Run a daemon.
    args :: [str] - command line arguments for daemon.
    '''
    try:
        pid = os.fork()
        if pid > 0:
            # parent waits for child's suicide
            os.waitpid(pid, 0)
            return
    except OSError, e:
        print >>sys.stderr, "fork#1 failed (%d) (%s)" % (e.errno, e.strerror)
        raise

    # child
    os.chdir("/")
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            # child exits immediately
            os._exit(0)
    except OSError, e:
        print >>sys.stderr, "fork#2 failed (%d) (%s)" % (e.errno, e.strerror)
        os._exit(1)

    # grandchild
    sys.stdin = open('/dev/null', 'r')
    sys.stdout = open('/dev/null', 'w')
    sys.stderr = open('/dev/null', 'w')

    subprocess.Popen(args, close_fds=True).wait()
    os._exit(0)


def wait_for_server_port(address, port, timeoutS=10):
    '''
    Wait for server port accepts connections.
    address :: str  - address or host name.
    port :: int     - port number.
    timeoutS :: int - timeout [sec].
    '''
    verify_type(address, str)
    verify_type(port, int)
    verify_type(timeoutS, int)
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        try:
            sock.connect((address, port))
            sock.close()
            return
        except socket.error, e:
            if e.errno not in [errno.ECONNREFUSED,
                               errno.ECONNABORTED, errno.ECONNRESET]:
                raise
            print 'wait_for_server_port:ignored', \
                address, port, e.errno, os.strerror(e.errno)
        time.sleep(0.3)
    raise Exception('wait_for_server_port:timeout', address, port, timeoutS)


def get_sha1(bdevPath, runCommand=run_command):
    '''
    Get sha1sum of a block device by full scan.
    bdevPath :: str - block device path.
    runCommand :: RunCommand
    return :: str  - sha1sum string.
    '''
    verify_type(bdevPath, str)
    verify_function(runCommand)
    ret = runCommand(['/usr/bin/sha1sum', bdevPath])
    return ret.split(' ')[0]


def flush_bufs(bdevPath, runCommand=run_command):
    '''
    Flush buffer of a block device.
    bdevPath :: str - block device path.
    runCommand :: RunCommand
    '''
    verify_type(bdevPath, str)
    verify_function(runCommand)
    runCommand(['/sbin/blockdev', '--flushbufs', bdevPath])


def zero_clear(bdevPath, offsetLb, sizeLb):
    '''
    Zero-clear a block device.
    bdevPath :: str - block device path.
    offsetLb :: int - offset [logical block].
    sizeLb :: innt  - size [logical block].
    '''
    verify_type(bdevPath, str)
    verify_type(offsetLb, int)
    verify_type(sizeLb, int)
    run_command(['/bin/dd', 'if=/dev/zero', 'of=' + bdevPath,
                 # 'oflag=direct',
                 'bs=512', 'seek=' + str(offsetLb), 'count=' + str(sizeLb)])
    # flush data to avoid overwriting after walb's writing
    fd = os.open(bdevPath, os.O_RDONLY)
    os.fdatasync(fd)
    os.close(fd)


def verify_equal_sha1(msg, md0, md1):
    '''
    Verify two sha1sum equals.
    msg :: str - message for error.
    md0 :: str - sha1sum
    md1 :: str - sha1sum
    '''
    verify_type(msg, str)
    verify_type(md0, str)
    verify_type(md1, str)
    if md0 == md1:
        print msg + ' ok :', md0
    else:
        raise Exception('fail ' + msg, md0, md1)


########################################
# Lvm utility functions.
########################################


def resize_lv(lvPath, curSizeMb, newSizeMb, doZeroClear):
    """
    Resize a logical volume.
      This command support shrink also.
    lvPath :: str       - lvm lv path.
    curSizeMb :: int    - current device size [MiB].
    newSizeMb :: int    - new device size [MiB].
    doZeroClear :: bool - True to zero-clear the extended area.
    """
    verify_type(lvPath, str)
    verify_type(curSizeMb, int)
    verify_type(newSizeMb, int)
    verify_type(doZeroClear, bool)

    if curSizeMb == newSizeMb:
        return
    run_command(['/sbin/lvresize', '-f', '-L', str(newSizeMb) + 'm', lvPath])
    # zero-clear is required for test only.
    if curSizeMb < newSizeMb and doZeroClear:
        zero_clear(lvPath, curSizeMb * 1024 * 1024 / 512,
                   (newSizeMb - curSizeMb) * 1024 * 1024 / 512)
    wait_for_lv_ready(lvPath)


def remove_lv(lvPath):
    '''
    Remove a logical volume.
    lvPath :: str - lvm lv path.
    '''
    wait_for_lv_ready(lvPath)
    for i in xrange(3):
        try:
            run_command(['/sbin/lvremove', '-f', lvPath])
            return
        except:
            print 'remove_lv failed', i, lvPath
            time.sleep(1)
    else:
        raise Exception('remove_lv:timeout', lvPath)


def get_lv_size(lvPath, runCommand=run_command):
    '''
    Get lv size.
    lvPath :: str - lvm lv path.
    runCommand :: RunCommand
    return :: device size [byte].
    '''
    ret = runCommand(['/sbin/lvdisplay', '-C', '--noheadings',
                      '-o', 'lv_size', '--units', 'b', lvPath])
    ret.strip()
    if ret[-1] != 'B':
        raise Exception('get_lv_size_mb: bad return value', ret)
    return int(ret[0:-1])


def get_lv_size_mb(lvPath, runCommand=run_command):
    '''
    Get lv size.
    lvPath :: str - lvm lv path.
    runCommand :: RunCommand
    return :: device size [MiB].
    '''
    sizeB = get_lv_size(lvPath, runCommand)
    if sizeB % Mebi != 0:
        raise Exception('get_lv_size_mb: not multiple of 1MiB.', sizeB)
    return sizeB / Mebi


def wait_for_lv_ready(lvPath, runCommand=run_command):
    '''
    lvPath :: str            - lvm device path.
    runCommand :: RunCommand
    '''
    flush_bufs(lvPath, runCommand)


########################################
# Verification functions.
########################################


def verify_type(obj, typeValue):
    '''
    obj - object.
    typeValue - type like int, str, list.

    '''
    if type(obj) != typeValue:
        raise 'invalid type', type(obj), typeValue


def verify_function(obj):
    '''
    obj - function object.

    '''
    def f():
        pass
    if type(obj) != type(f):
        raise 'not function type', type(obj)


def verify_list_type(obj, typeValue):
    '''
    obj - list object.
    typeValue - type like int, str.
    '''
    if type(obj) != list:
        raise 'invalid type', type(obj), list
    for x in obj:
        if type(x) != typeValue:
            raise 'invalid type', type(x), typeValue


def verify_gid_range(gidB, gidE, msg):
    '''
    gidB - begin gid.
    gidE - end gid.
    msg :: str - message for error.
    '''
    verify_type(gidB, int)
    verify_type(gidE, int)
    if gidB > gidE:
        raise Exception(msg, 'bad gid range', gidB, gidE)
