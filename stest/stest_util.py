#!/usr/bin/env python

import sys
sys.path.insert(0, './python/walb')
from walb import *

import socket
from contextlib import closing
import errno


def make_dir(pathStr):
    if not os.path.exists(pathStr):
        os.makedirs(pathStr)


def run_daemon(args, outPath=None):
    '''
    Run a daemon.
    args :: [str] - command line arguments for daemon.
    outPath :: str - stdout/stderr output path. None means blackhole.
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
    sys.stdout.flush()
    sys.stderr.flush()
    stdin = open('/dev/null', 'r')
    os.dup2(stdin.fileno(), sys.stdin.fileno())
    if outPath is None:
        outPath = '/dev/null'
    stdout = open(outPath, 'a+')
    stderr = open(outPath, 'a+')
    os.dup2(stdout.fileno(), sys.stdout.fileno())
    os.dup2(stderr.fileno(), sys.stderr.fileno())

    subprocess.Popen(args, close_fds=True)  # no need to wait for the subprocess.
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
        with closing(sock):
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


def flush_bufs(bdevPath, runCommand=run_local_command):
    '''
    Flush buffer of a block device.
    bdevPath :: str - block device path.
    runCommand :: RunCommand
    '''
    verify_type(bdevPath, str)
    verify_function(runCommand)
    runCommand(['/sbin/blockdev', '--flushbufs', bdevPath])


def zero_clear(bdevPath, offsetLb, sizeLb, runCommand=run_local_command):
    '''
    Zero-clear a block device.
    bdevPath :: str - block device path.
    offsetLb :: int - offset [logical block].
    sizeLb :: int  - size [logical block].
    runCommand :: RunCommand
    '''
    verify_type(bdevPath, str)
    verify_u64(offsetLb)
    verify_u64(sizeLb)
    runCommand(['/bin/dd', 'if=/dev/zero', 'of=' + bdevPath,
                'bs=512', 'seek=' + str(offsetLb), 'count=' + str(sizeLb),
                'conv=fdatasync'])


########################################
# Lvm utility functions.
########################################

def get_lv_size(lvPath, runCommand=run_local_command):
    '''
    Get lv size.
    lvPath :: str - lvm lv path.
    runCommand :: RunCommand
    return :: device size [byte].
    '''
    verify_type(lvPath, str)
    verify_function(runCommand)

    ret = runCommand(['/sbin/lvdisplay', '-C', '--noheadings', '-o', 'lv_size', '--units', 'b', lvPath])
    ret.strip()
    if ret[-1] != 'B':
        raise Exception('get_lv_size_mb: bad return value', ret)
    return int(ret[0:-1])


def get_lv_size_mb(lvPath, runCommand=run_local_command):
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


def resize_lv(lvPath, curSizeMb, newSizeMb, doZeroClear,
              runCommand=run_local_command):
    """
    Resize a logical volume.
      This command support shrink also.
    lvPath :: str       - lvm lv path.
    curSizeMb :: int    - current device size [MiB].
    newSizeMb :: int    - new device size [MiB].
    doZeroClear :: bool - True to zero-clear the extended area.
    runCommand :: RunCommand
    """
    verify_type(lvPath, str)
    verify_u64(curSizeMb)
    verify_u64(newSizeMb)
    verify_type(doZeroClear, bool)
    verify_function(runCommand)

    if curSizeMb == newSizeMb:
        return
    runCommand(['/sbin/lvresize', '-f', '-L', str(newSizeMb) + 'm', lvPath])
    # zero-clear is required for test only.
    if curSizeMb < newSizeMb and doZeroClear:
        zero_clear(lvPath, curSizeMb * 1024 * 1024 / 512,
                   (newSizeMb - curSizeMb) * 1024 * 1024 / 512,
                   runCommand)
    wait_for_lv_ready(lvPath, runCommand)


def remove_lv(lvPath, runCommand=run_local_command):
    '''
    Remove a logical volume.
    runCommand :: RunCommand
    lvPath :: str - lvm lv path.
    '''
    wait_for_lv_ready(lvPath, runCommand)
    retryTimes = 3
    for i in xrange(retryTimes):
        try:
            if i != 0 and not exists_file(lvPath, runCommand):
                return
            runCommand(['/sbin/lvremove', '-f', lvPath])
            return
        except:
            print 'remove_lv failed', i, lvPath
            time.sleep(1)
    else:
        raise Exception('remove_lv:timeout', lvPath)


def wait_for_lv_ready(lvPath, runCommand=run_local_command):
    '''
    lvPath :: str            - lvm device path.
    runCommand :: RunCommand
    '''
    flush_bufs(lvPath, runCommand)
