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

TIMEOUT_SEC = 100
cfg = None


def set_config(config):
    if config.binDir[0] != '/' or config.binDir[-1] != '/':
        raise Exception("binDir must abs path", config.binDir)
    if config.dataDir[0] != '/' or config.dataDir[-1] != '/':
        raise Exception("dataDir must abs path", config.dataDir)
    global cfg
    cfg = config


def get_config():
    return cfg


def make_dir(pathStr):
    if not os.path.exists(pathStr):
        os.makedirs(pathStr)


def to_str(ss):
    return " ".join(ss)


def get_debug_opt():
    if cfg.debug:
        return ["-debug"]
    else:
        return []


def get_host_port(s):
    return "localhost" + ":" + s.port


def run_command(args, putMsg=True):
    '''
    run a command.
    args :: [str] - command line arguments.
    putMsg :: bool - put debug message.
    return :: str  - standard output of the command.
    '''
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


def wait_for_server_port(s, timeoutS=10):
    '''
    Wait for server port accepts connections.
    s :: Server
    timeoutS :: int - timeout [sec].
    '''
    address = "localhost"
    port = int(s.port)
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        try:
            sock.connect((address, port))
            sock.close()
            return
        except socket.error, e:
            if e.errno not in [errno.ECONNREFUSED, errno.ECONNABORTED, errno.ECONNRESET]:
                raise
            print 'wait_for_server_port:ignored', s, e.errno, os.strerror(e.errno)
        time.sleep(0.3)
    raise Exception('wait_for_server_port:timeout', s)


def kill_all_servers():
    '''
    for scenario test.
    '''
    for s in ["storage-server", "proxy-server", "archive-server"]:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()
    time.sleep(0.3)


def remove_persistent_data(s):
    '''
    for scenario test.
    call shutdown() before calling this.
    '''
    shutil.rmtree(cfg.dataDir + s.name)
    if s in cfg.archiveL:
        for f in os.listdir('/dev/' + s.vg):
            if f[0:2] == 'i_':
                remove_lv('/dev/' + s.vg + '/' + f)


def verify_shutdown_mode(mode, msg):
    if mode not in ['graceful', 'force']:
        raise Exception(msg, 'bad mode', mode)


def write_random(bdevPath, sizeLb, offset=0, fixVar=None):
    '''
    for scenario test.
    '''
    args = [cfg.binDir + "/write_random_data",
        '-s', str(sizeLb), '-o', str(offset), bdevPath]
    if fixVar:
        args += ['-set', str(fixVar)]
    return run_command(args, False)


def get_sha1(bdevPath):
    '''
    Get sha1sum of a block device by full scan.
    bdevPath :: str - block device path.
    return :: str  - sha1sum string.
    '''
    verify_type(bdevPath, str)
    ret = run_command(['/usr/bin/sha1sum', bdevPath])
    return ret.split(' ')[0]


def flush_bufs(bdevPath):
    '''
    Flush buffer of a block device.
    bdevPath :: str - block device path.
    '''
    verify_type(bdevPath, str)
    run_command(['/sbin/blockdev', '--flushbufs', bdevPath])


def wait_for_lv_ready(lvPath, timeoutS=TIMEOUT_SEC):
    '''
    lvPath :: str   - lvm device path.
    '''
    flush_bufs(lvPath)


def verify_equal_sha1(msg, md0, md1):
    '''
    Verify two sha1sum equals.
    msg :: str - message for error.
    md0 :: str - sha1sum
    md1 :: str - sha1sum
    '''
    if md0 == md1:
        print msg + ' ok :', md0
    else:
        raise Exception('fail ' + msg, md0, md1)


class RandomWriter():
    def __init__(self, path):
        self.path = path
    def writing(self):
        while not self.quit:
            write_random(self.path, 1)
            time.sleep(0.05)
    def __enter__(self):
        self.quit = False
        self.th = threading.Thread(target=self.writing)
        self.th.setDaemon(True)
        self.th.start()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.quit = True
        self.th.join()


def zero_clear(bdevPath, offsetLb, sizeLb):
    '''
    Zero-clear a block device.
    bdevPath :: str - block device path.
    offsetLb :: int - offset [logical block].
    sizeLb :: innt  - size [logical block].
    '''
    run_command(['/bin/dd', 'if=/dev/zero', 'of=' + bdevPath, # 'oflag=direct',
                 'bs=512', 'seek=' + str(offsetLb), 'count=' + str(sizeLb)])
    # flush data to avoid overwriting after walb's writing
    fd = os.open(bdevPath, os.O_RDONLY)
    os.fdatasync(fd)
    os.close(fd)


def resize_lv(lvPath, beforeSizeMb, afterSizeMb, doZeroClear):
    """
    Resize a logical volume.
      This command support shrink also.
    lvPath :: str       - lvm lv path.
    beforeSizeMb :: int - current device size [MiB].
    afterSizeMb :: int  - new device size [MiB].
    doZeroClear :: bool - True to zero-clear the extended area.
    """
    if beforeSizeMb == afterSizeMb:
        return
    run_command(['/sbin/lvresize', '-f', '-L', str(afterSizeMb) + 'm', lvPath])
    # zero-clear is required for test only.
    if beforeSizeMb < afterSizeMb and doZeroClear:
        zero_clear(lvPath, beforeSizeMb * 1024 * 1024 / 512,
                   (afterSizeMb - beforeSizeMb) * 1024 * 1024 / 512)
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


def get_lv_size_mb(lvPath):
    '''
    Get lv size.
    lvPath :: str - lvm lv path.
    return :: device size [MiB].
    '''
    ret = run_command(['/sbin/lvdisplay', '-C', '--noheadings',
                       '-o', 'lv_size', '--units', 'b', lvPath])
    ret.strip()
    if ret[-1] != 'B':
        raise Exception('get_lv_size_mb: bad return value', ret)
    return int(ret[0:-1]) / 1024 / 1024


def verify_type(obj, typeValue):
    '''
    obj - object.
    typeValue - type like int, str, list.

    '''
    if type(obj) != typeValue:
        raise 'invalid type', type(obj), typeValue


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
