#!/usr/bin/env python

import time, shutil, threading, os, signal
from repeater import *

from stest_util import *

'''
These variables are defined for pylint.
Call set_config() to set them correctly.
These must be read-only.
'''
binDir = None # str
workDir = None # str
use_thinp = lambda: None # None -> None
isDebug = None # bool
TIMEOUT = None # int
wdevSizeMb = None # int
wdevL = None # [str]
sLayout = None # [Server]
sLayoutAll = None # [Server]
walbc = None # Controller
maxFgTasks = None # int
maxBgTasks = None # int


def set_config(toSymbolTbl, fromSymbolTbl):
    '''
    toSymbolTbl :: dict - symbols will be aded to this table.
    fromSymbolTbl :: dict - configuration (symbol table)
    '''
    for name, symbol in fromSymbolTbl.iteritems():
        if name in ['__builtins__', '__name__', '__doc__', '__package__']:
            continue
        toSymbolTbl[name] = symbol
        globals()[name] = symbol # this module also requires the symbols.


def setup_test(useTp):
    if useTp:
        use_thinp()
    kill_all_repeaters()
    kill_all_servers()
    run_local_command(['/bin/rm', '-rf', workDir])
    for ax in sLayoutAll.archiveL:
        if ax.vg:
            vgPath = '/dev/' + ax.vg + '/'
            if os.path.isdir(vgPath):
                for f in os.listdir(vgPath):
                    if f[0] == 'i':
                        run_local_command(['/sbin/lvremove', '-f', vgPath + f])
    make_dir(workDir)
    for wdev in wdevL:
        recreate_walb_dev(wdev)


def wait_for_server_ready(s, timeoutS=10):
    verify_type(s, Server)
    verify_type(timeoutS, int)
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        try:
            walbc.get_host_type(s)
            return
        except Exception, e:
            print 'retry, ', e
        time.sleep(0.3)
    raise Exception('wait_for_server_ready:timeout', s, timeoutS)


def get_orig_port(port):
    return port + 10000


def get_cmd_port(port):
    return port + 20000


def run_repeater(port, rateMbps=0, delayMsec=0):
    """
        run repeater
        port         ; ip to receive packets
        port + 10000 ; ip to send packets (original server)
        port + 20000 ; ip to recieve command
    """
    startup_repeater('localhost', get_orig_port(port), port, get_cmd_port(port),
                     rateMbps=rateMbps, delayMsec=delayMsec,
                     logPath=workDir + 'repeater.log',
                     outPath=workDir + 'repeater.out', isDebug=True)


def quit_repeater(s, doWait=True):
    send_cmd_to_repeater(get_cmd_port(s.port), 'quit')
    if doWait:
        wait_for_process_killed_on_port(s.port)


def quit_repeaters(sL):
    for s in sL:
        quit_repeater(s, doWait=False)
    for s in sL:
        wait_for_process_killed_on_port(s.port)


def start_repeater(s):
    send_cmd_to_repeater(get_cmd_port(s.port), 'start')


def stop_repeater(s):
    send_cmd_to_repeater(get_cmd_port(s.port), 'stop')


def startup(s, useRepeater=False, rateMbps=0, delayMsec=0, wait=True):
    wait_for_process_killed_on_port(s.port)
    make_dir(workDir + s.name)
    args = get_server_args(s, sLayout, isDebug=isDebug, useRepeater=useRepeater,
                           maxFgTasks=maxFgTasks, maxBgTasks=maxBgTasks)
    if isDebug:
        print 'cmd=', to_str(args)
    if useRepeater:
        run_repeater(s.port, rateMbps, delayMsec)
        wait_for_process_killed_on_port(get_orig_port(s.port))
    run_daemon(args, outPath=workDir + s.name + '.out')
    if wait:
        wait_for_server_ready(s)


def startup_list(sL, rL=None, rateMbps=0, delayMsec=0):
    '''
    sL :: [Server] - server list to start up.
    rL :: [Server] - server list to run repeater with.
    rateMbps :: int
    delayMsec :: int

    '''
    archiveL = [x for x in sL if x.kind == K_ARCHIVE]
    proxyL = [x for x in sL if x.kind == K_PROXY]
    storageL = [x for x in sL if x.kind == K_STORAGE]
    sL = archiveL + proxyL + storageL  # the order is important.
    if rL is None:
        rL = []
    for s in sL:
        startup(s, s in rL, rateMbps, delayMsec, wait=False)
    for s in sL:
        wait_for_server_ready(s)


def startup_all():
    startup_list(sLayout.get_all())


def get_pid(s):
    verify_type(s, Server)
    return int(walbc.run_ctl(s, ['get', 'pid']))


def wait_for_process_done(pid, timeoutS=10):
    verify_type(pid, int)
    verify_type(timeoutS, int)
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        try:
            s = run_local_command(['/bin/ps', '--no-headers', '-p', str(pid)])
            assert len(s) > 0
        except Exception, e:
            # not found the process
            print 'wait_for_process_done: ', e
            return
        time.sleep(1)
    raise Exception('wait_for_process_done: timeout', pid, timeoutS)


def shutdown(s, mode='graceful'):
    pid = get_pid(s)
    walbc.shutdown(s, mode)
    wait_for_process_done(pid)


def shutdown_list(sL, mode='graceful'):
    pidL = []
    for s in sL:
        pidL.append(get_pid(s))
    walbc.shutdown_list(sL, mode)
    for pid in pidL:
        wait_for_process_done(pid)


def shutdown_all(mode='graceful'):
    shutdown_list(walbc.sLayout.get_all(), mode)


def resize_storage_if_necessary(sx, vol, wdev, sizeMb):
    """
    assume init_storage() has been called before this.
    """
    if wdev.get_size_mb() >= sizeMb:
        return
    resize_lv(wdev.ddev, get_lv_size_mb(wdev.ddev), sizeMb, False)
    walbc.resize_storage(sx, vol, sizeMb)


def remove_persistent_data(s):
    '''
    Remove persistent data for scenario test.
    call shutdown() before calling this.
    s :: Server
    '''
    shutil.rmtree(workDir + s.name)
    if s in sLayoutAll.archiveL:
        for f in os.listdir('/dev/' + s.vg):
            if f[0:2] == 'i_':
                remove_lv('/dev/' + s.vg + '/' + f)


def write_random(bdevPath, sizeLb, offset=0, fixVar=None):
    '''
    Write random data.
    bdevPath :: str - block device path.
    sizeLb :: int   - written size [logical block].
    offset :: int   - start offset to write [logical block].
    fixVar :: int   - 0 to 255 or None.
    '''
    verify_type(bdevPath, str)
    verify_type(sizeLb, int)
    verify_type(offset, int)
    if fixVar is not None:
        verify_type(fixVar, int)
    args = [binDir + "/write_random_data",
            '-s', str(sizeLb), '-o', str(offset), bdevPath]
    if fixVar:
        args += ['-set', str(fixVar)]
    return run_local_command(args, False)


class RandomWriter(object):
    '''
    Random writer using a thread.

    '''
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


def kill_all_servers():
    for s in ['walb-storage', 'walb-proxy', 'walb-archive']:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()
    time.sleep(0.3)


def kill_all_repeaters():
    subprocess.Popen(['/usr/bin/killall', '-9', 'packet-repeater']).wait()
    time.sleep(0.3)


def write_over_wldev(wdev, overflow=False):
    '''
    Write to a walb device with size larger then log device.
    wdev :: Wdev     - walb device.
    overflow :: bool - True if you want to overflow the device.
    '''
    verify_type(wdev, Device)
    verify_type(overflow, bool)
    wldevSizeLb = get_lv_size(wdev.ldev) / Lbs
    wdevSizeLb = wdev.get_size_lb()
    print "wldevSizeLb, wdevSizeLb", wldevSizeLb, wdevSizeLb
    # write a little bigger size than wldevSizeLb
    remainLb = wldevSizeLb + 4
    writeMaxLb = min(wldevSizeLb, wdevSizeLb) / 2
    while remainLb > 0:
        writeLb = min(remainLb, writeMaxLb)
        print 'writing %d MiB' % (writeLb * Lbs / Mebi)
        write_random(wdev.path, writeLb)
        if not overflow:
            wdev.wait_for_log_empty()
        remainLb -= writeLb


def recreate_walb_dev(wdev):
    '''
    Reset walb device.
    wdev :: Device - walb device.
    '''
    verify_type(wdev, Device)
    if wdev.exists():
        wdev.delete()
    for path in [wdev.ldev, wdev.ddev]:
        if not os.path.exists(path):
            print '%s does not exist.' % path
            exit(1)
    resize_lv(wdev.ddev, get_lv_size_mb(wdev.ddev), wdevSizeMb, False)
    wdev.format_ldev()
    wdev.create()


def cleanup(vol, wdevL):
    '''
    Cleanup a volume.
    vol :: str             - volume name.
    wdevL :: [walb.Device] - list of walb devices.
    '''
    verify_type(vol, str)
    verify_type(wdevL, list, Device)
    for s in sLayout.get_all():
        walbc.clear(s, vol)
    for wdev in wdevL:
        recreate_walb_dev(wdev)


def get_sha1(bdevPath):
    '''
    Get sha1sum of a block device by full scan.
    bdevPath :: str - block device path.
    return :: str  - sha1sum string.
    '''
    verify_type(bdevPath, str)
    ret = run_local_command(['/usr/bin/sha1sum', bdevPath])
    return ret.split(' ')[0]


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


def get_sha1_of_restorable(ax, vol, gid):
    '''
    Get sha1sum of restorable snapshot.
    This is for test.
    ax :: Server  - archive server.
    vol :: str    - volume name.
    gid :: int    - generation id.
    return :: str - sha1sum.
    '''
    verify_type(ax, Server)
    verify_type(vol, str)
    verify_type(gid, int)
    walbc.restore(ax, vol, gid, TIMEOUT)
    md = get_sha1(get_restored_path(ax, vol, gid))
    walbc.del_restored(ax, vol, gid)
    return md


def restore_and_verify_sha1(msg, md0, ax, vol, gid):
    '''
    Restore a volume and verify sha1sum.
    This is for test.
    msg :: str   - message for error.
    md0 :: str   - sha1sum.
    ax :: Server - archive server.
    vol :: str   - volume name.
    gid :: int   - generation id.
    '''
    verify_type(msg, str)
    verify_type(md0, str)
    verify_type(ax, Server)
    verify_type(vol, str)
    verify_type(gid, int)
    md1 = get_sha1_of_restorable(ax, vol, gid)
    verify_equal_sha1(msg, md0, md1)


def verify_equal_restorable_list(msg, ax, ay, vol):
    '''
    Verify two restorable lists are the same.
    msg :: str   - message for error.
    ax :: Server - first archive server.
    ay :: Server - second archive server.
    vol :: str   - volume name.
    '''
    verify_type(msg, str)
    verify_type(ax, Server)
    verify_type(ay, Server)
    verify_type(vol, str)

    xL = walbc.get_restorable_gid(ax, vol)
    yL = walbc.get_restorable_gid(ay, vol)
    if isDebug:
        print 'list0', xL
        print 'list1', yL
    if xL != yL:
        raise Exception(msg, 'restorable lists differ', xL, yL)


def wait_for_process_killed_on_port(port, timeoutS=10):
    verify_type(port, int)
    verify_type(timeoutS, int)
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        try:
            s = run_local_command(['/usr/bin/lsof', '-i', 'TCP:%d' % port], putMsg=True)
            assert len(s.splitlines()) >= 2
        except:
            return
        time.sleep(1)
    raise Exception('wait_for_process_killed_on_port: timeout', port, timeoutS)


def print_action_info(action, info):
    '''
    action :: str
    info :: str
    '''
    verify_type(action, str)
    verify_type(info, str)
    print '++++++++++++++++++++++++++++++++++++++++', action, info
