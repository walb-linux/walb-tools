#!/usr/bin/env python

import os
import itertools, shutil, threading
import random
import datetime
from walb import *
from repeater import *

isDebug = True

workDir = os.getcwd() + '/stest/tmp/'
binDir = os.getcwd() + '/binsrc/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'


def D(name):
    return workDir + name


def L(name):
    return workDir + name + '.log'


s0 = Server('s0', 'localhost', 10000, K_STORAGE, binDir, D('s0'), L('s0'))
s1 = Server('s1', 'localhost', 10001, K_STORAGE, binDir, D('s1'), L('s1'))
s2 = Server('s2', 'localhost', 10002, K_STORAGE, binDir, D('s2'), L('s2'))
p0 = Server('p0', 'localhost', 10100, K_PROXY,   binDir, D('p0'), L('p0'))
p1 = Server('p1', 'localhost', 10101, K_PROXY,   binDir, D('p1'), L('p1'))
p2 = Server('p2', 'localhost', 10102, K_PROXY,   binDir, D('p2'), L('p2'))
a0 = Server('a0', 'localhost', 10200, K_ARCHIVE, binDir, D('a0'), L('a0'), 'vg0')
a1 = Server('a1', 'localhost', 10201, K_ARCHIVE, binDir, D('a1'), L('a1'), 'vg1')
a2 = Server('a2', 'localhost', 10202, K_ARCHIVE, binDir, D('a2'), L('a2'), 'vg2')

VOL = 'vol0'
wdevSizeMb = 12

sLayout = ServerLayout([s0, s1], [p0, p1], [a0, a1])
sLayoutAll = ServerLayout([s0, s1, s2], [p0, p1, p2], [a0, a1, a2])
sLayoutRepeater = ServerLayout([s0], [p0], [a0])

wdev0 = Device(0, '/dev/test/log',  '/dev/test/data',  wdevcPath)
wdev1 = Device(1, '/dev/test/log2', '/dev/test/data2', wdevcPath)
wdev2 = Device(2, '/dev/test/log3', '/dev/test/data3', wdevcPath)
wdevL = [wdev0, wdev1, wdev2]

walbc = Controller(walbcPath, sLayout, isDebug)

g_count = 0


def setup_test():
    run_local_command(['/bin/rm', '-rf', workDir])
    for ax in sLayoutAll.archiveL:
        if ax.vg:
            vgPath = '/dev/' + ax.vg + '/'
            if os.path.isdir(vgPath):
                for f in os.listdir(vgPath):
                    if f[0] == 'i':
                        run_local_command(['/sbin/lvremove', '-f', vgPath + f])
    make_dir(workDir)
    kill_all_servers()
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


def get_cmd_port(port):
    return port + 20000

def run_repeater(port, rateMbps=10, delayMsec=0):
    """
        run repeater
        port         ; ip to receive packets
        port + 10000 ; ip to send packets (original server)
        port + 20000 ; ip to recieve command
    """
    startup_repeater('localhost', port + 10000, port, get_cmd_port(port), rateMbps=rateMbps, delayMsec=delayMsec, isDebug=False)


def quit_repeater(s, doSleep=True):
    send_cmd_to_repeater(get_cmd_port(s.port), 'quit')
    time.sleep(1)


def quit_repeaters(sL):
    for s in sL:
        quit_repeater(s, doSleep=False)
    time.sleep(1)


def start_repeater(s):
    send_cmd_to_repeater(get_cmd_port(s.port), 'start')


def stop_repeater(s):
    send_cmd_to_repeater(get_cmd_port(s.port), 'stop')


def startup(s, useRepeater=False, delayMsec=0, rateMbps=0):
    make_dir(workDir + s.name)
    args = get_server_args(s, sLayout, useRepeater=useRepeater)
    if isDebug:
        print 'cmd=', to_str(args)
    if useRepeater:
        run_repeater(s.port, rateMbps, delayMsec)
    run_daemon(args)
    wait_for_server_ready(s)


def kill_all_repeaters():
    subprocess.Popen(['/usr/bin/pkill', '-9', 'packet-repeater'])
    time.sleep(0.3)


def startup_list(sL):
    for s in sL:
        startup(s)


def startup_all():
    kill_all_repeaters()
    startup_list(sLayout.get_all())


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


class RandomWriter():
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
    '''
    for scenario test.
    '''
    for s in ['walb-storage', 'walb-proxy', 'walb-archive']:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()
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
        print 'writing %d MiB' % (writeLb * 512 / 1024 / 1024)
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
    verify_list_type(wdevL, Device)
    for s in sLayout.get_all():
        walbc.clear_vol(s, vol)
    for wdev in wdevL:
        recreate_walb_dev(wdev)


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
    walbc.restore(ax, vol, gid)
    md = get_sha1(walbc.get_restored_path(ax, vol, gid))
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


def verify_equal_list_restorable(msg, ax, ay, vol):
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

    xL = walbc.list_restorable(ax, vol)
    yL = walbc.list_restorable(ay, vol)
    if isDebug:
        print 'list0', xL
        print 'list1', yL
    if xL != yL:
        raise Exception(msg, 'list_restorable differ', xL, yL)


###############################################################################
# Normal scenario tests.
###############################################################################


def test_n1():
    """
        full-backup -> sha1 -> restore -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n1:full-backup", g_count
    walbc.init_storage(s0, VOL, wdev0.path)
    walbc.init_storage(s1, VOL, wdev1.path)
    write_random(wdev0.path, 1)
    md0 = get_sha1(wdev0.path)
    gid = walbc.full_backup(s0, VOL)
    restore_and_verify_sha1('test_n1', md0, a0, VOL, gid)
    print 'test_n1:succeeded'


def test_n2():
    """
        write -> sha1 -> snapshot -> restore -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n2:snapshot", g_count
    write_random(wdev0.path, 1)
    md0 = get_sha1(wdev0.path)
    gid = walbc.snapshot_sync(s0, VOL, [a0])
    print "gid=", gid
    print walbc.list_restorable(a0, VOL)
    restore_and_verify_sha1('test_n2', md0, a0, VOL, gid)
    print 'test_n2:succeeded'


def test_n3():
    """
        hash-backup -> sha1 -> restore -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n3:hash-backup", g_count
    walbc.set_slave_storage(s0, VOL)
    write_random(wdev0.path, 1)
    md0 = get_sha1(wdev0.path)
    gid = walbc.hash_backup(s0, VOL)
    print "gid=", gid
    restore_and_verify_sha1('test_n3', md0, a0, VOL, gid)
    print 'test_n3:succeeded'


def printL(aL, bL):
    print '[',
    for a in aL:
        print a.name,
    print '], [',
    for b in bL:
        print b.name,
    print ']'


def test_stop(stopL, startL):
    printL(stopL, startL)
    with RandomWriter(wdev0.path):
        for s in stopL:
            walbc.stop(s, VOL)
            time.sleep(0.1)

        time.sleep(0.5)

        for s in startL:
            walbc.start(s, VOL)
            time.sleep(0.1)

    md0 = get_sha1(wdev0.path)
    gid = walbc.snapshot_sync(s0, VOL, [a0])
    restore_and_verify_sha1('test_stop', md0, a0, VOL, gid)


def test_n4_detail(numPatterns=0):
    """
        stop -> start -> snapshot -> sha1
        if numPatterns == 0 then all possible patterns will be tested.
        otherwise, numPatterns patterns only will be tested.
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n4:stop", g_count
    perm = itertools.permutations
    chain = itertools.chain.from_iterable
    setLL = [[s0], [p0], [a0], [s0, p0], [s0, a0], [p0, a0], [s0, p0, a0]]
    combiL = [(xL, yL) for xL in chain(map(perm, setLL)) for yL in perm(xL)]
    print "combiL.len", len(combiL)
    if numPatterns > 0 and numPatterns < len(combiL):
        targetL = random.sample(combiL, numPatterns)
    else:
        targetL = combiL
    for stopL, startL in targetL:
        test_stop(stopL, startL)
#        printL(stopL, startL)
    print 'test_n4:succeeded'


def test_n4a():
    test_n4_detail()


def test_n4b():
    test_n4_detail(5)


def test_n5():
    """
        apply -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n5:apply", g_count
    with RandomWriter(wdev0.path):
        time.sleep(0.5)
        gid = walbc.snapshot_sync(s0, VOL, [a0])
        time.sleep(0.5)
    md0 = get_sha1_of_restorable(a0, VOL, gid)
    walbc.apply_diff(a0, VOL, gid)
    restore_and_verify_sha1('test_n5', md0, a0, VOL, gid)
    print 'test_n5:succeeded'


def test_n6():
    """
        merge -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n6:merge", g_count
    with RandomWriter(wdev0.path):
        time.sleep(0.5)
        gidB = walbc.snapshot_sync(s0, VOL, [a0])
        time.sleep(1)
        # create more than two diff files
        walbc.stop(s0, VOL)
        walbc.stop(p0, VOL, 'empty')
        walbc.start(s0, VOL)
        walbc.start(p0, VOL)
        time.sleep(1)
        gidE = walbc.snapshot_sync(s0, VOL, [a0])
        gidL = walbc.list_restorable(a0, VOL, 'all')
        posB = gidL.index(gidB)
        posE = gidL.index(gidE)
        print "gidB", gidB, "gidE", gidE, "gidL", gidL
        if posE - posB < 2:
            raise Exception('test_n6:bad range', gidB, gidE, gidL)
        time.sleep(0.5)
    # merge gidB and gidE

    md0 = get_sha1_of_restorable(a0, VOL, gidE)
    walbc.merge_diff(a0, VOL, gidB, gidE)
    print "merged gidL", walbc.list_restorable(a0, VOL, 'all')
    restore_and_verify_sha1('test_n6', md0, a0, VOL, gidE)
    print 'test_n6:succeeded'


def test_n7():
    """
        replicate (no synchronizing, full) -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_n7:replicate-full', g_count
    walbc.replicate(a0, VOL, a1, False)
    verify_equal_list_restorable('test_n7', a0, a1, VOL)
    gid = walbc.get_latest_clean_snapshot(a0, VOL)
    md0 = get_sha1_of_restorable(a0, VOL, gid)
    md1 = get_sha1_of_restorable(a1, VOL, gid)
    verify_equal_sha1('test_n7', md0, md1)
    print 'test_n7:succeeded'


def test_n8():
    """
        replicate (no synchronizing, diff) -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_n8:replicate-diff', g_count
    write_random(wdev0.path, 1)
    gid0 = walbc.snapshot_sync(s0, VOL, [a0])
    gidA0 = walbc.get_latest_clean_snapshot(a0, VOL)
    if gidA0 != gid0:
        raise Exception('test_n8:wrong gid', gidA0, gid0)
    gidA1 = walbc.get_latest_clean_snapshot(a1, VOL)
    if gidA0 <= gidA1:
        raise Exception('test_n8:no progress', gidA0, gidA1)
    walbc.replicate(a0, VOL, a1, False)
    verify_equal_list_restorable('test_n8', a0, a1, VOL)
    gid1 = walbc.get_latest_clean_snapshot(a0, VOL)
    md0 = get_sha1_of_restorable(a0, VOL, gid1)
    md1 = get_sha1_of_restorable(a1, VOL, gid1)
    verify_equal_sha1('test_n8', md0, md1)
    print 'test_n8:succeeded'


def test_n9():
    """
        replicate (no synchronizing, hash) -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_n9:replicate-hash', g_count
    write_random(wdev0.path, 1)
    gid0 = walbc.snapshot_sync(s0, VOL, [a0])
    walbc.apply_diff(a0, VOL, gid0)
    list0 = walbc.list_restorable(a0, VOL)
    if len(list0) != 1:
        raise Exception('test_n9: list size must be 1', list0)
    write_random(wdev0.path, 1)
    walbc.replicate(a0, VOL, a1, False)
    gid1a0 = walbc.get_latest_clean_snapshot(a0, VOL)
    gid1a1 = walbc.get_latest_clean_snapshot(a1, VOL)
    if gid1a0 != gid1a1:
        raise Exception('test_n9: gid differ', gid1a0, gid1a1)
    md0 = get_sha1_of_restorable(a0, VOL, gid1a0)
    md1 = get_sha1_of_restorable(a1, VOL, gid1a1)
    verify_equal_sha1('test_n9', md0, md1)
    print 'test_n9:succeeded'


def test_n10():
    """
        replicate (sychronizing) -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_n10:replicate-synchronizing', g_count
    with RandomWriter(wdev0.path):
        time.sleep(0.5)
        walbc.replicate(a0, VOL, a1, True)
        time.sleep(0.5)
        #gid0 = snapshot_sync(s0, VOL, [a0, a1])
        gid0 = walbc.snapshot_async(s0, VOL)
        walbc.wait_for_restorable(a0, VOL, gid0)
        walbc.wait_for_restorable(a1, VOL, gid0)
        md0 = get_sha1_of_restorable(a0, VOL, gid0)
        md1 = get_sha1_of_restorable(a1, VOL, gid0)
        verify_equal_sha1('test_n10', md0, md1)
        walbc.stop_sync(a1, VOL)
        time.sleep(0.5)
    gid1 = walbc.snapshot_sync(s0, VOL, [a0])
    gid1a1 = walbc.get_latest_clean_snapshot(a1, VOL)
    if gid1 <= gid1a1:
        raise Exception('test_n10: not stopped synchronizing', gid1, gid1a1)
    print 'test_n10:succeeded'


def test_n11(doZeroClear):
    """
        doZeroClear is true:
            resize -> snapshot -> sha1
        otherwise:
            resize -> hash backup -> sha1

    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_n11:resize', doZeroClear, g_count
    with RandomWriter(wdev0.path):
        prevSize = wdev0.get_size_mb()
        walbc.snapshot_sync(s0, VOL, [a0])
        # lvm extent size is 4MiB
        resize_lv(wdev0.ddev, prevSize, prevSize + 4, doZeroClear)
        resize_lv(wdev1.ddev, prevSize, prevSize + 4, doZeroClear)
        walbc.resize(VOL, prevSize + 4, doZeroClear)
        curSize = wdev0.get_size_mb()
        if curSize != prevSize + 4:
            raise Exception('test_n11:bad size', prevSize, curSize)
    write_random(wdev0.path, 1, prevSize * 1024 * 1024 / 512)
    if doZeroClear:
        gid = walbc.snapshot_sync(s0, VOL, [a0])
    else:
        walbc.set_slave_storage(s0, VOL)
        gid = walbc.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_n11', md0, md1)
    print 'test_n11:succeeded'


def test_n11a():
    test_n11(True)


def test_n11b():
    test_n11(False)


def test_n12():
    """
        change master to slave
        -> change slave to master (hash-sync)
        -> sha1
        once more.

    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_n12:exchange-master-slave', g_count
    with RandomWriter(wdev0.path):
        with RandomWriter(wdev1.path):
            time.sleep(0.3)
            walbc.set_slave_storage(s0, VOL)
            time.sleep(0.3)
    gid = walbc.hash_backup(s1, VOL)
    md0 = get_sha1(wdev1.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_n12', md0, md1)
    st0 = walbc.get_state(s0, VOL)
    if st0 != 'Slave':
        raise Exception('test_n12:s0:1:bad state', st0)
    st1 = walbc.get_state(s1, VOL)
    if st1 != 'Master':
        raise Exception('test_n12:s1:1:bad state', st1)

    walbc.set_slave_storage(s1, VOL)
    gid = walbc.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_n12', md0, md1)
    st0 = walbc.get_state(s0, VOL)
    if st0 != 'Master':
        raise Exception('test_n12:s0:2:bad state', st0)
    st1 = walbc.get_state(s1, VOL)
    if st1 != 'Slave':
        raise Exception('test_n12:s0:2:bad state', st1)
    print 'test_n12:succeeded'


###############################################################################
# Misoperation scenario tests.
###############################################################################


def test_m1():
    """
        full-bkp --> full-bkp fails.
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_m1:full-bkp-after-full-bkp-fails', g_count
    write_random(wdev0.path, 1)
    walbc.stop_sync(a0, VOL)
    try:
        walbc.full_backup(s0, VOL, 10)
    except:
        # expect to catch an exception.
        walbc.start_sync(a0, VOL)
        print 'test_m1:succeeded'
        return
    raise Exception('test_m1:full_backup did not fail')


def test_m2():
    """
        init --> hash-bkp fails.
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_m2:hash-bkp-fails.', g_count
    walbc.stop_sync(a0, VOL)
    walbc.stop(a0, VOL)
    walbc.reset_vol(a0, VOL)
    write_random(wdev0.path, 1)
    try:
        walbc.hash_backup(s0, VOL, 10)
    except:
        # expect to catch an exception.
        walbc.set_slave_storage(s0, VOL)
        walbc.full_backup(s0, VOL)
        print 'test_m2:succeeded'
        return
    raise Exception('test_m2:hash_backup did not fail')


def test_m3():
    """
        resize at storage -> write -> wdiff-transfer fails.
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_m3:resize-fails', g_count
    prevSizeMb = wdev0.get_size_mb()
    walbc.snapshot_sync(s0, VOL, [a0])
    newSizeMb = prevSizeMb + 4  # lvm extent size is 4MiB
    resize_lv(wdev0.ddev, prevSizeMb, newSizeMb, True)
    resize_lv(wdev1.ddev, prevSizeMb, newSizeMb, True)
    walbc.resize_storage(s0, VOL, newSizeMb)
    walbc.resize_storage(s1, VOL, newSizeMb)
    write_random(wdev0.path, 1, prevSizeMb * 1024 * 1024 / 512)
    curSizeMb = wdev0.get_size_mb()
    if curSizeMb != newSizeMb:
        raise Exception('test_m3:bad size', newSizeMb, curSizeMb)
    gid1 = walbc.snapshot_async(s0, VOL)
    walbc.verify_not_restorable(a0, VOL, gid1, 10, 'test_m3')
    if not walbc.is_wdiff_send_error(p0, VOL, a0):
        raise Exception('test_m3:must occur wdiff-send-error')
    walbc.resize_archive(a0, VOL, newSizeMb, True)
    walbc.kick_all_storage()
    walbc.kick_all(sLayout.proxyL)
    walbc.wait_for_restorable(a0, VOL, gid1)
    if walbc.is_wdiff_send_error(p0, VOL, a0):
        raise Exception('test_m3:must not occur wdiff-send-error')
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid1)
    verify_equal_sha1('test_m3', md0, md1)
    print 'test_m3:succeeded'


###############################################################################
# Error scenario tests.
###############################################################################


def test_e1():
    """
        p0 down -> write over wldev amount -> p0 up -> snapshot -> sha1

    """
    print '++++++++++++++++++++++++++++++++++++++ test_e1:proxy-down', g_count
    walbc.shutdown(p0, 'force')
    write_over_wldev(wdev0)
    walbc.verify_not_overflow(s0, VOL)
    startup(p0)
    walbc.kick_all_storage()
    gid = walbc.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e1', md0, md1)
    print 'test_e1:succeeded'


def test_e2():
    """
        a0 down -> write over wldev amount -> a0 up -> snapshot -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e2:archive-down', g_count
    walbc.shutdown(a0, 'force')
    write_over_wldev(wdev0)
    walbc.verify_not_overflow(s0, VOL)
    gid = walbc.snapshot_async(s0, VOL)
    walbc.verify_not_restorable(a0, VOL, gid, 10, 'test_e2')
    startup(a0)
    walbc.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e2', md0, md1)
    print 'test_e2:succeeded'


def test_e3():
    """
        s0 down -> write -> s0 up -> snapshot -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e3:storage-down', g_count
    walbc.shutdown(s0, 'force')
    write_random(wdev0.path, 1)
    startup(s0)
    gid = walbc.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e3', md0, md1)
    print 'test_e3:succeeded'


def test_e4():
    """
        s0 down -> write over wldev amount -> s0 up -> hash-backup-> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e4:storage-down-overflow', g_count
    walbc.shutdown(s0, 'force')
    write_over_wldev(wdev0, overflow=True)
    startup(s0)
    if not walbc.is_overflow(s0, VOL):
        raise Exception('test_e4:must be overflow')
    gid = walbc.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e4', md0, md1)
    print 'test_e4:succeeded'


def test_e5():
    """
        p0 data lost -> p0 up -> hash-backup -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e5:proxy-data-lost', g_count
    walbc.stop(a0, VOL, 'force')
    write_random(wdev0.path, 1)
    time.sleep(3)  # wait for the log is sent to p0.
    walbc.shutdown(p0, 'force')
    remove_persistent_data(p0)
    walbc.start(a0, VOL)
    startup(p0)
    walbc.start_sync(a0, VOL)
    write_random(wdev0.path, 1)
    gid0 = walbc.snapshot_async(s0, VOL)
    walbc.verify_not_restorable(a0, VOL, gid0, 10, 'test_e5')
    gid1 = walbc.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid1)
    verify_equal_sha1('test_e5', md0, md1)
    print 'test_e5:succeeded'


def test_e6():
    """
        a0 data lost -> a0 up -> full-backup -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e6:primary-archive-data-lost', g_count
    walbc.shutdown(a0, 'force')
    remove_persistent_data(a0)
    startup(a0)
    walbc.run_ctl(a0, ['init-vol', VOL])
    gid0 = walbc.snapshot_async(s0, VOL)
    walbc.verify_not_restorable(a0, VOL, gid0, 10, 'test_e6')
    gid1 = walbc.full_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid1)
    verify_equal_sha1('test_e6', md0, md1)
    print 'test_e6:succeeded'


def test_e7():
    """
        a1 data lost -> a1 up -> replicate -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e7:secondary-archive-data-lost', g_count
    write_random(wdev0.path, 1)
    walbc.replicate(a0, VOL, a1, True)
    walbc.shutdown(a1, 'force')
    remove_persistent_data(a1)
    startup(a1)
    write_random(wdev0.path, 1)
    gid0 = walbc.snapshot_async(s0, VOL)
    walbc.wait_for_restorable(a0, VOL, gid0)
    walbc.verify_not_restorable(a1, VOL, gid0, 10, 'test_e7')
    walbc.replicate(a0, VOL, a1, True)
    write_random(wdev0.path, 1)
    gid1 = walbc.snapshot_sync(s0, VOL, [a0, a1])
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid1)
    md2 = get_sha1_of_restorable(a1, VOL, gid1)
    verify_equal_sha1('test_e7:1', md0, md1)
    verify_equal_sha1('test_e7:2', md1, md2)
    walbc.stop_sync(a1, VOL)
    print 'test_e7:succeeded'


def test_e8():
    """
        s0 data lost -> s0 up -> hash-backup -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e8:storage-data-lost', g_count
    write_random(wdev0.path, 1)
    walbc.shutdown(s0, 'force')
    remove_persistent_data(s0)
    startup(s0)
    write_random(wdev0.path, 1)
    walbc.run_ctl(s0, ['init-vol', VOL, wdev0.path])
    write_random(wdev0.path, 1)
    gid = walbc.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e8', md0, md1)
    print 'test_e8:succeeded'


def init_repeater_test(rL=[], delayMsec=0, rateMbps=0):
    walbc.shutdown_all('force')
    target = sLayoutRepeater
    walbc.set_server_layout(target)
    for s in target.get_all():
        startup(s, s in rL, delayMsec, rateMbps)


def exit_repeater_test(rL):
    walbc.shutdown_all('force')
    quit_repeaters(rL)
    target = sLayout
    walbc.set_server_layout(target)
    for s in target.get_all():
        startup(s)


def test_e9():
    """
        down network between s0 and p0 -> recover -> synchronizing
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e9:network down and recover', g_count
    rL = [p0]
    init_repeater_test(rL)
    stop_repeater(p0)
    write_random(wdev0.path, 1)
    md0 = get_sha1(wdev0.path)
    gid = walbc.snapshot_async(s0, VOL)
    walbc.verify_not_restorable(a0, VOL, gid, 10, 'test_e9')
    start_repeater(p0)
    gid = walbc.snapshot_sync(s0, VOL, [a0])
    restore_and_verify_sha1('test_e9', md0, a0, VOL, gid)
    # recover default layout
    exit_repeater_test(rL)
    print 'test_e9:succeeded'


def test_e10():
    """
        down network between s0 and p0 -> write overflow -> hash-backup -> synchronizing
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_e10:network down and overflow', g_count
    rL = [p0]
    init_repeater_test(rL)
    stop_repeater(p0)
    write_over_wldev(wdev0, overflow=True)
    try:
        walbc.snapshot_async(s0, VOL)
        raise Exception('test_e10:expect wlog overflow')
    except:
        print 'test_e10:wlog overflow ok'
        pass
    start_repeater(p0)
    gid = walbc.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e10', md0, md1)
    # stop repeater
    exit_repeater_test(rL)
    print 'test_e10:succeeded'


###############################################################################
# Replacement scenario tests.
###############################################################################


def test_r1():
    """
        replace s0 by s2
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_r1:replace-storage', g_count
    startup(s2)
    walbc.init_storage(s2, VOL, wdev2.path)
    resize_storage_if_necessary(s2, VOL, wdev2, wdev0.get_size_mb())

    walbc.set_slave_storage(s0, VOL)
    write_random(wdev2.path, 1)
    gid = walbc.hash_backup(s2, VOL)
    md0 = get_sha1(wdev2.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r1:0', md0, md1)
    walbc.clear_vol(s0, VOL)

    # turn back to the beginning state.
    walbc.init_storage(s0, VOL, wdev0.path)
    walbc.set_slave_storage(s2, VOL)
    write_random(wdev0.path, 1)
    gid = walbc.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r1:1', md0, md1)

    walbc.clear_vol(s2, VOL)
    walbc.shutdown(s2)
    print 'test_r1:succeeded'


def replace_proxy(pDel, pAdd, volL, newServerLayout):
    """
        replace pDel by pAdd.
        Before calling this,
            pDel must be running.
            pAdd must be down.

    """
    startup(pAdd)
    for vol in volL:
        walbc.copy_archive_info(pDel, vol, pAdd)
        walbc.stop_async(pDel, vol, 'empty')
    for vol in volL:
        walbc.wait_for_stopped(pDel, vol)
        walbc.clear_vol(pDel, vol)
    walbc.shutdown(pDel)

    walbc.set_server_layout(newServerLayout)
    for sx in newServerLayout.storageL:
        walbc.shutdown(sx)
        startup(sx)


def test_r2():
    """
        replace p0 by p2
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_r2:replace-proxy', g_count
    sLayout2 = sLayout.replace(proxyL=[p2, p1])
    replace_proxy(p0, p2, [VOL], sLayout2)
    write_random(wdev0.path, 1)
    gid = walbc.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r2:0', md0, md1)

    # turn back to the beginning state.
    replace_proxy(p2, p0, [VOL], sLayout)
    write_random(wdev0.path, 1)
    gid = walbc.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r2:1', md0, md1)
    print 'test_r2:succeeded'


def replace_archive(aDel, aAdd, volL, newServerLayout):
    isSyncL = []
    for vol in volL:
        isSyncL.append(walbc.is_synchronizing(aDel, vol))
    startup(aAdd)
    for vol, isSync in zip(volL, isSyncL):
        walbc.replicate(aDel, vol, aAdd, isSync)
        if isSync:
            walbc.stop_sync(aDel, vol)
        walbc.clear_vol(aDel, vol)
    walbc.shutdown(aDel)

    walbc.set_server_layout(newServerLayout)
    for sx in newServerLayout.storageL:
        walbc.shutdown(sx)
        startup(sx)


def test_replace_archive_sync(ax, ay, newServerLayout):
    '''
    Replace ax by ay.
    ax :: Server        - must be synchronizing.
    ay :: Server        - must not be started.
    newServerLayout :: ServerLayout
    '''
    if not walbc.is_synchronizing(ax, VOL):
        raise Exception('test_replace_archive_sync: not synchronizing', ax, ay)
    replace_archive(ax, ay, [VOL], newServerLayout)
    write_random(wdev0.path, 1)
    gid = walbc.snapshot_sync(s0, VOL, [ay])
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(ay, VOL, gid)
    print 'test_replace_archive_sync', ax, ay
    verify_equal_sha1('test_replace_archive_sync:', md0, md1)


def test_replace_archive_nosync(ax, ay, newServerLayout):
    '''
    replace ax by ay.
    ax must not be synchronizing.
    ay must not be started.
    '''
    if walbc.is_synchronizing(ax, VOL):
        raise Exception('test_replace_archive_nosync: synchronizing', ax, ay)
    md0 = get_sha1_of_restorable(
        ax, VOL, walbc.get_latest_clean_snapshot(ax, VOL))
    replace_archive(ax, ay, [VOL], newServerLayout)
    write_random(wdev0.path, 1)
    gid = walbc.get_latest_clean_snapshot(ay, VOL)
    md1 = get_sha1_of_restorable(ay, VOL, gid)
    verify_equal_sha1('test_replace_archive_nosync', md0, md1)


def test_r3():
    """
        replace a0 by a2
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_r3:replace-primary-archive', g_count
    sLayout2 = sLayout.replace(archiveL=[a2, a1])
    test_replace_archive_sync(a0, a2, sLayout2)
    test_replace_archive_sync(a2, a0, sLayout)
    print 'test_r3:succeeded'


def test_r4():
    """
        replace a1 by a2
    """
    print '++++++++++++++++++++++++++++++++++++++ ' \
        'test_r4:replace-secondary-archive', g_count
    isSync = walbc.is_synchronizing(a1, VOL)
    if isSync:
        raise Exception('test_r4:a1 synchronizing', a1)

    walbc.replicate(a0, VOL, a1, False)  # preparation

    sLayout2 = sLayout.replace(archiveL=[a0, a2])
    test_replace_archive_nosync(a1, a2, sLayout2)
    test_replace_archive_nosync(a2, a1, sLayout)
    print 'test_r4:succeeded'


###############################################################################
# Main
###############################################################################


allL = ['n1', 'n2', 'n3', 'n4b', 'n5', 'n6', 'n7', 'n8', 'n9',
        'n10', 'n11a', 'n11b', 'n12',
        'm1', 'm2', 'm3',
        'e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7', 'e8',
        'e9', 'e10', 'e11', 'e12', 'e13',
        'r1', 'r2', 'r3', 'r4',
       ]


def main():
    try:
        count = int(sys.argv[1])
        pos = 2
    except:
        count = 1
        pos = 1

    testL = sys.argv[pos:]
    if not testL:
        testL = allL

    print "count", count
    print 'test', testL
    setup_test()
    startup_all()
    for i in xrange(count):
        global g_count
        g_count = i
        print "===============================", i, datetime.datetime.today()
        for test in testL:
            (globals()['test_' + test])()
        if i != count - 1:
            cleanup(VOL, wdevL)
    # shutdown_all()


if __name__ == "__main__":
    main()
