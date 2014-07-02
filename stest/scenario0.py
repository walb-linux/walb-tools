#!/usr/bin/env python

import os
import itertools
import random
import datetime
from walb_cmd import *

workDir = os.getcwd() + '/stest/tmp/'
binDir = os.getcwd() + '/binsrc/'


def D(name):
    return workDir + name


def L(name):
    return workDir + name + '.log'


s0 = Server('s0', 'localhost', 10000, K_STORAGE, binDir, D('s0'), L('s0'), None)
s1 = Server('s1', 'localhost', 10001, K_STORAGE, binDir, D('s1'), L('s1'), None)
s2 = Server('s2', 'localhost', 10002, K_STORAGE, binDir, D('s2'), L('s2'), None)
p0 = Server('p0', 'localhost', 10100, K_PROXY,   binDir, D('p0'), L('p0'), None)
p1 = Server('p1', 'localhost', 10101, K_PROXY,   binDir, D('p1'), L('p1'), None)
p2 = Server('p2', 'localhost', 10102, K_PROXY,   binDir, D('p2'), L('p2'), None)
a0 = Server('a0', 'localhost', 10200, K_ARCHIVE, binDir, D('a0'), L('a0'), 'vg0')
a1 = Server('a1', 'localhost', 10201, K_ARCHIVE, binDir, D('a1'), L('a1'), 'vg1')
a2 = Server('a2', 'localhost', 10202, K_ARCHIVE, binDir, D('a2'), L('a2'), 'vg2')

isDebug = True
config = Config(isDebug, binDir, workDir, [s0, s1], [p0, p1], [a0, a1])


wdev0 = Wdev(0, '/dev/walb/0', '/dev/test/data', '/dev/test/log', 12)
wdev1 = Wdev(1, '/dev/walb/1', '/dev/test/data2', '/dev/test/log2', 12)
wdev2 = Wdev(2, '/dev/walb/2', '/dev/test/data3', '/dev/test/log3', 12)
wdevL = [wdev0, wdev1, wdev2]

VOL = 'vol0'
g_count = 0


def replace_config(cfg, archiveL = None, proxyL = None):
    if not archiveL:
        archiveL = cfg.archiveL
    if not proxyL:
        proxyL = cfg.proxyL
    return Config(cfg.debug, cfg.binDir, cfg.dataDir, cfg.storageL,
                  proxyL, archiveL)


def setup_test():
    run_command(['/bin/rm', '-rf', WORK_DIR])
    for ax in config.archiveL:
        if ax.vg:
            vgPath = '/dev/' + ax.vg + '/';
            if os.path.isdir(vgPath):
                for f in os.listdir(vgPath):
                    if f[0] == 'i':
                        run_command(['/sbin/lvremove', '-f', vgPath + f])
    make_dir(WORK_DIR)
    kill_all_servers()
    walb = Walb(config)
    for wdev in wdevL:
        walb.reset_walb_dev(wdev)


def startup(s):
    '''
    for scenario test.
    '''
    make_dir(config.dataDir + s.name)
    args = Walb(config).get_server_args(s)
    if config.debug:
        print 'cmd=', to_str(args)
    run_daemon(args)
    wait_for_server_port('localhost', s.port)


def startup_all():
    '''
    for scenario test.
    '''
    for s in config.archiveL + config.proxyL + config.storageL:
        startup(s)


def resize_storage_if_necessary(sx, vol, wdev, sizeMb):
    """
        assume init() has been called before this.
    """
    walb = Walb(config)
    if walb.get_walb_dev_sizeMb(wdev) >= sizeMb:
        return
    resize_lv(wdev.data, get_lv_size_mb(wdev.data), sizeMb, False)
    walb.resize_storage(sx, vol, sizeMb)


def remove_persistent_data(s):
    '''
    Remove persistent data for scenario test.
    call shutdown() before calling this.
    s :: Server
    '''
    shutil.rmtree(cfg.dataDir + s.name)
    if s in cfg.archiveL:
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
    args = [cfg.binDir + "/write_random_data",
        '-s', str(sizeLb), '-o', str(offset), bdevPath]
    if fixVar:
        args += ['-set', str(fixVar)]
    return run_command(args, False)


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
    for s in ["storage-server", "proxy-server", "archive-server"]:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()
    time.sleep(0.3)


def write_over_wldev(self, wdev, overflow=False):
    '''
    Write to a walb device with size larger then log device.
    wdev :: Wdev     - walb device.
    overflow :: bool - True if you want to overflow the device.
    '''
    walb = Walb(config)
    verify_type(wdev, Wdev)
    verify_type(overflow, bool)
    wldevSizeLb = get_lv_size_mb(wdev.log) * 1024 * 1024 / 512
    wdevSizeLb = walb.get_walb_dev_sizeMb(wdev) * 1024 * 1024 / 512
    print "wldevSizeLb, wdevSizeLb", wldevSizeLb, wdevSizeLb
    # write a little bigger size than wldevSizeLb
    remainLb = wldevSizeLb + 4
    writeMaxLb = min(wldevSizeLb, wdevSizeLb) / 2
    while remainLb > 0:
        writeLb = min(remainLb, writeMaxLb)
        print 'writing %d MiB' % (writeLb * 512 / 1024 / 1024)
        write_random(wdev.path, writeLb)
        if not overflow:
            walb.wait_for_log_empty(wdev)
        remainLb -= writeLb


################################################################################
# Normal scenario tests.
################################################################################


def test_n1():
    """
        full-backup -> sha1 -> restore -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n1:full-backup", g_count
    walb = Walb(config)
    walb.init(s0, VOL, wdev0.path)
    walb.init(s1, VOL, wdev1.path)
    write_random(wdev0.path, 1)
    md0 = get_sha1(wdev0.path)
    gid = walb.full_backup(s0, VOL)
    walb.restore_and_verify_sha1('test_n1', md0, a0, VOL, gid)
    print 'test_n1:succeeded'


def test_n2():
    """
        write -> sha1 -> snapshot -> restore -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n2:snapshot", g_count
    walb = Walb(config)
    write_random(wdev0.path, 1)
    md0 = get_sha1(wdev0.path)
    gid = walb.snapshot_sync(s0, VOL, [a0])
    print "gid=", gid
    print walb.list_restorable(a0, VOL)
    walb.restore_and_verify_sha1('test_n2', md0, a0, VOL, gid)
    print 'test_n2:succeeded'


def test_n3():
    """
        hash-backup -> sha1 -> restore -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n3:hash-backup", g_count
    walb = Walb(config)
    walb.set_slave_storage(s0, VOL)
    write_random(wdev0.path, 1)
    md0 = get_sha1(wdev0.path)
    gid = walb.hash_backup(s0, VOL)
    print "gid=", gid
    walb.restore_and_verify_sha1('test_n3', md0, a0, VOL, gid)
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
    walb = Walb(config)
    printL(stopL, startL)
    with RandomWriter(wdev0.path):
        for s in stopL:
            walb.stop(s, VOL)
            time.sleep(0.1)

        time.sleep(0.5)

        for s in startL:
            walb.start(s, VOL)
            time.sleep(0.1)

    md0 = get_sha1(wdev0.path)
    gid = walb.snapshot_sync(s0, VOL, [a0])
    walb.restore_and_verify_sha1('test_stop', md0, a0, VOL, gid)


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
    walb = Walb(config)
    with RandomWriter(wdev0.path):
        time.sleep(0.5)
        gid = walb.snapshot_sync(s0, VOL, [a0])
        time.sleep(0.5)
    md0 = walb.get_sha1_of_restorable(a0, VOL, gid)
    walb.apply_diff(a0, VOL, gid)
    walb.restore_and_verify_sha1('test_n5', md0, a0, VOL, gid)
    print 'test_n5:succeeded'


def test_n6():
    """
        merge -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n6:merge", g_count
    walb = Walb(config)
    with RandomWriter(wdev0.path):
        time.sleep(0.5)
        gidB = walb.snapshot_sync(s0, VOL, [a0])
        time.sleep(1)
        # create more than two diff files
        walb.stop(s0, VOL)
        walb.stop(p0, VOL, 'empty')
        walb.start(s0, VOL)
        walb.start(p0, VOL)
        time.sleep(1)
        gidE = walb.snapshot_sync(s0, VOL, [a0])
        gidL = walb.list_restorable(a0, VOL, 'all')
        posB = gidL.index(gidB)
        posE = gidL.index(gidE)
        print "gidB", gidB, "gidE", gidE, "gidL", gidL
        if posE - posB < 2:
            raise Exception('test_n6:bad range', gidB, gidE, gidL)
        time.sleep(0.5)
    # merge gidB and gidE

    md0 = walb.get_sha1_of_restorable(a0, VOL, gidE)
    walb.merge_diff(a0, VOL, gidB, gidE)
    print "merged gidL", walb.list_restorable(a0, VOL, 'all')
    walb.restore_and_verify_sha1('test_n6', md0, a0, VOL, gidE)
    print 'test_n6:succeeded'


def test_n7():
    """
        replicate (no synchronizing, full) -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n7:replicate-full", g_count
    walb = Walb(config)
    walb.replicate(a0, VOL, a1, False)
    walb.verify_equal_list_restorable('test_n7', a0, a1, VOL)
    gid = walb.get_latest_clean_snapshot(a0, VOL)
    md0 = walb.get_sha1_of_restorable(a0, VOL, gid)
    md1 = walb.get_sha1_of_restorable(a1, VOL, gid)
    verify_equal_sha1('test_n7', md0, md1)
    print 'test_n7:succeeded'


def test_n8():
    """
        replicate (no synchronizing, diff) -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n8:replicate-diff", g_count
    walb = Walb(config)
    write_random(wdev0.path, 1)
    gid0 = walb.snapshot_sync(s0, VOL, [a0])
    gidA0 = walb.get_latest_clean_snapshot(a0, VOL)
    if gidA0 != gid0:
        raise Exception('test_n8:wrong gid', gidA0, gid0)
    gidA1 = walb.get_latest_clean_snapshot(a1, VOL)
    if gidA0 <= gidA1:
        raise Exception('test_n8:no progress', gidA0, gidA1)
    walb.replicate(a0, VOL, a1, False)
    walb.verify_equal_list_restorable('test_n8', a0, a1, VOL)
    gid1 = walb.get_latest_clean_snapshot(a0, VOL)
    md0 = walb.get_sha1_of_restorable(a0, VOL, gid1)
    md1 = walb.get_sha1_of_restorable(a1, VOL, gid1)
    verify_equal_sha1('test_n8', md0, md1)
    print 'test_n8:succeeded'


def test_n9():
    """
        replicate (no synchronizing, hash) -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n9:replicate-hash", g_count
    walb = Walb(config)
    write_random(wdev0.path, 1)
    gid0 = walb.snapshot_sync(s0, VOL, [a0])
    walb.apply_diff(a0, VOL, gid0)
    list0 = walb.list_restorable(a0, VOL)
    if len(list0) != 1:
        raise Exception('test_n9: list size must be 1', list0)
    write_random(wdev0.path, 1)
    walb.replicate(a0, VOL, a1, False)
    gid1a0 = walb.get_latest_clean_snapshot(a0, VOL)
    gid1a1 = walb.get_latest_clean_snapshot(a1, VOL)
    if gid1a0 != gid1a1:
        raise Exception('test_n9: gid differ', gid1a0, gid1a1)
    md0 = walb.get_sha1_of_restorable(a0, VOL, gid1a0)
    md1 = walb.get_sha1_of_restorable(a1, VOL, gid1a1)
    verify_equal_sha1('test_n9', md0, md1)
    print 'test_n9:succeeded'


def test_n10():
    """
        replicate (sychronizing) -> sha1
    """
    print "++++++++++++++++++++++++++++++++++++++ test_n10:replicate-synchronizing", g_count
    walb = Walb(config)
    with RandomWriter(wdev0.path):
        time.sleep(0.5)
        walb.replicate(a0, VOL, a1, True)
        time.sleep(0.5)
        #gid0 = snapshot_sync(s0, VOL, [a0, a1])
        gid0 = walb.snapshot_async(s0, VOL)
        walb.wait_for_restorable(a0, VOL, gid0)
        walb.wait_for_restorable(a1, VOL, gid0)
        md0 = walb.get_sha1_of_restorable(a0, VOL, gid0)
        md1 = walb.get_sha1_of_restorable(a1, VOL, gid0)
        verify_equal_sha1('test_n10', md0, md1)
        walb.stop_sync(a1, VOL)
        time.sleep(0.5)
    gid1 = walb.snapshot_sync(s0, VOL, [a0])
    gid1a1 = walb.get_latest_clean_snapshot(a1, VOL)
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
    print "++++++++++++++++++++++++++++++++++++++ test_n11:resize", doZeroClear, g_count
    walb = Walb(config)
    with RandomWriter(wdev0.path):
        prevSize = walb.get_walb_dev_sizeMb(wdev0)
        walb.snapshot_sync(s0, VOL, [a0])
        # lvm extent size is 4MiB
        resize_lv(wdev0.data, prevSize, prevSize + 4, doZeroClear)
        resize_lv(wdev1.data, prevSize, prevSize + 4, doZeroClear)
        walb.resize(VOL, prevSize + 4, doZeroClear)
        curSize = walb.get_walb_dev_sizeMb(wdev0)
        if curSize != prevSize + 4:
            raise Exception('test_n11:bad size', prevSize, curSize)
    write_random(wdev0.path, 1, prevSize * 1024 * 1024 / 512)
    if doZeroClear:
        gid = walb.snapshot_sync(s0, VOL, [a0])
    else:
        walb.set_slave_storage(s0, VOL)
        gid = walb.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
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
    print "++++++++++++++++++++++++++++++++++++++ test_n12:exchange-master-slave", g_count
    walb = Walb(config)
    with RandomWriter(wdev0.path):
        with RandomWriter(wdev1.path):
            time.sleep(0.3)
            walb.set_slave_storage(s0, VOL)
            time.sleep(0.3)
    gid = walb.hash_backup(s1, VOL)
    md0 = get_sha1(wdev1.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_n12', md0, md1)
    st0 = walb.get_state(s0, VOL)
    if st0 != 'Slave':
        raise Exception('test_n12:s0:1:bad state', st0)
    st1 = walb.get_state(s1, VOL)
    if st1 != 'Master':
        raise Exception('test_n12:s1:1:bad state', st1)

    walb.set_slave_storage(s1, VOL)
    gid = walb.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_n12', md0, md1)
    st0 = walb.get_state(s0, VOL)
    if st0 != 'Master':
        raise Exception('test_n12:s0:2:bad state', st0)
    st1 = walb.get_state(s1, VOL)
    if st1 != 'Slave':
        raise Exception('test_n12:s0:2:bad state', st1)
    print 'test_n12:succeeded'


################################################################################
# Misoperation scenario tests.
################################################################################


def test_m1():
    """
        full-bkp --> full-bkp fails.
    """
    print "++++++++++++++++++++++++++++++++++++++ test_m1:full-bkp-after-full-bkp-fails", g_count
    walb = Walb(config)
    write_random(wdev0.path, 1)
    walb.stop_sync(a0, VOL)
    try:
        walb.full_backup(s0, VOL, 10)
    except:
        # expect to catch an exception.
        walb.start_sync(a0, VOL)
        print 'test_m1:succeeded'
        return
    raise Exception('test_m1:full_backup did not fail')


def test_m2():
    """
        init --> hash-bkp fails.
    """
    print "++++++++++++++++++++++++++++++++++++++ test_m2:hash-bkp-fails.", g_count
    walb = Walb(config)
    walb.stop_sync(a0, VOL)
    walb.stop(a0, VOL)
    walb.reset_vol(a0, VOL)
    write_random(wdev0.path, 1)
    try:
        walb.hash_backup(s0, VOL, 10)
    except:
        # expect to catch an exception.
        walb.set_slave_storage(s0, VOL)
        walb.full_backup(s0, VOL)
        print 'test_m2:succeeded'
        return
    raise Exception('test_m2:hash_backup did not fail')


def test_m3():
    """
        resize at storage -> write -> wdiff-transfer fails.
    """
    print '++++++++++++++++++++++++++++++++++++++ test_m3:resize-fails', g_count
    walb = Walb(config)
    prevSizeMb = walb.get_walb_dev_sizeMb(wdev0)
    walb.snapshot_sync(s0, VOL, [a0])
    newSizeMb = prevSizeMb + 4  # lvm extent size is 4MiB
    resize_lv(wdev0.data, prevSizeMb, newSizeMb, True)
    resize_lv(wdev1.data, prevSizeMb, newSizeMb, True)
    walb.resize_storage(s0, VOL, newSizeMb)
    walb.resize_storage(s1, VOL, newSizeMb)
    write_random(wdev0.path, 1, prevSizeMb * 1024 * 1024 / 512)
    curSizeMb = walb.get_walb_dev_sizeMb(wdev0)
    if curSizeMb != newSizeMb:
        raise Exception('test_m3:bad size', newSizeMb, curSizeMb)
    gid1 = walb.snapshot_async(s0, VOL)
    walb.verify_not_restorable(a0, VOL, gid1, 10, 'test_m3')
    if not walb.is_wdiff_send_error(p0, VOL, a0):
        raise Exception('test_m3:must occur wdiff-send-error')
    walb.resize_archive(a0, VOL, newSizeMb, True)
    walb.kick_all_storage()
    walb.kick_all(config.proxyL)
    walb.wait_for_restorable(a0, VOL, gid1)
    if walb.is_wdiff_send_error(p0, VOL, a0):
        raise Exception('test_m3:must not occur wdiff-send-error')
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid1)
    verify_equal_sha1('test_m3', md0, md1)
    print 'test_m3:succeeded'


################################################################################
# Error scenario tests.
################################################################################


def test_e1():
    """
        p0 down -> write over wldev amount -> p0 up -> snapshot -> sha1

    """
    print '++++++++++++++++++++++++++++++++++++++ test_e1:proxy-down', g_count
    walb = Walb(config)
    walb.shutdown(p0, 'force')
    write_over_wldev(wdev0)
    walb.verify_not_overflow(s0, VOL)
    startup(p0)
    walb.kick_all_storage()
    gid = walb.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e1', md0, md1)
    print 'test_e1:succeeded'


def test_e2():
    """
        a0 down -> write over wldev amount -> a0 up -> snapshot -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ test_e2:archive-down', g_count
    walb = Walb(config)
    walb.shutdown(a0, 'force')
    write_over_wldev(wdev0)
    walb.verify_not_overflow(s0, VOL)
    gid = walb.snapshot_async(s0, VOL)
    walb.verify_not_restorable(a0, VOL, gid, 10, 'test_e2')
    startup(a0)
    walb.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e2', md0, md1)
    print 'test_e2:succeeded'


def test_e3():
    """
        s0 down -> write -> s0 up -> snapshot -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ test_e3:storage-down', g_count
    walb = Walb(config)
    walb.shutdown(s0, 'force')
    write_random(wdev0.path, 1)
    startup(s0)
    gid = walb.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e3', md0, md1)
    print 'test_e3:succeeded'


def test_e4():
    """
        s0 down -> write over wldev amount -> s0 up -> hash-backup-> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ test_e4:storage-down-overflow', g_count
    walb = Walb(config)
    walb.shutdown(s0, 'force')
    write_over_wldev(wdev0, overflow=True)
    startup(s0)
    if not walb.is_overflow(s0, VOL):
        raise Exception('test_e4:must be overflow')
    gid = walb.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e4', md0, md1)
    print 'test_e4:succeeded'


def test_e5():
    """
        p0 data lost -> p0 up -> hash-backup -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ test_e5:proxy-data-lost', g_count
    walb = Walb(config)
    walb.stop(a0, VOL, 'force')
    write_random(wdev0.path, 1)
    time.sleep(3)  # wait for the log is sent to p0.
    walb.shutdown(p0, 'force')
    remove_persistent_data(p0)
    walb.start(a0, VOL)
    startup(p0)
    walb.start_sync(a0, VOL)
    write_random(wdev0.path, 1)
    gid0 = walb.snapshot_async(s0, VOL)
    walb.verify_not_restorable(a0, VOL, gid0, 10, 'test_e5')
    gid1 = walb.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid1)
    verify_equal_sha1('test_e5', md0, md1)
    print 'test_e5:succeeded'


def test_e6():
    """
        a0 data lost -> a0 up -> full-backup -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ test_e6:primary-archive-data-lost', g_count
    walb = Walb(config)
    walb.shutdown(a0, 'force')
    remove_persistent_data(a0)
    startup(a0)
    walb.run_ctl(a0, ['init-vol', VOL])
    gid0 = walb.snapshot_async(s0, VOL)
    walb.verify_not_restorable(a0, VOL, gid0, 10, 'test_e6')
    gid1 = walb.full_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid1)
    verify_equal_sha1('test_e6', md0, md1)
    print 'test_e6:succeeded'


def test_e7():
    """
        a1 data lost -> a1 up -> replicate -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ test_e7:secondary-archive-data-lost', g_count
    walb = Walb(config)
    write_random(wdev0.path, 1)
    walb.replicate(a0, VOL, a1, True)
    walb.shutdown(a1, 'force')
    remove_persistent_data(a1)
    startup(a1)
    write_random(wdev0.path, 1)
    gid0 = walb.snapshot_async(s0, VOL)
    walb.wait_for_restorable(a0, VOL, gid0)
    walb.verify_not_restorable(a1, VOL, gid0, 10, 'test_e7')
    walb.replicate(a0, VOL, a1, True)
    write_random(wdev0.path, 1)
    gid1 = walb.snapshot_sync(s0, VOL, [a0, a1])
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid1)
    md2 = walb.get_sha1_of_restorable(a1, VOL, gid1)
    verify_equal_sha1('test_e7:1', md0, md1)
    verify_equal_sha1('test_e7:2', md1, md2)
    walb.stop_sync(a1, VOL)
    print 'test_e7:succeeded'


def test_e8():
    """
        s0 data lost -> s0 up -> hash-backup -> sha1
    """
    print '++++++++++++++++++++++++++++++++++++++ test_e8:storage-data-lost', g_count
    walb = Walb(config)
    write_random(wdev0.path, 1)
    walb.shutdown(s0, 'force')
    remove_persistent_data(s0)
    startup(s0)
    write_random(wdev0.path, 1)
    walb.run_ctl(s0, ['init-vol', VOL, wdev0.path])
    write_random(wdev0.path, 1)
    gid = walb.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_e8', md0, md1)
    print 'test_e8:succeeded'


################################################################################
# Replacement scenario tests.
################################################################################


def test_r1():
    """
        replace s0 by s2
    """
    print '++++++++++++++++++++++++++++++++++++++ test_r1:replace-storage', g_count
    walb = Walb(config)
    startup(s2)
    walb.init(s2, VOL, wdev2.path)
    resize_storage_if_necessary(s2, VOL, wdev2, walb.get_walb_dev_sizeMb(wdev0))

    walb.set_slave_storage(s0, VOL)
    write_random(wdev2.path, 1)
    gid = walb.hash_backup(s2, VOL)
    md0 = get_sha1(wdev2.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r1:0', md0, md1)
    walb.clear_vol(s0, VOL)

    # turn back to the beginning state.
    walb.init(s0, VOL, wdev0.path)
    walb.set_slave_storage(s2, VOL)
    write_random(wdev0.path, 1)
    gid = walb.hash_backup(s0, VOL)
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r1:1', md0, md1)

    walb.clear_vol(s2, VOL)
    walb.shutdown(s2)
    print 'test_r1:succeeded'


def replace_proxy(pDel, pAdd, volL, newConfig):
    """
        replace pDel by pAdd.
        Before calling this,
            pDel must be running.
            pAdd must be down.

    """
    walb = Walb(config)
    startup(pAdd)
    for vol in volL:
        walb.copy_archive_info(pDel, vol, pAdd)
        walb.run_ctl(pDel, ['stop', vol, 'empty'])
    for vol in volL:
        walb.wait_for_stopped(pDel, vol)
        walb.clear_vol(pDel, vol)
    walb.shutdown(pDel)

    set_config(newConfig)
    walb = Walb(newConfig)
    for sx in newConfig.storageL:
        walb.shutdown(sx)
        startup(sx)


def test_r2():
    """
        replace p0 by p2
    """
    print '++++++++++++++++++++++++++++++++++++++ test_r2:replace-proxy', g_count
    config2 = replace_config(config, proxyL=[p2, p1])
    walb = Walb(config2)
    replace_proxy(p0, p2, [VOL], config2)
    write_random(wdev0.path, 1)
    gid = walb.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r2:0', md0, md1)

    # turn back to the beginning state.
    walb = Walb(config)
    replace_proxy(p2, p0, [VOL], config)
    write_random(wdev0.path, 1)
    gid = walb.snapshot_sync(s0, VOL, [a0])
    md0 = get_sha1(wdev0.path)
    md1 = walb.get_sha1_of_restorable(a0, VOL, gid)
    verify_equal_sha1('test_r2:1', md0, md1)
    print 'test_r2:succeeded'


def replace_archive(aDel, aAdd, volL, newConfig):
    walb0 = Walb(config)
    isSyncL = []
    for vol in volL:
        isSyncL.append(walb0.is_synchronizing(aDel, vol))
    startup(aAdd)
    for vol, isSync in zip(volL, isSyncL):
        walb0.replicate(aDel, vol, aAdd, isSync)
        if isSync:
            walb0.stop_sync(aDel, vol)
        walb0.clear_vol(aDel, vol)
    walb0.shutdown(aDel)

    walb1 = Walb(newConfig)
    set_config(newConfig)
    for sx in newConfig.storageL:
        walb1.shutdown(sx)
        startup(sx)


def test_replace_archive_sync(ax, ay, newConfig):
    '''
    Replace ax by ay.
    ax :: Server        - must be synchronizing.
    ay :: Server        - must not be started.
    newConfig :: Config - new configuration.
    '''
    walb0 = Walb(config)
    if not walb0.is_synchronizing(ax, VOL):
        raise Exception('test_replace_archive_sync: not synchronizing', ax, ay)
    replace_archive(ax, ay, [VOL], newConfig)
    walb1 = Walb(newConfig)
    write_random(wdev0.path, 1)
    gid = walb1.snapshot_sync(s0, VOL, [ay])
    md0 = get_sha1(wdev0.path)
    md1 = walb1.get_sha1_of_restorable(ay, VOL, gid)
    print 'test_replace_archive_sync', ax, ay
    verify_equal_sha1('test_replace_archive_sync:', md0, md1)


def test_replace_archive_nosync(ax, ay, newConfig):
    """
    replace ax by ay.
    ax must not be synchronizing.
    ay must not be started.

    """
    walb0 = Walb(config)
    if walb0.is_synchronizing(ax, VOL):
        raise Exception('test_replace_archive_nosync: synchronizing', ax, ay)
    md0 = walb0.get_sha1_of_restorable(
        ax, VOL, walb0.get_latest_clean_snapshot(ax, VOL))
    replace_archive(ax, ay, [VOL], newConfig)
    walb1 = Walb(newConfig)
    write_random(wdev0.path, 1)
    gid = walb1.get_latest_clean_snapshot(ay, VOL)
    md1 = walb1.get_sha1_of_restorable(ay, VOL, gid)
    verify_equal_sha1('test_replace_archive_nosync', md0, md1)


def test_r3():
    """
        replace a0 by a2
    """
    print '++++++++++++++++++++++++++++++++++++++ test_r3:replace-primary-archive', g_count
    config2 = replace_config(config, archiveL=[a2, a1])
    test_replace_archive_sync(a0, a2, config2)
    test_replace_archive_sync(a2, a0, config)
    print 'test_r3:succeeded'


def test_r4():
    """
        replace a1 by a2
    """
    print '++++++++++++++++++++++++++++++++++++++ test_r4:replace-secondary-archive', g_count
    walb = Walb(config)
    isSync = walb.is_synchronizing(a1, VOL)
    if isSync:
        raise Exception('test_r4:a1 synchronizing', a1)

    walb.replicate(a0, VOL, a1, False)  # preparation

    config2 = replace_config(config, archiveL=[a0, a2])
    test_replace_archive_nosync(a1, a2, config2)
    test_replace_archive_nosync(a2, a1, config)
    print 'test_r4:succeeded'


################################################################################
# Main
################################################################################


allL = ['n1', 'n2', 'n3', 'n4b', 'n5', 'n6', 'n7', 'n8', 'n9', 'n10', 'n11a', 'n11b', 'n12',
        'm1', 'm2', 'm3',
        'e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7', 'e8',
        'r1', 'r2', 'r3', 'r4']


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
            Walb(config).cleanup(VOL, wdevL)
    # shutdown_all()


if __name__ == "__main__":
    main()
