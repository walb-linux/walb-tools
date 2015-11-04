#!/usr/bin/env python

import sys
sys.path.insert(0, './python/walb')
from walb import *

import itertools, random, datetime, tempfile
from stest_util import *
from repeater import *
from common import *


'''
These variables are defined for pylint.
They will be set by set_config().
These must be read-only.
'''
walbc = None
VOL = None
s0 = None
s1 = None
s2 = None
p0 = None
p1 = None
p2 = None
a0 = None
a1 = None
a2 = None
wdev0 = None
wdev1 = None
wdev2 = None
sLayoutRepeater1 = None
sLayoutRepeater2 = None

import config0
set_config(globals(), config0.get_config())

g_count = 0

###############################################################################
# Normal scenario tests.
###############################################################################


def test_n1():
    """
        full-backup -> sha1 -> restore -> sha1
    """
    info = 'test_n1:full-backup count:%d' % g_count
    try:
        print_action_info('START', info)
        walbc.init_storage(s0, VOL, wdev0.path)
        walbc.init_storage(s1, VOL, wdev1.path)
        write_random(wdev0.path, 1)
        md0 = get_sha1(wdev0.path)
        gid = walbc.full_backup(s0, VOL, TIMEOUT)
        restore_and_verify_sha1('test_n1', md0, a0, VOL, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n2():
    """
        write -> sha1 -> snapshot -> restore -> sha1
    """
    info = 'test_n2:snapshot count:%d' % g_count
    try:
        print_action_info('START', info)
        write_random(wdev0.path, 1)
        md0 = get_sha1(wdev0.path)
        gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        print "gid=", gid
        walbc.print_restorable(a0, VOL)
        restore_and_verify_sha1('test_n2', md0, a0, VOL, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n3():
    """
        hash-backup -> sha1 -> restore -> sha1
    """
    info = 'test_n3:hash-backup count:%d' % g_count
    try:
        print_action_info('START', info)
        walbc.go_standby(s0, VOL)
        write_random(wdev0.path, 1)
        md0 = get_sha1(wdev0.path)
        gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        print "gid=", gid
        restore_and_verify_sha1('test_n3', md0, a0, VOL, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def printL2(aL, bL):
    print '[',
    for a in aL:
        print a.name,
    print '], [',
    for b in bL:
        print b.name,
    print ']'


def test_stop(stopL, startL):
    printL2(stopL, startL)
    with RandomWriter(wdev0.path):
        for s in stopL:
            walbc.stop(s, VOL)
            time.sleep(0.1)

        time.sleep(0.5)

        for s in startL:
            walbc.start(s, VOL)
            time.sleep(0.1)

    md0 = get_sha1(wdev0.path)
    gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
    restore_and_verify_sha1('test_stop', md0, a0, VOL, gid)


def test_n4_detail(numPatterns=0):
    """
        stop -> start -> snapshot -> sha1
        if numPatterns == 0 then all possible patterns will be tested.
        otherwise, numPatterns patterns only will be tested.
    """
    info = 'test_n4:stop count:%d' % g_count
    try:
        print_action_info('START', info)
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
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n4a():
    test_n4_detail()


def test_n4b():
    test_n4_detail(5)


def test_n5():
    """
        apply -> sha1
    """
    info = 'test_n5:apply count:%d' % g_count
    try:
        print_action_info('START', info)
        with RandomWriter(wdev0.path):
            time.sleep(0.5)
            gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
            time.sleep(0.5)
        md0 = get_sha1_of_restorable(a0, VOL, gid)
        walbc.apply(a0, VOL, gid, TIMEOUT)
        restore_and_verify_sha1('test_n5', md0, a0, VOL, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n6():
    """
        merge -> sha1
    """
    info = 'test_n6:merge count:%d' % g_count
    try:
        print_action_info('START', info)
        with RandomWriter(wdev0.path):
            time.sleep(0.5)
            gidB = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
            time.sleep(1)
            # create more than two diff files
            walbc.stop(s0, VOL)
            walbc.stop(p0, VOL, 'empty')
            walbc.start(s0, VOL)
            walbc.start(p0, VOL)
            time.sleep(1)
            gidE = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
            gidL = walbc.get_restorable_gid(a0, VOL, 'all')
            posB = gidL.index(gidB)
            posE = gidL.index(gidE)
            print "gidB", gidB, "gidE", gidE, "gidL", gidL
            if posE - posB < 2:
                raise Exception('test_n6:bad range', gidB, gidE, gidL)
            time.sleep(0.5)
        # merge gidB and gidE

        md0 = get_sha1_of_restorable(a0, VOL, gidE)
        walbc.merge(a0, VOL, gidB, gidE, TIMEOUT)
        print "merged gidL", walbc.get_restorable_gid(a0, VOL, 'all')
        restore_and_verify_sha1('test_n6', md0, a0, VOL, gidE)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n7():
    """
        replicate (no synchronizing, full) -> sha1
    """
    info = 'test_n7:replicate-full count:%d' % g_count
    try:
        print_action_info('START', info)
        walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        walbc.replicate(a0, VOL, a1, False, TIMEOUT)
        verify_equal_restorable_list('test_n7', a0, a1, VOL)
        gid = walbc.get_latest_clean_snapshot(a0, VOL)
        md0 = get_sha1_of_restorable(a0, VOL, gid)
        md1 = get_sha1_of_restorable(a1, VOL, gid)
        verify_equal_sha1('test_n7', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n8():
    """
        replicate (no synchronizing, diff) -> sha1
    """
    info = 'test_n8:replicate-diff count:%d' % g_count
    try:
        print_action_info('START', info)
        write_random(wdev0.path, 1)
        gid0 = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        gidA0 = walbc.get_latest_clean_snapshot(a0, VOL)
        if gidA0 != gid0:
            raise Exception('test_n8:wrong gid', gidA0, gid0)
        gidA1 = walbc.get_latest_clean_snapshot(a1, VOL)
        if gidA0 <= gidA1:
            raise Exception('test_n8:no progress', gidA0, gidA1)
        walbc.replicate(a0, VOL, a1, False, TIMEOUT)
        verify_equal_restorable_list('test_n8', a0, a1, VOL)
        gid1 = walbc.get_latest_clean_snapshot(a0, VOL)
        md0 = get_sha1_of_restorable(a0, VOL, gid1)
        md1 = get_sha1_of_restorable(a1, VOL, gid1)
        verify_equal_sha1('test_n8', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n9():
    """
        replicate (no synchronizing, hash) -> sha1
    """
    info = 'test_n9:replicate-hash count:%d' % g_count
    try:
        print_action_info('START', info)
        write_random(wdev0.path, 1)
        gid0 = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        walbc.apply(a0, VOL, gid0, TIMEOUT)
        list0 = walbc.get_restorable_gid(a0, VOL)
        if len(list0) != 1:
            raise Exception('test_n9: list size must be 1', list0)
        write_random(wdev0.path, 1)
        walbc.replicate(a0, VOL, a1, False, TIMEOUT)
        gid1a0 = walbc.get_latest_clean_snapshot(a0, VOL)
        gid1a1 = walbc.get_latest_clean_snapshot(a1, VOL)
        if gid1a0 != gid1a1:
            raise Exception('test_n9: gid differ', gid1a0, gid1a1)
        md0 = get_sha1_of_restorable(a0, VOL, gid1a0)
        md1 = get_sha1_of_restorable(a1, VOL, gid1a1)
        verify_equal_sha1('test_n9', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n10():
    """
        replicate (sychronizing) -> sha1
    """
    info = 'test_n10:replicate-synchronizing count:%d' % g_count
    try:
        print_action_info('START', info)
        with RandomWriter(wdev0.path):
            time.sleep(0.5)
            walbc.replicate(a0, VOL, a1, True, TIMEOUT)
            time.sleep(0.5)
            #gid0 = snapshot(s0, VOL, [a0, a1])
            gid0 = walbc.snapshot_nbk(s0, VOL)
            walbc.wait_for_restorable(a0, VOL, gid0, TIMEOUT)
            walbc.wait_for_restorable(a1, VOL, gid0, TIMEOUT)
            md0 = get_sha1_of_restorable(a0, VOL, gid0)
            md1 = get_sha1_of_restorable(a1, VOL, gid0)
            verify_equal_sha1('test_n10', md0, md1)
            walbc.stop_synchronizing(a1, VOL)
            time.sleep(0.5)
        gid1 = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        gid1a1 = walbc.get_latest_clean_snapshot(a1, VOL)
        if gid1 <= gid1a1:
            raise Exception('test_n10: not stopped synchronizing', gid1, gid1a1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n11(doZeroClear):
    """
        doZeroClear is true:
            resize -> snapshot -> sha1
        otherwise:
            resize -> hash backup -> sha1

    """
    info = 'test_n11:resize count:%d' % g_count
    try:
        print_action_info('START', info)
        with RandomWriter(wdev0.path):
            prevSize = wdev0.get_size_mb()
            walbc.snapshot(s0, VOL, [a0], TIMEOUT)
            # lvm extent size is 4MiB
            resize_lv(wdev0.ddev, prevSize, prevSize + 4, doZeroClear)
            resize_lv(wdev1.ddev, prevSize, prevSize + 4, doZeroClear)
            walbc.resize(VOL, prevSize + 4, doZeroClear)
            curSize = wdev0.get_size_mb()
            if curSize != prevSize + 4:
                raise Exception('test_n11:bad size', prevSize, curSize)
        write_random(wdev0.path, 1, prevSize * Mebi / Lbs)
        if doZeroClear:
            gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        else:
            walbc.go_standby(s0, VOL)
            gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_n11', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n11a():
    test_n11(True)


def test_n11b():
    test_n11(False)


def test_n12():
    """
        change target to standby
        -> change standby to target (hash-sync)
        -> sha1
        once more.
    """
    info = 'test_n12:exchange-target-standby count:%d' % g_count
    try:
        print_action_info('START', info)
        with RandomWriter(wdev0.path):
            with RandomWriter(wdev1.path):
                time.sleep(0.3)
                walbc.go_standby(s0, VOL)
                time.sleep(0.3)
        gid = walbc.hash_backup(s1, VOL, TIMEOUT)
        md0 = get_sha1(wdev1.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_n12', md0, md1)
        st0 = walbc.get_state(s0, VOL)
        if st0 != 'Standby':
            raise Exception('test_n12:s0:1:bad state', st0)
        st1 = walbc.get_state(s1, VOL)
        if st1 != 'Target':
            raise Exception('test_n12:s1:1:bad state', st1)

        walbc.go_standby(s1, VOL)
        gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_n12', md0, md1)
        st0 = walbc.get_state(s0, VOL)
        if st0 != 'Target':
            raise Exception('test_n12:s0:2:bad state', st0)
        st1 = walbc.get_state(s1, VOL)
        if st1 != 'Standby':
            raise Exception('test_n12:s0:2:bad state', st1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n13():
    """
        take snapshot n-times --> disable snapshots --> enable snapshots.
    """
    info = 'test_n13:disable-enable-snapshot: count:%d' % g_count
    try:
        print_action_info('START', info)
        walbc._apply_all(a0, VOL, TIMEOUT)
        n = 5
        gidL = []
        for _ in xrange(n):
            gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
            gidL.append(gid)
        walbc.disable_snapshot(a0, VOL, gidL[:n-1])
        restorableL = walbc.get_restorable_gid(a0, VOL)
        if len(restorableL) != 2:
            raise Exception('test_n13: restorable list length is not 2',
                            len(restorableL), restorableL)
        walbc.enable_snapshot(a0, VOL, gidL[:n-1])
        restorableL = walbc.get_restorable_gid(a0, VOL)
        if len(restorableL) != n + 1:
            raise Exception('test_n13: restorable list length is not %d' % n,
                            len(restorableL), restorableL)
        print 'test_n13:succeeded'
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_n14():
    '''
        replicate a0 to a1
        --> delete archive in a0
        --> full backup to a0
        --> confirm to fail normal replication a0 to a1
        --> resync a0 to a1
    '''
    info = 'test_n14:resync: count:{}'.format(g_count)
    try:
        print_action_info('START', info)
        walbc.replicate_once(a0, VOL, a1)
        walbc.stop(a0, VOL)
        walbc.clear(a0, VOL)
        walbc.stop(s0, VOL)
        write_random(wdev0.path, 1, offset=0)
        write_random(wdev0.path, 2, offset=1)
        md0 = get_sha1(wdev0.path)
        gid = walbc.full_backup(s0, VOL)
        try:
            walbc.replicate_once(a0, VOL, a1, doResync=False)
            testFailed = True
        except:
            testFailed = False
        if testFailed:
            raise Exception('test_n14: must fail with doResync=False')
        walbc.replicate_once(a0, VOL, a1, doResync=True)
        restore_and_verify_sha1('test_n14:a0', md0, a0, VOL, gid)
        restore_and_verify_sha1('test_n14:a1', md0, a1, VOL, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


###############################################################################
# Misoperation scenario tests.
###############################################################################


def test_m1():
    """
        full-bkp --> full-bkp fails.
    """
    info = 'test_m1:full-bkp-after-full-bkp-fails count:%d' % g_count
    try:
        print_action_info('START', info)
        write_random(wdev0.path, 1)
        walbc.stop_synchronizing(a0, VOL)
        try:
            walbc.full_backup(s0, VOL, 10)
            testFailed = True
        except:
            # expect to catch an exception.
            walbc.start_synchronizing(a0, VOL)
            testFailed = False
        if testFailed:
            raise Exception('test_m1:full_backup did not fail')
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_m2():
    """
        init --> hash-bkp fails.
    """
    info = 'test_m2:hash-bkp-fails count:%d' % g_count
    try:
        print_action_info('START', info)
        walbc.stop_synchronizing(a0, VOL)
        walbc.stop(a0, VOL)
        walbc.reset(a0, VOL)
        write_random(wdev0.path, 1)
        try:
            walbc.hash_backup(s0, VOL, 10)
            testFailed = True
        except:
            # expect to catch an exception.
            walbc.go_standby(s0, VOL)
            walbc.full_backup(s0, VOL, TIMEOUT)
            testFailed = False
        if testFailed:
            raise Exception('test_m2:hash_backup did not fail')
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_m3():
    """
        resize at storage -> write -> wdiff-transfer fails.
    """
    info = 'test_m3:resize-fails count:%d' % g_count
    try:
        print_action_info('START', info)
        prevSizeMb = wdev0.get_size_mb()
        walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        newSizeMb = prevSizeMb + 4  # lvm extent size is 4MiB
        resize_lv(wdev0.ddev, prevSizeMb, newSizeMb, True)
        resize_lv(wdev1.ddev, prevSizeMb, newSizeMb, True)
        walbc.resize_storage(s0, VOL, newSizeMb)
        walbc.resize_storage(s1, VOL, newSizeMb)
        write_random(wdev0.path, 1, prevSizeMb * Mebi / Lbs)
        curSizeMb = wdev0.get_size_mb()
        if curSizeMb != newSizeMb:
            raise Exception('test_m3:bad size', newSizeMb, curSizeMb)
        gid1 = walbc.snapshot_nbk(s0, VOL)
        walbc.verify_not_restorable(a0, VOL, gid1, 10, 'test_m3')
        if not walbc.is_wdiff_send_error(p0, VOL, a0):
            raise Exception('test_m3:must occur wdiff-send-error')
        walbc.resize_archive(a0, VOL, newSizeMb, True)
        walbc.kick_storage_all()
        walbc.kick_all(sLayout.proxyL)
        walbc.wait_for_restorable(a0, VOL, gid1, TIMEOUT)
        if walbc.is_wdiff_send_error(p0, VOL, a0):
            raise Exception('test_m3:must not occur wdiff-send-error')
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid1)
        verify_equal_sha1('test_m3', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


###############################################################################
# Error scenario tests.
###############################################################################


def test_e1():
    """
        p0 down -> write over wldev amount -> p0 up -> snapshot -> sha1

    """
    info = 'test_e1:proxy-down count:%d' % g_count
    try:
        print_action_info('START', info)
        shutdown(p0, 'force')
        write_over_wldev(wdev0)
        walbc.verify_not_overflow(s0, VOL)
        startup(p0)
        walbc.kick_storage_all()
        gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_e1', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e2():
    """
        a0 down -> write over wldev amount -> a0 up -> snapshot -> sha1
    """
    info = 'test_e2:archive-down count:%d' % g_count
    try:
        print_action_info('START', info)
        shutdown(a0, 'force')
        write_over_wldev(wdev0)
        walbc.verify_not_overflow(s0, VOL)
        gid = walbc.snapshot_nbk(s0, VOL)
        walbc.verify_not_restorable(a0, VOL, gid, 10, 'test_e2')
        startup(a0)
        walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_e2', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e3():
    """
        s0 down -> write -> s0 up -> snapshot -> sha1
    """
    info = 'test_e3:storage-down count:%d' % g_count
    try:
        print_action_info('START', info)
        shutdown(s0, 'force')
        write_random(wdev0.path, 1)
        startup(s0)
        gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_e3', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e4():
    """
        s0 down -> write over wldev amount -> s0 up -> hash-backup-> sha1
    """
    info = 'test_e4:storage-down-overflow count:%d' % g_count
    try:
        print_action_info('START', info)
        shutdown(s0, 'force')
        write_over_wldev(wdev0, overflow=True)
        startup(s0)
        if not walbc.is_overflow(s0, VOL):
            raise Exception('test_e4:must be overflow')
        walbc.stop(s0, VOL)
        gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_e4', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e5():
    """
        p0 data lost -> p0 up -> hash-backup -> sha1
    """
    info = 'test_e5:proxy-data-lost count:%d' % g_count
    try:
        print_action_info('START', info)
        walbc.stop(a0, VOL, 'force')
        write_random(wdev0.path, 1)
        time.sleep(3)  # wait for the log is sent to p0.
        shutdown(p0, 'force')
        remove_persistent_data(p0)
        walbc.start(a0, VOL)
        startup(p0)
        walbc.start_synchronizing(a0, VOL)
        write_random(wdev0.path, 1)
        gid0 = walbc.snapshot_nbk(s0, VOL)
        walbc.verify_not_restorable(a0, VOL, gid0, 10, 'test_e5')
        walbc.stop(s0, VOL)
        gid1 = walbc.hash_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid1)
        verify_equal_sha1('test_e5', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e6():
    """
        a0 data lost -> a0 up -> full-backup -> sha1
    """
    info = 'test_e6:primary-archive-data-lost count:%d' % g_count
    try:
        print_action_info('START', info)
        shutdown(a0, 'force')
        remove_persistent_data(a0)
        startup(a0)
        walbc.run_ctl(a0, ['init-vol', VOL])
        gid0 = walbc.snapshot_nbk(s0, VOL)
        walbc.verify_not_restorable(a0, VOL, gid0, 10, 'test_e6')
        walbc.stop(s0, VOL)
        gid1 = walbc.full_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid1)
        verify_equal_sha1('test_e6', md0, md1)
        walbc.replicate_once(a0, VOL, a1, TIMEOUT, doResync=True) # copy archive uuid.
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e7():
    """
        a1 data lost -> a1 up -> replicate -> sha1
    """
    info = 'test_e7:secondary-archive-data-lost count:%d' % g_count
    try:
        print_action_info('START', info)
        write_random(wdev0.path, 1)
        walbc.replicate(a0, VOL, a1, True, TIMEOUT)
        shutdown(a1, 'force')
        remove_persistent_data(a1)
        startup(a1)
        write_random(wdev0.path, 1)
        gid0 = walbc.snapshot_nbk(s0, VOL)
        walbc.wait_for_restorable(a0, VOL, gid0, TIMEOUT)
        walbc.verify_not_restorable(a1, VOL, gid0, 10, 'test_e7')
        walbc.replicate(a0, VOL, a1, True, TIMEOUT)
        write_random(wdev0.path, 1)
        gid1 = walbc.snapshot(s0, VOL, [a0, a1], TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid1)
        md2 = get_sha1_of_restorable(a1, VOL, gid1)
        verify_equal_sha1('test_e7:1', md0, md1)
        verify_equal_sha1('test_e7:2', md1, md2)
        walbc.stop_synchronizing(a1, VOL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e8():
    """
        s0 data lost -> s0 up -> hash-backup -> sha1
    """
    info = 'test_e8:storage-data-lost count:%d' % g_count
    try:
        print_action_info('START', info)
        write_random(wdev0.path, 1)
        shutdown(s0, 'force')
        remove_persistent_data(s0)
        startup(s0)
        write_random(wdev0.path, 1)
        walbc.run_ctl(s0, ['init-vol', VOL, wdev0.path])
        write_random(wdev0.path, 1)
        gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_e8', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def init_repeater_test(sLayoutRepeater, rL, rateMbps=0, delayMsec=0):
    shutdown_all('force')
    walbc.set_server_layout(sLayoutRepeater)
    startup_list(sLayoutRepeater.get_all(), rL, rateMbps, delayMsec)


def exit_repeater_test(rL):
    shutdown_all('force')
    quit_repeaters(rL)
    walbc.set_server_layout(sLayout)
    startup_list(sLayout.get_all())


def test_e9():
    """
        down network between s0 and p0 -> recover -> synchronizing
    """
    info = 'test_e9:network_down_and_recover_storage_proxy count:%d' % g_count
    try:
        print_action_info('START', info)
        rL = [p0]
        init_repeater_test(sLayoutRepeater1, rL)
        stop_repeater(p0)
        write_random(wdev0.path, 1)
        md0 = get_sha1(wdev0.path)
        gid = walbc.snapshot_nbk(s0, VOL)
        walbc.verify_not_restorable(a0, VOL, gid, 20, 'test_e9')
        start_repeater(p0)
        gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        restore_and_verify_sha1('test_e9', md0, a0, VOL, gid)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e10():
    """
        down network between s0 and p0 -> write overflow -> hash-backup -> synchronizing
    """
    info = 'test_e10:network_down_and_overflow count:%d' % g_count
    try:
        print_action_info('START', info)
        rL = [p0]
        init_repeater_test(sLayoutRepeater1, rL)
        stop_repeater(p0)
        write_over_wldev(wdev0, overflow=True)
        try:
            walbc.snapshot_nbk(s0, VOL)
            raise Exception('test_e10:expect wlog overflow')
        except:
            print 'test_e10:wlog overflow ok'
        start_repeater(p0)
        walbc.stop(s0, VOL)
        gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_e10', md0, md1)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def get_original_server(s):
    return Server(s.name, s.address, get_orig_port(s.port),
                  s.kind, s.binDir, s.dataDir, s.logPath, s.vg)


def test_e11():
    """
        down network between p0 and a0 -> recover -> synchronizing
    """
    info = 'test_e11:network_down_and_recover_proxy_archive count:%d' % g_count
    try:
        print_action_info('START', info)
        rL = [a0]
        init_repeater_test(sLayoutRepeater1, rL)
        with RandomWriter(wdev0.path):
            time.sleep(0.5)
            stop_repeater(a0)
            time.sleep(0.5)

        gid = walbc.snapshot_nbk(s0, VOL)
        a0x = get_original_server(a0)
        walbc.verify_not_restorable(a0x, VOL, gid, 10, 'test_e11')
        start_repeater(a0)

        walbc.kick_all([p0]) # to decrese retry interval
        walbc.wait_for_restorable(a0, VOL, gid, TIMEOUT)

        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_e11', md0, md1)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e12():
    """
        down network between p0 and a0 in full-backup -> recover -> synchronizing
    """
    info = 'test_e12:network_down_and_recover_full-backup count:%d' % g_count
    try:
        print_action_info('START', info)
        sizeMb = wdev0.get_size_mb()
        rateMbps = sizeMb * 8 / 10  # to complete full transfer in 10 sec.
        print 'test_e12:rateMbps', rateMbps
        rL = [a0]
        init_repeater_test(sLayoutRepeater1, rL, rateMbps=rateMbps)
        walbc.stop(s0, VOL)
        #zero_clear(wdev0.path, 0, wdev0.get_size_lb())
        run_local_command(['/bin/dd', 'if=/dev/urandom', 'of=' + wdev0.path, 'bs=512', 'count=' + str(wdev0.get_size_lb()), 'conv=fdatasync'])
        walbc.reset(s0, VOL)

        walbc.stop(a0, VOL)
        walbc.reset(a0, VOL)

        write_random(wdev0.path, 1)
        md0 = get_sha1(wdev0.path)
        print 'test_e12:full_backup'
        gid = walbc.full_backup(s0, VOL, TIMEOUT, block=False)
        print 'test_e12:wait 3sec'
        time.sleep(3)
        stop_repeater(a0)
        # try to full sync, it must fail.
        timeoutS = 20
        walbc._wait_for_state_change(s0, VOL, sDuringFullSync, sSyncReady, timeoutS)
        a0x = get_original_server(a0)
        walbc._wait_for_state_change(a0x, VOL, aDuringFullSync, aSyncReady, timeoutS)
        start_repeater(a0)
        gid = walbc.full_backup(s0, VOL, TIMEOUT)
        restore_and_verify_sha1('test_e12', md0, a0, VOL, gid)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e13():
    """
        down network between s0 and a0 in hash-backup -> recover -> synchronizing
    """
    info = 'test_e13:network_down_and_recover_hash-backup count:%d' % g_count
    try:
        print_action_info('START', info)
        sizeMb = wdev0.get_size_mb()
        rateMbps = sizeMb * 8 / 10  # to complete full transfer in 10 sec.
        print 'test_e13:rateMbps', rateMbps
        rL = [a0]
        init_repeater_test(sLayoutRepeater1, rL, rateMbps=rateMbps)
        walbc.stop(s0, VOL)
        walbc.reset(s0, VOL)

        sizeLb = wdev0.get_size_lb()
        print "sizeLb", sizeLb
        write_random(wdev0.path, sizeLb / 3)
        list0 = walbc.get_restorable_gid(a0, VOL, opt='all')

        md0 = get_sha1(wdev0.path)
        print 'test_e13:hash_backup'
        gid = walbc.hash_backup(s0, VOL, TIMEOUT, block=False)
        print 'test_e13:wait 1sec'
        time.sleep(1)
        list1 = walbc.get_restorable_gid(a0, VOL, opt='all')
        if list0 != list1:
            raise Exception('test_e13: not equal list', list0, list1)
        stop_repeater(a0)
        # try to hash sync, it must fail.
        timeoutS = 20
        walbc._wait_for_state_change(s0, VOL, sDuringHashSync, sSyncReady, timeoutS)
        a0x = get_original_server(a0)
        walbc._wait_for_state_change(a0x, VOL, aDuringHashSync, aArchived, timeoutS)
        start_repeater(a0)
        gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        restore_and_verify_sha1('test_e13', md0, a0, VOL, gid)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e14():
    """
        down network between a0 and a1 during full-repl -> recover -> full-repl.
    """
    info = 'test_e14:network_down_and_recover_full-repl count:%d' % g_count
    try:
        print_action_info('START', info)
        sizeMb = wdev0.get_size_mb()
        rateMbps = sizeMb * 8 / 10  # to complete full transfer in 10 sec.
        print 'test_e14:rateMbps', rateMbps
        rL = [a1]
        init_repeater_test(sLayoutRepeater2, rL, rateMbps=rateMbps)
        st = walbc.get_state(a1, VOL)
        if st in aActive:
            walbc.stop(a1, VOL)
            walbc.reset(a1, VOL)
        elif st == aClear:
            walbc.run_ctl(a1, ['init-vol', VOL])

        gid = walbc.replicate_once_nbk(a0, VOL, a1)
        print 'test_e14:wait 3sec'
        time.sleep(3)
        stop_repeater(a1)
        timeoutS = 20
        walbc._wait_for_no_action(a0, VOL, aaReplSync, timeoutS)
        a1x = get_original_server(a1)
        walbc._wait_for_state_change(a1x, VOL, aDuringReplicate, aSyncReady, timeoutS)
        start_repeater(a1)
        gid = walbc.replicate_once(a0, VOL, a1, TIMEOUT)

        md0 = get_sha1_of_restorable(a0, VOL, gid)
        md1 = get_sha1_of_restorable(a1, VOL, gid)
        verify_equal_sha1('test_e14', md0, md1)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e15():
    """
        down network between a0 and a1 during hash-repl -> recover -> hash-repl.
    """
    info = 'test_e15:network_down_and_recover_hash-repl count:%d' % g_count
    try:
        print_action_info('START', info)
        # prepare for hash-repl.
        walbc.replicate_once(a0, VOL, a1, TIMEOUT)
        write_random(wdev0.path, 1)
        gid0 = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        walbc.apply(a0, VOL, gid0, TIMEOUT)
        write_random(wdev0.path, wdev0.get_size_lb() / 3)
        gid1 = walbc.snapshot(s0, VOL, [a0], TIMEOUT)

        sizeMb = wdev0.get_size_mb()
        rateMbps = sizeMb * 8 / 10  # to complete full transfer in 10 sec.
        print 'test_e15:rateMbps', rateMbps
        rL = [a1]
        init_repeater_test(sLayoutRepeater2, rL, rateMbps=rateMbps)
        walbc.replicate_once_nbk(a0, VOL, a1)
        print 'test_e15:wait 3sec'
        time.sleep(3)
        stop_repeater(a1)
        timeoutS = 20
        walbc._wait_for_no_action(a0, VOL, aaReplSync, timeoutS)
        a1x = get_original_server(a1)
        walbc._wait_for_state_change(a1x, VOL, aDuringReplicate, aArchived, timeoutS)
        start_repeater(a1)
        gid2 = walbc.replicate_once(a0, VOL, a1, TIMEOUT)
        if gid1 != gid2:
            raise Exception('test_e15: gid1 differs gid2', gid1, gid2)

        md0 = get_sha1_of_restorable(a0, VOL, gid1)
        md1 = get_sha1_of_restorable(a1, VOL, gid1)
        verify_equal_sha1('test_e15', md0, md1)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_e16():
    """
        down network between a0 and a1 during diff-repl -> recover -> diff-repl.
    """
    info = 'test_e16:network_down_and_recover_full-repl count:%d' % g_count
    try:
        print_action_info('START', info)
        write_random(wdev0.path, wdev0.get_size_lb() / 3)
        gid0 = walbc.snapshot(s0, VOL, [a0], TIMEOUT)

        sizeMb = wdev0.get_size_mb()
        rateMbps = sizeMb * 8 / 10  # to complete full transfer in 10 sec.
        print 'test_e16:rateMbps', rateMbps
        rL = [a1]
        init_repeater_test(sLayoutRepeater2, rL, rateMbps=rateMbps)
        walbc.replicate_once_nbk(a0, VOL, a1)
        print 'test_e16:wait 3sec'
        time.sleep(3)
        stop_repeater(a1)
        timeoutS = 20
        walbc._wait_for_no_action(a0, VOL, aaReplSync, timeoutS)
        a1x = get_original_server(a1)
        walbc._wait_for_state_change(a1x, VOL, aDuringReplicate, aArchived, timeoutS)
        gid1 = walbc.get_latest_clean_snapshot(a1x, VOL)
        if gid0 == gid1:
            raise Exception('test_e16: must not be equal', gid0, gid1)
        start_repeater(a1)
        gid2 = walbc.replicate_once(a0, VOL, a1, TIMEOUT)
        if gid0 != gid2:
            raise Exception('test_e16: must be equal', gid0, gid2)

        md0 = get_sha1_of_restorable(a0, VOL, gid2)
        md1 = get_sha1_of_restorable(a1, VOL, gid2)
        verify_equal_sha1('test_e16', md0, md1)

        exit_repeater_test(rL)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def create_dummy_diff(devSizeB, logSizeB):
    '''
    devSizeB :: int          - device size [byte]
    logSizeB :: int          - log size [byte]
    return :: TemporaryFile  - written diff file.

    '''
    verify_type(devSizeB, int)
    verify_type(logSizeB, int)
    wlog = tempfile.NamedTemporaryFile(dir=workDir)
    wdiff = tempfile.NamedTemporaryFile(dir=workDir)
    run_local_command([binDir + 'wlog-gen',
                       '-s', str(devSizeB),
                       '-z', str(logSizeB),
                       '-o', wlog.name], putMsg=True)
    run_local_command([binDir + 'wlog-to-wdiff',
                       '-i', wlog.name,
                       '-o', wdiff.name], putMsg=True)
    wlog.close()
    wdiff.flush()
    return wdiff


def try_to_send_dummy_diff_and_verify(
        ax, vol, sizeB, wdiffPath, gid0, gid1, uuid, verifyMsg):
    '''
    ax        - archive server
    vol       - volume identifier
    sizeB     - volume size [byte]
    wdiffPath - walb diff path to send.
    gid0      - begin gid
    gid1      - end gid
    uuid      - uuid of the volume.
    verifyMsg - message to verify.

    '''
    verify_type(ax, Server)
    verify_server_kind(ax, [K_ARCHIVE])
    verify_type(vol, str)
    verify_type(sizeB, int)
    verify_type(wdiffPath, str)
    verify_type(gid0, int)
    verify_type(gid1, int)
    verify_type(uuid, str)
    verify_type(verifyMsg, str)

    args = [binDir + 'wdiff-send',
            ax.address, str(ax.port), vol, str(sizeB), wdiffPath,
            '-g0', str(gid0),
            '-g1', str(gid1),
            '-uuid', uuid,
            '-time', datetime.datetime.today().strftime('%Y%m%d%H%M%S'),
            '-merge', '0',
            '-msg', verifyMsg]
    run_local_command(args, putMsg=True)


def test_e17():
    """
        execute wdiff-transfer twice for a wdiff file and
        confirm ot be rejected with 'too-old-diff' message.
    """
    info = 'test_e17:wdiff-transfer-twice count:%d' % g_count
    try:
        print_action_info('START', info)

        gid0 = walbc.get_latest_clean_snapshot(a0, VOL)
        gid1 = gid0 + 1
        devSizeB = wdev0.get_size_lb() * Lbs
        logSizeB = Mebi
        uuid = walbc.get_uuid(a0, VOL)
        uuidx = walbc.get_uuid(s0, VOL)
        if uuid != uuidx:
            raise Exception('test_e17: uuid differ', uuid, uuidx)

        wdiff = create_dummy_diff(devSizeB, logSizeB)
        try_to_send_dummy_diff_and_verify(
            a0, VOL, devSizeB, wdiff.name, gid0, gid1, uuid, 'accept')
        try_to_send_dummy_diff_and_verify(
            a0, VOL, devSizeB, wdiff.name, gid0, gid1, uuid, 'too-old-diff')
        gid1x = walbc.get_latest_clean_snapshot(a0, VOL)
        if gid1 != gid1x:
            raise Exception('test_e17: bad latest gid', gid1x, gid1)

        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


###############################################################################
# Replacement scenario tests.
###############################################################################


def test_r1():
    """
        replace s0 by s2
    """
    info = 'test_r1:replace-storage count:%d' % g_count
    try:
        print_action_info('START', info)
        startup(s2)
        walbc.init_storage(s2, VOL, wdev2.path)
        resize_storage_if_necessary(s2, VOL, wdev2, wdev0.get_size_mb())

        walbc.go_standby(s0, VOL)
        write_random(wdev2.path, 1)
        gid = walbc.hash_backup(s2, VOL, TIMEOUT)
        md0 = get_sha1(wdev2.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_r1:0', md0, md1)
        walbc.clear(s0, VOL)

        # turn back to the beginning state.
        walbc.init_storage(s0, VOL, wdev0.path)
        walbc.go_standby(s2, VOL)
        write_random(wdev0.path, 1)
        gid = walbc.hash_backup(s0, VOL, TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_r1:1', md0, md1)

        walbc.clear(s2, VOL)
        shutdown(s2)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


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
        walbc.stop_nbk(pDel, vol, 'empty')
    for vol in volL:
        walbc.wait_for_stopped(pDel, vol)
        walbc.clear(pDel, vol)
    shutdown(pDel)

    walbc.set_server_layout(newServerLayout)
    for sx in newServerLayout.storageL:
        shutdown(sx)
        startup(sx)


def test_r2():
    """
        replace p0 by p2
    """
    info = 'test_r2:replace-proxy count:%d' % g_count
    try:
        print_action_info('START', info)
        sLayout2 = sLayout.replace(proxyL=[p2, p1])
        replace_proxy(p0, p2, [VOL], sLayout2)
        write_random(wdev0.path, 1)
        gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_r2:0', md0, md1)

        # turn back to the beginning state.
        replace_proxy(p2, p0, [VOL], sLayout)
        write_random(wdev0.path, 1)
        gid = walbc.snapshot(s0, VOL, [a0], TIMEOUT)
        md0 = get_sha1(wdev0.path)
        md1 = get_sha1_of_restorable(a0, VOL, gid)
        verify_equal_sha1('test_r2:1', md0, md1)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def replace_archive(aDel, aAdd, volL, newServerLayout):
    isSyncL = []
    for vol in volL:
        isSyncL.append(walbc.is_synchronizing(aDel, vol))
    startup(aAdd)
    for vol, isSync in zip(volL, isSyncL):
        walbc.replicate(aDel, vol, aAdd, isSync, TIMEOUT)
        if isSync:
            walbc.stop_synchronizing(aDel, vol)
        walbc.clear(aDel, vol)
    shutdown(aDel)

    walbc.set_server_layout(newServerLayout)
    for sx in newServerLayout.storageL:
        shutdown(sx)
        startup(sx)


def test_replace_archive_synchronizing(ax, ay, newServerLayout):
    '''
    Replace ax by ay.
    ax :: Server        - must be synchronizing.
    ay :: Server        - must not be started.
    newServerLayout :: ServerLayout
    '''
    if not walbc.is_synchronizing(ax, VOL):
        raise Exception('test_replace_archive_synchronizing: not synchronizing', ax, ay)
    replace_archive(ax, ay, [VOL], newServerLayout)
    write_random(wdev0.path, 1)
    gid = walbc.snapshot(s0, VOL, [ay], TIMEOUT)
    md0 = get_sha1(wdev0.path)
    md1 = get_sha1_of_restorable(ay, VOL, gid)
    print 'test_replace_archive_synchronizing', ax, ay
    verify_equal_sha1('test_replace_archive_synchronizing:', md0, md1)


def test_replace_archive_nosynchronizing(ax, ay, newServerLayout):
    '''
    replace ax by ay.
    ax must not be synchronizing.
    ay must not be started.
    '''
    if walbc.is_synchronizing(ax, VOL):
        raise Exception('test_replace_archive_nosynchronizing: synchronizing', ax, ay)
    md0 = get_sha1_of_restorable(
        ax, VOL, walbc.get_latest_clean_snapshot(ax, VOL))
    replace_archive(ax, ay, [VOL], newServerLayout)
    write_random(wdev0.path, 1)
    gid = walbc.get_latest_clean_snapshot(ay, VOL)
    md1 = get_sha1_of_restorable(ay, VOL, gid)
    verify_equal_sha1('test_replace_archive_nosynchronizing', md0, md1)


def test_r3():
    """
        replace a0 by a2
    """
    info = 'test_r3:replace-primary-archive count:%d' % g_count
    try:
        print_action_info('START', info)
        sLayout2 = sLayout.replace(archiveL=[a2, a1])
        test_replace_archive_synchronizing(a0, a2, sLayout2)
        test_replace_archive_synchronizing(a2, a0, sLayout)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def test_r4():
    """
        replace a1 by a2
    """
    info = 'test_r4:replace-secondary-archive count:%d' % g_count
    try:
        print_action_info('START', info)
        isSync = walbc.is_synchronizing(a1, VOL)
        if isSync:
            raise Exception('test_r4:a1 synchronizing', a1)

        walbc.replicate(a0, VOL, a1, False, TIMEOUT)  # preparation

        sLayout2 = sLayout.replace(archiveL=[a0, a2])
        test_replace_archive_nosynchronizing(a1, a2, sLayout2)
        test_replace_archive_nosynchronizing(a2, a1, sLayout)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


###############################################################################
# Main
###############################################################################


allL = ['n1', 'n2', 'n3', 'n4b', 'n5', 'n6', 'n7', 'n8', 'n9',
        'n10', 'n11a', 'n11b', 'n12', 'n13', 'n14',
        'm1', 'm2', 'm3',
        'e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7', 'e8',
        'e9', 'e10', 'e11', 'e12', 'e13',
        'e14', 'e15', 'e16', 'e17',
        'r1', 'r2', 'r3', 'r4',
       ]


def main():
    """
        [-c count] [-tp] [n1 n2 ...]
    """
    i = 1
    argv = " ".join(sys.argv).split()
    args = len(argv)
    count = 1
    useTp = False
    testL = []
    while i < args:
        c = argv[i]
        if c == "-tp":
            useTp = True
        elif i + 1 < args and c == "-c":
            count = int(argv[i + 1])
            i += 1
        else:
            testL.append(c)
        i += 1
    if not testL:
        testL = allL

    print "count", count
    print "test", testL
    print "useTp", useTp
    setup_test(useTp)
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
