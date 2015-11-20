#!/usr/bin/env python

import sys
sys.path.insert(0, './python')
from walblib import *

import random, traceback, signal
from stest_util import *
from repeater import *
from common import *

'''
These variables are defined for pylint.
They will be set by set_config().
These must be read-only.
'''
walbc = None
VOL0 = None
VOL1 = None
VOL2 = None
s0 = None
p0 = None
a0 = None
wdev0 = None
wdev1 = None
wdev2 = None

import config1
set_config(globals(), config1.get_config())

g_count = 0


################################################################################
# Scenarios
################################################################################


def do_full_backup(threadId, count, vol, wdev):
    try:
        info = 'full_backup thread:%d count:%d %s %s' % (threadId, count, vol, wdev.path)
        print_action_info('START', info)
        if walbc.get_state(s0, vol) == sClear:
            walbc.init_storage(s0, vol, wdev.path)
        else:
            walbc.stop(s0, vol)
            if walbc.get_state(a0, vol) != aClear:
                walbc.clear(a0, vol)

        write_random(wdev.path, 1)
        md0 = get_sha1(wdev.path)
        gid = walbc.full_backup(s0, vol, TIMEOUT)
        print 'gid=', gid
        restore_and_verify_sha1(info, md0, a0, vol, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def do_continuous_backup(threadId, count, vol, wdev):
    try:
        info = 'continuous_backup thread:%d count:%d %s %s' % (threadId, count, vol, wdev.path)
        print_action_info('START', info)
        write_random(wdev.path, 1)
        md0 = get_sha1(wdev.path)
        gid = walbc.snapshot(s0, vol, [a0], TIMEOUT)
        print 'gid=', gid
        walbc.print_restorable(a0, vol)
        restore_and_verify_sha1(info, md0, a0, vol, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def do_hash_backup(threadId, count, vol, wdev):
    try:
        info = 'hash_backup thread:%d count:%d %s %s' % (threadId, count, vol, wdev.path)
        print_action_info('START', info)
        walbc.go_standby(s0, vol)
        write_random(wdev.path, 1)
        md0 = get_sha1(wdev.path)
        gid = walbc.hash_backup(s0, vol, TIMEOUT)
        print 'gid=', gid
        restore_and_verify_sha1(info, md0, a0, vol, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def do_merge(threadId, count, vol, wdev):
    try:
        info = 'merge thread:%d count:%d %s %s' % (threadId, count, vol, wdev.path)
        print_action_info('START', info)
        with RandomWriter(wdev.path):
            time.sleep(0.5)
            gidB = walbc.snapshot(s0, vol, [a0], TIMEOUT)
            time.sleep(1)
            walbc.stop(s0, vol)
            walbc.stop(p0, vol, 'empty')
            walbc.start(s0, vol)
            walbc.start(p0, vol)
            time.sleep(1)
            gidE = walbc.snapshot(s0, vol, [a0], TIMEOUT)
            gidL = walbc.get_restorable_gid(a0, vol, 'all')
            posB = gidL.index(gidB)
            posE = gidL.index(gidE)
            print 'gidB', gidB, 'gidE', gidE, 'gidL', gidL
            if posE - posB < 2:
                raise Exception('bad range', info)
            time.sleep(0.5)
        md0 = get_sha1_of_restorable(a0, vol, gidE)
        walbc.merge(a0, vol, gidB, gidE, TIMEOUT)
        print 'merged gidL', walbc.get_restorable_gid(a0, vol, 'all')
        restore_and_verify_sha1(info, md0, a0, vol, gidE)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def do_apply(threadId, count, vol, wdev):
    try:
        info = 'apply thread:%d count:%d %s %s' % (threadId, count, vol, wdev.path)
        print_action_info('START', info)
        with RandomWriter(wdev.path):
            time.sleep(0.5)
            gid = walbc.snapshot(s0, vol, [a0], TIMEOUT)
            time.sleep(0.5)
        md0 = get_sha1_of_restorable(a0, vol, gid)
        walbc.apply(a0, vol, gid, TIMEOUT)
        restore_and_verify_sha1(info, md0, a0, vol, gid)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise


def do_stop_start(threadId, count, vol, wdev):
    try:
        info = 'stop_start thread:%d count:%d %s %s' % (threadId, count, vol, wdev.path)
        print_action_info('START', info)
        s = random.choice(walbc.sLayout.get_all())
        walbc.stop(s, vol)
        time.sleep(0.5)
        walbc.start(s, vol)
        print_action_info('SUCCESS', info)
    except Exception:
        print_action_info('FAILURE', info)
        raise



################################################################################
# main
################################################################################

ActionList = [do_full_backup, do_continuous_backup, do_hash_backup, do_merge, do_apply, do_stop_start]

targetVolL = [VOL0, VOL1, VOL2]
targetWdevL = [wdev0, wdev1, wdev2]


class TestWorker(object):
    '''
    Test worker for concurrent running.
    '''
    def __init__(self, threadId, count, vol, wdev):
        '''
        threadId :: int
        count :: int - loop times.
        vol :: str
        wdev :: Device
        '''
        verify_type(threadId, int)
        verify_type(count, int)
        verify_type(vol, str)
        verify_type(wdev, Device)
        self.threadId = threadId
        self.count = count
        self.vol = vol
        self.wdev = wdev

    def run(self):
        '''
        Main thread procedure.
        '''
        try:
            do_full_backup(self.threadId, -1, self.vol, self.wdev)
            for i in xrange(self.count):
                action = random.choice(ActionList)
                action(self.threadId, i, self.vol, self.wdev)
        except Exception:
            print traceback.format_exc()
            os._exit(1)


def main():
    '''
    [-c count] [-tp] [-th threads]
    '''
    i = 1
    argv = ' '.join(sys.argv).split()
    argc = len(argv)
    count = 10
    useTp = False
    nrThreads = 3
    while i < argc:
        c = argv[i]
        if c == '-tp':
            useTp = True
        elif i + 1 < argc and c == '-c':
            i += 1
            count = int(argv[i])
        elif i + 1 < argc and c == '-th':
            i += 1
            nrThreads = int(argv[i])
            if nrThreads > 3:
                raise Exception('scenario1:number of threads must be up to 3', nrThreads)
        else:
            raise Exception('scenario1:bad argument', i, c)
        i += 1
    print 'count', count
    print 'useTp', useTp
    print 'threads', nrThreads
    setup_test(useTp)
    startup_all()
    threads = []
    i = 0
    for vol, wdev in zip(targetVolL, targetWdevL)[:nrThreads]:
        worker = TestWorker(i, count, vol, wdev)
        th = threading.Thread(target=worker.run)
        th.setDaemon(True)
        th.start()
        threads.append(th)
        i += 1
    for th in threads:
        th.join()


if __name__ == '__main__':
    main()
