#!/usr/bin/env python

import sys
import os
import time
from util import *

BIN = os.path.dirname(sys.argv[0]) + '/binsrc/'


def get_wdev_path(wdevId):
    '''
    wdevId :: int - walb device id.
    return :: str - walb device path.
    '''
    return '/dev/walb/%d' % wdevId


def get_wldev_path(wdevId):
    '''
    wdevId :: int - walb device id.
    return :: str - walb log device path.
    '''
    return '/dev/walb/L%d' % wdevId


def get_lsids(wdevId):
    '''
    wdevId :: int - walb device id.
    return :: {str: int} - key is lsid name, value is lsid.
    '''
    def create_key_value(ls):
        ret = []
        for s in ls:
            (k, v) = s.strip().split()
            ret.append((k, int(v)))
        return dict(ret)

    path = '/sys/block/walb!%d/walb/lsids' % wdevId
    out = run_local_command(['/bin/cat', path]).strip()
    return create_key_value(out.split('\n'))


LsidKindList = ['latest', 'flush' 'completed', 'permanent', 'written', 'prev_written', 'oldest']

def verify_lsid_kind(kind):
    '''
    kind :: str - lsid kind
    '''
    return kind in LsidKindList


def wait_for_lsid(wdevId, kind, lsid, timeoS):
    '''
    wdevId :: int   - walb device id.
    kind :: str     - kind.
    lsid :: int     - lsid.
    timeoS :: int - timeout [sec].
    '''
    verify_type(wdevId, int)
    verify_lsid_kind(kind)
    verify_type(lsid, int)
    verify_type(timeoS, int)

    t0 = time.time()
    while time.time() < t0 + timeoS:
        if get_lsids(wdevId)[kind] >= lsid:
            return
        time.sleep(0.3)
    raise Exception('wait_for_perment')


def is_log_overflow(wdevId):
    '''
    wdevId :: int  - walb device id.
    return :: bool - True if overflow.
    '''
    ret = run_local_command([BIN + 'wdevc', 'is-log-overflow', get_wdev_path(wdevId)])
    return int(ret) != 0


def get_wlog_file_name(wdevId, lsid0, lsid1):
    '''
    wdevId :: str - walb device id.
    lsid0 :: int  - begin lsid.
    lsid1 :: int  - end lsid.
    return :: str - file name.
    '''
    return '%d-%d-%d.wlog' % (wdevId, lsid0, lsid1)


def get_oldest_latest_lsid(wdevId):
    '''
    wdevId :: int       - walb device id
    return :: (int,int) - oldest,latest lsid pair.
    '''
    verify_type(wdevId, int)
    lsids = get_lsids(wdevId)
    return lsids['oldest'], lsids['latest']


def force_checkpoint_if_necessary(wdevId, lsid1, timeoS):
    '''
    wdevId :: int - walb device id
    lsid1 :: int  - lsid1 to wait.
    '''
    wait_for_lsid(wdevId, 'written', lsid1, timeoS)
    lsids = get_lsids(wdevId)
    if lsids['prev_written'] < lsid1:
        run_local_command([BIN + 'wdevc', 'force-checkpoint', get_wdev_path(wdevId)])
    wait_for_lsid(wdevId, 'prev_written', lsid1, timeoS)


def main():
    wdevId = int(sys.argv[1])
    intervalS = 1
    lsid0, lsid1 = get_oldest_latest_lsid(wdevId)

    while True:
        if lsid0 == lsid1:
            print 'there are no wlogs to extract', lsid0, lsid1, time.ctime()
            time.sleep(intervalS)
            lsid0, lsid1 = get_oldest_latest_lsid(wdevId)
            continue
        if is_log_overflow(wdevId):
            raise Exception('main: walb device log overflow')

        print 'extracting', lsid0, lsid1, '...'
        wlogFileName = get_wlog_file_name(wdevId, lsid0, lsid1)
        tmpFileName = wlogFileName + '.tmp'
        wait_for_lsid(wdevId, 'permanent', lsid1, timeoS=10)
        run_local_command([BIN + 'wlog-cat', '-b', str(lsid0), '-e', str(lsid1),
                           '-o', tmpFileName, '-s', get_wldev_path(wdevId)])
        run_local_command(['/bin/mv', tmpFileName, wlogFileName])
        run_local_command(['/usr/bin/pigz', '-f', wlogFileName])
        force_checkpoint_if_necessary(wdevId, lsid1, 10)
        run_local_command([BIN + 'wdevc', 'set-oldest-lsid', get_wdev_path(wdevId), str(lsid1)])

        lsid0, lsid1 = get_oldest_latest_lsid(wdevId)


if __name__ == '__main__':
    main()
