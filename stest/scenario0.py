import os
import itertools
from walb_cmd import *


s0 = Server('s0', '10000', K_STORAGE, None)
s1 = Server('s1', '10001', K_STORAGE, None)
s2 = Server('s2', '10002', K_STORAGE, None)
p0 = Server('p0', '10100', K_PROXY, None)
p1 = Server('p1', '10101', K_PROXY, None)
p2 = Server('p2', '10102', K_PROXY, None)
a0 = Server('a0', '10200', K_ARCHIVE, 'vg0')
a1 = Server('a1', '10201', K_ARCHIVE, 'vg1')
#a2 = Server('a2', '10202', None)

WORK_DIR = os.getcwd() + '/stest/tmp/'

config = Config(False, os.getcwd() + '/binsrc/', WORK_DIR, [s0, s1], [p0, p1], [a0, a1])

WDEV_PATH = '/dev/walb/0'
VOL = 'vol0'

set_config(config)

def setup_test():
    run_command(['/bin/rm', '-rf', WORK_DIR])
    if os.path.isdir('/dev/vg0'):
        for f in os.listdir('/dev/vg0'):
            if f[0] == 'i':
                run_command(['/sbin/lvremove', '-f', '/dev/vg0/' + f])
    make_dir(WORK_DIR)
    kill_all_servers()
    startup_all()


def test_n1():
    """
        full-backup -> sha1 -> restore -> sha1
    """
    print "test_n1:full-backup"
    init(s0, VOL, WDEV_PATH)
    write_random(WDEV_PATH, 1)
    md0 = get_sha1(WDEV_PATH)
    gid = full_backup(s0, VOL)
    restore_and_verify_sha1('test_n1', md0, a0, VOL, gid)

def test_n2():
    """
        write -> sha1 -> snapshot -> restore -> sha1
    """
    print "test_n2:snapshot"
    write_random(WDEV_PATH, 1)
    md0 = get_sha1(WDEV_PATH)
    gid = snapshot_sync(s0, VOL, [a0])
    print "gid=", gid
    print list_restorable(a0, VOL)
    restore_and_verify_sha1('test_n2', md0, a0, VOL, gid)

def test_n3():
    """
        hash-backup -> sha1 -> restore -> sha1
    """
    print "test_n3:hash-backup"
    set_slave_storage(s0, VOL)
    write_random(WDEV_PATH, 1)
    md0 = get_sha1(WDEV_PATH)
    gid = hash_backup(s0, VOL)
    print "gid=", gid
    restore_and_verify_sha1('test_n3', md0, a0, VOL, gid)

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
    t = startWriting(WDEV_PATH)

    for s in stopL:
        stop(s, VOL)
        time.sleep(0.1)

    time.sleep(0.5)

    for s in startL:
        start(s, VOL)
        time.sleep(0.1)

    stopWriting(t)

    md0 = get_sha1(WDEV_PATH)
    gid = snapshot_sync(s0, VOL, [a0])
    restore_and_verify_sha1('test_stop', md0, a0, VOL, gid)


def test_n4():
    """
        stop -> start -> snapshot -> sha1
    """
    print "test_n4:stop"
    setLL = [[s0], [p0], [a0], [s0, p0], [s0, a0], [p0, a0], [s0, p0, a0]]
    for setL in setLL:
        for stopL in itertools.permutations(setL):
            for startL in itertools.permutations(setL):
                test_stop(stopL, startL)
#                printL(stopL, startL)

def main():
    setup_test()
    test_n1()
#    test_n2()
#    test_n3()
    test_n4()

if __name__ == "__main__":
    main()
