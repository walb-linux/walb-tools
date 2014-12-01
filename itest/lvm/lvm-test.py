import sys
sys.path.append("../")
from run import *

'''
lvm test.

'''
VG = 'test'  # volume group name.
TP = 'pool'  # thin provision pool or None.
BIN = 'sudo ../../binsrc/'
VOL = 'vol'
SNAP = 'vol_s'


def get_tp_opt():
    if TP is None:
        return '-tp %s' % TP
    else:
        return ''


def get_lvm_path(name):
    return '/dev/' + VG + '/' + name


def non_tp_test():
    run(BIN + 'lvm-mgr create -vg %s -s 12M %s' % (VG, VOL))
    run('sudo dd if=/dev/zero of=%s oflag=direct bs=1M count=12' % get_lvm_path(VOL))
    run(BIN + 'lvm-mgr snap -vg %s -lv %s %s' % (VG, VOL, SNAP))
    run(BIN + 'lvm-mgr resize -vg %s -s 24M %s' % (VG, SNAP))
    run(BIN + 'lvm-mgr remove -vg %s %s' % (VG, SNAP))
    run(BIN + 'lvm-mgr resize -vg %s -s 24M %s' % (VG, VOL))
    run(BIN + 'lvm-mgr remove -vg %s %s' % (VG, VOL))


def tp_test():
    run(BIN + 'lvm-mgr exists-tp -vg %s -tp %s' % (VG, TP))
    run(BIN + 'lvm-mgr create -vg %s -tp %s -s 12M %s' % (VG, TP, VOL))
    run('sudo dd if=/dev/zero of=%s oflag=direct bs=1M count=12' % get_lvm_path(VOL))
    run(BIN + 'lvm-mgr snap -vg %s -lv %s %s' % (VG, VOL, SNAP))
    run(BIN + 'lvm-mgr resize -vg %s -s 24M %s' % (VG, VOL))
    run(BIN + 'lvm-mgr resize -vg %s -s 24M %s' % (VG, SNAP))
    run(BIN + 'lvm-mgr remove -vg %s %s' % (VG, SNAP))
    run(BIN + 'lvm-mgr remove -vg %s %s' % (VG, VOL))


if __name__ == '__main__':
    non_tp_test()
    if TP is not None:
        tp_test()
