import sys
sys.path.append("../")
from run import *

'''
crash test of walb module.

What to do:

1. load crashblk and walb module.
2. put crashblkc executable or its symlink in ../../binsrc/ directory.
3. prepare two crashblk devices LDEV and DDEV.
4. prepare crashblkc, wdevc, and crash-test executable binaries
   and set WDEVC, CRASHBLKC, and CRASH_TEST variable.
5. run this script.

Parameters:

WDEV_NAME: walb device name.
NR_THREADS: number of threads.
'''

LDEV = '/dev/crashblk0'
DDEV = '/dev/crashblk1'
WDEV_NAME = 0
NR_THREADS = 8

BIN = 'sudo ../../binsrc/'
WDEV = '/dev/walb/%s' % WDEV_NAME

def do_write_expr(crashDev, mode, isOverlap):
    '''
    crashDev :: str   - crash device.
    mode :: str       - 'crash' or 'write-error' or 'rw-error'.
    isOverlap :: bool - True to run overlap test.

    '''
    run(BIN + 'wdevc format-ldev %s %s > /dev/null' % (LDEV, DDEV))
    run(BIN + 'wdevc create-wdev %s %s -n %d > /dev/null' % (LDEV, DDEV, WDEV_NAME))

    timeoutS = 10
    if isOverlap:
        opt = '-ol'
    else:
        opt = '-nr %d' % NR_THREADS
    proc = run_async(BIN + 'crash-test write %s -to %d %s > %s'
                     % (opt, timeoutS, WDEV, 'write.log'))
    time.sleep(3)
    if mode == 'crash':
        run(BIN + 'crashblkc crash %s' % crashDev)
    else:
        run(BIN + 'crashblkc io-error %s %s' %
            (crashDev, ('w' if mode == 'write-error' else 'rw')))
    if not proc.wait():
        raise EnvironmentError(proc.args)

    run(BIN + 'wdevc delete-wdev %s' % WDEV)
    run(BIN + 'crashblkc recover %s' % crashDev)
    run(BIN + 'wdevc create-wdev %s %s > /dev/null' % (LDEV, DDEV))
    run(BIN + 'crash-test read %s %s > read.log' % (opt, WDEV))
    run(BIN + 'wdevc delete-wdev %s' % WDEV)
    if isOverlap:
        opt = '-ol'
    else:
        opt = ''
    proc = run_async(BIN + 'crash-test verify %s %s %s'
                     % (opt, 'write.log', 'read.log'))
    if not proc.wait():
        raise Exception('TEST_FAILURE write_test', crashDev, mode, isOverlap)
    print 'TEST_SUCCESS write_test', crashDev, mode


def read_first_block(devPath):
    '''
    devPath :: str
    return :: bool
    '''
    try:
        run('sudo dd if=%s of=/dev/null iflag=direct bs=4096 count=1' % devPath)
        return True
    except:
        return False


def do_read_expr(crashDev, canRead):
    '''
    crashDev :: str - crash device.
    canRead :: bool - True if read must success, or False.
    '''

    run(BIN + 'wdevc format-ldev %s %s > /dev/null' % (LDEV, DDEV))
    run(BIN + 'wdevc create-wdev %s %s -n %s > /dev/null'
        % (LDEV, DDEV, WDEV_NAME))
    ret = read_first_block(WDEV)
    if not ret:
        raise Exception('TEST_FAILURE read_test read must success')

    run(BIN + 'crashblkc io-error %s r' % crashDev)

    ret = read_first_block(WDEV)
    if ret != canRead:
        raise Exception('TEST_FAILURE read_test read must %s' % ('success' if canRead else 'fail'))

    run(BIN + 'crashblkc recover %s' % crashDev)

    ret = read_first_block(WDEV)
    if not ret:
        raise Exception('TEST_FAILURE read_test read must success')

    run(BIN + 'wdevc delete-wdev %s' % WDEV)
    print 'TEST_SUCCESS read_test', crashDev, canRead


if __name__ == '__main__':

    for isOverlap in [False, True]:
        for mode in ['crash', 'write-error', 'rw-error']:
            print 'write_test', LDEV, mode, isOverlap
            do_write_expr(LDEV, mode, isOverlap)
            print 'write_test', DDEV, mode, isOverlap
            do_write_expr(DDEV, mode, isOverlap)

    do_read_expr(LDEV, True)
    do_read_expr(DDEV, False)
