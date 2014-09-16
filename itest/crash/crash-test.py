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


LDEV size requirements:
  size > (BLOCK_SIZE * NR_THREADS * (1000 / IO_INTERVAL_MS)) * 2
  size > (BLOCK_SIZE * NR_THREADS_REORDER * (1000 / IO_INTERVAL_MS)) * 2

DDEV size requirements:
  size > BLOCK_SIZE * NR_THREADS
  size > BLOCK_SIZE * NR_THREADS_REORDER

'''

LDEV = '/dev/crashblk0'
DDEV = '/dev/crashblk1'
WDEV_NAME = 0  # walb device name.
NR_THREADS = 8  # number of threads when reorder flag is off.
NR_THREADS_REORDER = 128  # number of threads when reorder flag is on.

BLOCK_SIZE = '512' # can be '4K' or so.
FLUSH_INTERVAL_MS = 5  # flush interval [ms]
IO_INTERVAL_MS = 1  # write interval [ms]

BIN = 'sudo ../../binsrc/'
WDEV = '/dev/walb/%s' % WDEV_NAME

def do_write_expr(crashDev, mode, isOverlap, lostPct, reorder):
    '''
    This test will check checkpointing and redo functionalities
    of walb devices will work well.

    crashDev :: str   - crash device.
    mode :: str       - 'crash' or 'write-error' or 'rw-error'.
    isOverlap :: bool - True to run overlap test.
    lostPct :: int    - lost percentage in crash. [0,100].
    reorder :: bool   - True if you want to reorder IOs.

    '''
    for bdev in [LDEV, DDEV]:
        run(BIN + 'crashblkc set-reorder %s %d' % (bdev, 1 if reorder else 0))
    run(BIN + 'crashblkc set-lost-pct %s %d' % (crashDev, lostPct))
    run(BIN + 'wdevc format-ldev %s %s > /dev/null' % (LDEV, DDEV))
    run(BIN + 'wdevc create-wdev %s %s -n %d > /dev/null' % (LDEV, DDEV, WDEV_NAME))

    timeoutS = 5
    if isOverlap:
        opt = '-ol'
    else:
        opt = '-nr %d' % (NR_THREADS_REORDER if reorder else NR_THREADS)
    proc = run_async(BIN + 'crash-test write %s -to %d -bs %s -ii %d -fi %d %s > %s'
                     % (opt, timeoutS, BLOCK_SIZE, IO_INTERVAL_MS, FLUSH_INTERVAL_MS, WDEV, 'write.log'))
    time.sleep(2)
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
    run(BIN + 'crash-test read %s -bs %s %s > read.log' % (opt, BLOCK_SIZE, WDEV))
    run(BIN + 'wdevc delete-wdev %s' % WDEV)
    if isOverlap:
        opt = '-ol'
    else:
        opt = ''
    proc = run_async(BIN + 'crash-test verify %s %s %s'
                     % (opt, 'write.log', 'read.log'))
    if not proc.wait():
        raise Exception('TEST_FAILURE write_test', crashDev, mode, isOverlap, lostPct)
    print 'TEST_SUCCESS write_test', crashDev, mode, isOverlap, lostPct


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


def write_first_block(devPath):
    '''
    devPath :: str
    return :: bool
    '''
    try:
        run('sudo dd if=/dev/zero of=%s oflag=direct bs=4096 count=1' % devPath)
        return True
    except:
        return False


def do_read_expr(crashDev, canRead):
    '''
    Assemble -> crash log or data device -> try to read.

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


def wait_for_written(wdevName, timeoutS=10, intervalS=0.5):
    '''
    wdevName :: str  - walb device name.
    timeoutS :: int  - timeout [sec]
    intervalS :: int - interval [sec]

    '''
    def get_lsids(wdevName):
        m = {}
        with open('/sys/block/walb!%s/walb/lsids' % wdevName, 'r') as f:
            for line in f.read().split('\n'):
                if len(line) == 0:
                    continue
                name, lsid = line.split()
                m[name] = int(lsid)
        return m

    m = get_lsids(wdevName)
    lsid0 = m['latest']
    lsid1 = m['written']
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        if lsid1 >= lsid0:
            return
        print 'wait_for_written: latest %d written %d' % (lsid0, lsid1)
        time.sleep(intervalS)
        lsid1 = get_lsids(wdevName)['written']
    raise Exception('wait_for_written: timeout', wdevName, timeoutS)


def do_write_read_expr():
    '''
    (1) Make the data device read-error state.
    (2) Write a block.
    (3) Wait for the block data will be evicted from its pending data.
    (4) Try to read the block and verify its failure.

    '''
    run(BIN + 'wdevc format-ldev %s %s > /dev/null' % (LDEV, DDEV))
    run(BIN + 'wdevc create-wdev %s %s -n %s > /dev/null'
        % (LDEV, DDEV, WDEV_NAME))

    run(BIN + 'crashblkc io-error %s r' % DDEV)
    write_first_block(WDEV)
    wait_for_written(WDEV_NAME)

    ret = read_first_block(WDEV)
    if ret:
        raise Exception('TEST_FAILURE write_read_test read must fail')

    run(BIN + 'crashblkc recover %s' % DDEV)
    run(BIN + 'wdevc delete-wdev %s' % WDEV)
    print 'TEST_SUCCESS write_read_test'


if __name__ == '__main__':

    for reorder in [False, True]:
        for lostPct in [100, 90, 50]:
            for isOverlap in [False, True]:
                for mode in ['crash', 'write-error', 'rw-error']:
                    print 'write_test', LDEV, mode, isOverlap, lostPct, reorder
                    do_write_expr(LDEV, mode, isOverlap, lostPct, reorder)
                    print 'write_test', DDEV, mode, isOverlap, lostPct, reorder
                    do_write_expr(DDEV, mode, isOverlap, lostPct, reorder)

    do_read_expr(LDEV, True)
    do_read_expr(DDEV, False)
    do_write_read_expr()
