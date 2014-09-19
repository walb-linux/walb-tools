import sys
sys.path.append("../")
from run import *

'''
crash test of walb module.

What to do:

1. load crashblk and walb module.
2. put crashblkc executable or its symlink in ../../binsrc/ directory.
3. prepare crashblkc, wdevc, and crash-test executable binaries
   and set WDEVC, CRASHBLKC, and CRASH_TEST variable.
4. run this script.


LDEV size requirements:
  size > (BLOCK_SIZE * NR_THREADS * (1000 / IO_INTERVAL_MS)) * 2
  size > (BLOCK_SIZE * NR_THREADS_REORDER * (1000 / IO_INTERVAL_MS)) * 2

DDEV size requirements:
  size > BLOCK_SIZE * NR_THREADS
  size > BLOCK_SIZE * NR_THREADS_REORDER

'''

NR_LOOP = 1 # number of test loops.
LDEV_SIZE = '100M' # log device size [byte].
DDEV_SIZE = '100M' # data device size [byte].
WDEV_NAME = 0  # walb device name.
NR_THREADS = 8  # number of threads when reorder flag is off.
NR_THREADS_REORDER = 128  # number of threads when reorder flag is on.

BLOCK_SIZE = '512' # can be '4K' or so.
FLUSH_INTERVAL_MS = 5  # flush interval [ms]
IO_INTERVAL_MS = 1  # write interval [ms]

CRASHTEST_TIMEOUT_S = 5  # crash-test timeout [sec]
CRASHTEST_RUNNING_S = 2  # crash-test running period [sec]

BIN = 'sudo ../../binsrc/'
WDEV = '/dev/walb/%s' % WDEV_NAME

CRASH_DEV_PREFIX = '/dev/crashblk'


def print_line():
    print ('=' * 80)


def create_crashblk_device(sizeStr):
    tmpFileName = 'device.id'
    run(BIN + 'crashblkc create %s > %s' % (sizeStr, tmpFileName))
    with open(tmpFileName, 'r') as f:
        iD = int(f.read().strip())
    return iD


def create_crashblk_devices():
    global LDEV, DDEV
    iD = create_crashblk_device(LDEV_SIZE)
    LDEV = CRASH_DEV_PREFIX + str(iD)
    iD = create_crashblk_device(DDEV_SIZE)
    DDEV = CRASH_DEV_PREFIX + str(iD)


def delete_crashblk_devices():
    for bdev in [LDEV, DDEV]:
        run(BIN + 'crashblkc delete %s' % bdev)


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
    print_line()
    print 'TEST_INIT write_test', crashDev, mode, isOverlap, lostPct, reorder

    for bdev in [LDEV, DDEV]:
        run(BIN + 'crashblkc set-reorder %s %d' % (bdev, 1 if reorder else 0))
    run(BIN + 'crashblkc set-lost-pct %s %d' % (crashDev, lostPct))
    run(BIN + 'wdevc format-ldev %s %s > /dev/null' % (LDEV, DDEV))
    run(BIN + 'wdevc create-wdev %s %s -n %d > /dev/null' % (LDEV, DDEV, WDEV_NAME))
    #run(BIN + 'wdevc set-checkpoint-interval %s %d' % (WDEV, 1000))

    if isOverlap:
        opt = '-ol'
    else:
        opt = '-nr %d' % (NR_THREADS_REORDER if reorder else NR_THREADS)
    proc = run_async(BIN + 'crash-test write %s -to %d -bs %s -ii %d -fi %d %s > %s'
                     % (opt, CRASHTEST_TIMEOUT_S, BLOCK_SIZE, IO_INTERVAL_MS, FLUSH_INTERVAL_MS, WDEV, 'write.log'))
    print 'sleep %d sec...' % CRASHTEST_RUNNING_S
    time.sleep(CRASHTEST_RUNNING_S)
    if mode == 'crash':
        run(BIN + 'crashblkc crash %s' % crashDev)
    else:
        run(BIN + 'crashblkc io-error %s %s' %
            (crashDev, ('w' if mode == 'write-error' else 'rw')))
    if not proc.wait():
        raise EnvironmentError(proc.args)

    run(BIN + 'wdevc delete-wdev -f %s' % WDEV)
    run(BIN + 'crashblkc recover %s' % crashDev)
    run(BIN + 'wdevc create-wdev %s %s > /dev/null' % (LDEV, DDEV))
    run(BIN + 'crash-test read %s -bs %s %s > read.log' % (opt, BLOCK_SIZE, WDEV))
    run(BIN + 'wdevc delete-wdev -f %s' % WDEV)
    if isOverlap:
        opt = '-ol'
    else:
        opt = ''
    proc = run_async(BIN + 'crash-test verify %s %s %s'
                     % (opt, 'write.log', 'read.log'))
    if not proc.wait():
        raise Exception('TEST_FAILURE write_test', crashDev, mode, isOverlap, lostPct, reorder)
    print 'TEST_SUCCESS write_test', crashDev, mode, isOverlap, lostPct, reorder
    print_line()


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
    print_line()
    print 'TEST_INIT read_test', crashDev, canRead

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

    run(BIN + 'wdevc delete-wdev -f %s' % WDEV)
    print 'TEST_SUCCESS read_test', crashDev, canRead
    print_line()


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
    print_line()
    print 'TEST_INIT write_read_test'
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
    run(BIN + 'wdevc delete-wdev -f %s' % WDEV)
    print 'TEST_SUCCESS write_read_test'
    print_line()


if __name__ == '__main__':

    create_crashblk_devices()

    for i in xrange(NR_LOOP):
        print 'LOOP', i
        for reorder in [False, True]:
            for lostPct in [100, 90, 50]:
                for isOverlap in [False, True]:
                    for mode in ['crash', 'write-error', 'rw-error']:
                        for dev in [LDEV, DDEV]:
                            do_write_expr(dev, mode, isOverlap, lostPct, reorder)

        do_read_expr(LDEV, True)
        do_read_expr(DDEV, False)
        do_write_read_expr()

    delete_crashblk_devices()
