import sys
sys.path.append("../")
from run import *

BIN="../../binsrc"

def make_zero_image(*args):
    for i in args:
        run("dd if=/dev/zero of=./ddev32M.%d bs=1048576 count=32" % i)

def prepare_test():
    # Generate wlog/wdiff files for test.
    print "#################### Generate wlog/wdiff files for test ####################"
    run("dd if=/dev/urandom of=./ddev32M bs=1048576 count=32")
    for i in xrange(1, 5):
        #BIN/wlog-gen --nodiscard -s 32M -o ${i}.wlog
        run(BIN + ("/wlog-gen -s 32M -z 32M --minDiscardSize 512 --maxDiscardSize 1M -o %d.wlog" % i))
        run(BIN + ("/wlog-to-wdiff > %d.wdiff < %d.wlog" % (i, i)))

def log_diff_equality_test():
    print "#################### Log/diff equality test ####################"
    for i in xrange(1, 5):
        make_zero_image(0, 1)
        run(BIN + ("/wlog-redo -z ddev32M.0 < %d.wlog" % i))
        run(BIN + ("/wdiff-redo -z ddev32M.1 < %d.wdiff" % i))
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.1")
        check_result("log/diff equality test %dth wlog/wdiff." % i)

def full_image_test():
    print "#################### Full image test ####################"
    run(BIN + "/wdiff-full < ddev32M > 0.wdiff")
    make_zero_image(0)
    run(BIN + "/wdiff-redo -z ddev32M.0 < 0.wdiff")
    run(BIN + "/bdiff -b 512 ddev32M ddev32M.0")
    check_result("full image test")

def consolidation_test1():
    print "##################### Consolidation test ####################"
    make_zero_image(0, 1, 2)
    for i in xrange(1, 5):
        run(BIN + ("/wdiff-redo -z ddev32M.0 < %d.wdiff" % i))
        run(BIN + ("/wlog-redo -z ddev32M.1 < %d.wlog" % i))

    run(BIN + "/wdiff-merge -stat -x 16K -i 1.wdiff 2.wdiff 3.wdiff 4.wdiff -o all.wdiff")
    run(BIN + "/wdiff-redo -z ddev32M.2 < all.wdiff")
    run("sha1sum ddev32M.0 ddev32M.1 ddev32M.2")
    run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.1")
    check_result("consolidation test 1a.")
    run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.2")
    check_result("consolidation test 1b.")

def consolidation_test2():
    print "##################### Consolidation test ####################"
    run("cp ddev32M ddev32M.0")
    for i in xrange(1, 5):
        run(BIN + ("/wdiff-redo -z ddev32M.0 < %d.wdiff" % i))

    run(BIN + "/virt-full-cat -stat -i ddev32M -o ddev32M.1 -d 1.wdiff 2.wdiff 3.wdiff 4.wdiff")
    run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.1")
    check_result("consolidation test 3.")

def max_io_blocks_test():
    print "#################### MaxIoBlocks test #################### "
    make_zero_image(0, 1, 2)
    for i in xrange(1, 5):
        run(BIN + ("/wlog-to-wdiff -x  4K < %d.wlog > %d-4K.wdiff" % (i, i)))
        run(BIN + ("/wlog-to-wdiff -x 16K < %d.wlog > %d-16K.wdiff" % (i, i)))
        run(BIN + ("/wdiff-redo -z ddev32M.0 < %d.wdiff" % i))
        run(BIN + ("/wdiff-redo -z ddev32M.1 < %d-4K.wdiff" % i))
        run(BIN + ("/wdiff-redo -z ddev32M.2 < %d-16K.wdiff" % i))
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.1")
        check_result("maxIoBlocks test %dth wdiff 4K" % i)
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.2")
        check_result("maxIoBlocks test %dth wdiff 16K" % i)

def main():
    prepare_test()
    log_diff_equality_test()
    full_image_test()
    consolidation_test1()
    consolidation_test2()
    max_io_blocks_test()
    print "TEST_SUCCESS"

if __name__ == '__main__':
    main()

