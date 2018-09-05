import sys
sys.path.append("../")
from run import *

BIN="../../binsrc"

def make_zero_image(*args):
    for i in args:
        run("dd oflag=direct if=/dev/zero of=./ddev32M.{} bs=1048576 count=32".format(i))

def prepare_test():
    # Generate wlog/wdiff files for test.
    print "#################### Generate wlog/wdiff files for test ####################"
    run("dd oflag=direct if=/dev/urandom of=./ddev32M bs=1048576 count=32")
    for i in xrange(1, 5):
        #BIN/wlog-gen --nodiscard -s 32M -o ${i}.wlog
        run(BIN + ("/wlog-gen -s 32M -z 32M --minDiscardSize 512 --maxDiscardSize 1M -o {}.wlog".format(i)))
        run(BIN + ("/wlog-to-wdiff -o {}.s.wdiff -i {}.wlog".format(i, i)))
        run(BIN + ("/wlog-to-wdiff -indexed -o {}.i.wdiff -i {}.wlog".format(i, i)))

def log_diff_equality_test():
    print "#################### Log/diff equality test ####################"
    for i in xrange(1, 5):
        make_zero_image(0, 1, 2)
        run(BIN + ("/wlog-redo -z ddev32M.0 < {}.wlog".format(i)))
        run(BIN + ("/wdiff-redo -z ddev32M.1 -i {}.s.wdiff".format(i)))
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.1")
        check_result("log/diff equality test {}th wlog/s.wdiff.".format(i))
        run(BIN + ("/wdiff-redo -z ddev32M.2 -i {}.i.wdiff".format(i)))
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.2")
        check_result("log/diff equality test {}th wlog/i.wdiff.".format(i))

def full_image_test():
    print "#################### Full image test ####################"
    run(BIN + "/wdiff-full -i ddev32M -o 0.s.wdiff")
    run(BIN + "/wdiff-full -indexed -i ddev32M -o 0.i.wdiff")
    make_zero_image(0, 1)
    run(BIN + "/wdiff-redo -z ddev32M.0 -i 0.s.wdiff")
    run(BIN + "/bdiff -b 512 ddev32M ddev32M.0")
    check_result("full image test (s.wdiff)")
    run(BIN + "/wdiff-redo -z ddev32M.1 -i 0.i.wdiff")
    run(BIN + "/bdiff -b 512 ddev32M ddev32M.1")
    check_result("full image test (i.wdiff)")

def consolidation_test1():
    print "##################### Consolidation test ####################"
    make_zero_image(0, 1, 2, 3, 4)
    for i in xrange(1, 5):
        run(BIN + ("/wlog-redo -z ddev32M.0 < {}.wlog".format(i)))
        run(BIN + ("/wdiff-redo -z ddev32M.1 -i {}.s.wdiff".format(i)))
        run(BIN + ("/wdiff-redo -z ddev32M.2 -i {}.i.wdiff".format(i)))

    run(BIN + "/wdiff-merge -stat -x 16K -i {} -o all.s.wdiff".format(' '.join('{}.s.wdiff'.format(i) for i in xrange(1, 5))))
    run(BIN + "/wdiff-redo -z ddev32M.3 -i all.s.wdiff")
    run(BIN + "/wdiff-merge -stat -x 16K -i {} -o all.i.wdiff".format(' '.join('{}.i.wdiff'.format(i) for i in xrange(1, 5))))
    run(BIN + "/wdiff-redo -z ddev32M.4 -i all.i.wdiff")
    run("sha1sum {}".format(' '.join('ddev32M.{}'.format(i) for i in xrange(0, 5))))
    for i in xrange(1, 5):
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.{}".format(i))
        check_result("consolidation test 1-{}.".format(i))

def consolidation_test2():
    print "##################### Consolidation test ####################"
    run("cp ddev32M ddev32M.0")
    for i in xrange(1, 5):
        run(BIN + ("/wdiff-redo -z ddev32M.0 -i {}.s.wdiff".format(i)))

    run(BIN + "/virt-full-cat -stat -i ddev32M -o ddev32M.1 -d {}".format(' '.join('{}.s.wdiff'.format(i) for i in xrange(1, 5))))
    run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.1")
    check_result("consolidation test 2s.")
    run(BIN + "/virt-full-cat -stat -i ddev32M -o ddev32M.2 -d {}".format(' '.join('{}.i.wdiff'.format(i) for i in xrange(1, 5))))
    run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.2")
    check_result("consolidation test 2i.")

def max_io_blocks_test():
    print "#################### MaxIoBlocks test #################### "
    make_zero_image(0, 1, 2)
    for i in xrange(1, 5):
        run(BIN + ("/wlog-to-wdiff -x  4K < {}.wlog > {}-4K.wdiff".format(i, i)))
        run(BIN + ("/wlog-to-wdiff -x 16K < {}.wlog > {}-16K.wdiff".format(i, i)))
        run(BIN + ("/wdiff-redo -z ddev32M.0 < {}.s.wdiff".format(i)))
        run(BIN + ("/wdiff-redo -z ddev32M.1 < {}-4K.wdiff".format(i)))
        run(BIN + ("/wdiff-redo -z ddev32M.2 < {}-16K.wdiff".format(i)))
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.1")
        check_result("maxIoBlocks test {}th wdiff 4K".format(i))
        run(BIN + "/bdiff -b 512 ddev32M.0 ddev32M.2")
        check_result("maxIoBlocks test {}th wdiff 16K".format(i))

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
