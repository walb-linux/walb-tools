#!/bin/sh

BIN=../../binsrc

make_zero_image()
{
  local i
  for i in $@; do
    dd if=/dev/zero of=./ddev32M.${i} bs=1048576 count=32
  done
}

check_result()
{
  local retValue=$1
  shift 1
  local msg="$@"
  if [ $retValue -ne 0 ]; then
    echo "TSET_FAILURE: ${msg}"
    exit 1
  else
    echo "TEST_SUCCESS: ${msg}"
  fi
}

prepare_test()
{
  # Generate wlog/wdiff files for test.
  echo "#################### Generate wlog/wdiff files for test ####################"
  dd if=/dev/urandom of=./ddev32M bs=1048576 count=32
  local i
  for i in 1 2 3 4; do
    #$BIN/wlog-gen --nodiscard -s 32M -o ${i}.wlog
    $BIN/wlog-gen -s 32M -z 32M -o ${i}.wlog
    $BIN/wlog-to-wdiff > ${i}.wdiff < ${i}.wlog
  done
}

log_diff_equality_test()
{
  echo "#################### Log/diff equality test ####################"
  local i
  for i in 1 2 3 4; do
    make_zero_image 0 1
    $BIN/wlog-redo -z ddev32M.0 < ${i}.wlog
    $BIN/wdiff-redo -z ddev32M.1 < ${i}.wdiff
    $BIN/bdiff -b 512 ddev32M.0 ddev32M.1
    check_result $? "log/diff equality test ${i}th wlog/wdiff."
  done
}

full_image_test()
{
  # Full image test.
  echo "#################### Full image test ####################"
  $BIN/wdiff-full < ddev32M > 0.wdiff
  make_zero_image 0
  $BIN/wdiff-redo -z ddev32M.0 < 0.wdiff
  $BIN/bdiff -b 512 ddev32M ddev32M.0
  check_result $? "full image test"
}

consolidation_test1()
{
  echo "##################### Consolidation test ####################"
  make_zero_image 0 1 2
  local i
  for i in 1 2 3 4; do
    $BIN/wdiff-redo -z ddev32M.0 < ${i}.wdiff
    $BIN/wlog-redo -z ddev32M.1 < ${i}.wlog
  done
  $BIN/wdiff-merge -x 16K -i 1.wdiff 2.wdiff 3.wdiff 4.wdiff -o all.wdiff
  $BIN/wdiff-redo -z ddev32M.2 < all.wdiff
  sha1sum ddev32M.0 ddev32M.1 ddev32M.2
  $BIN/bdiff -b 512 ddev32M.0 ddev32M.1
  check_result $? "consolidation test 1a."
  $BIN/bdiff -b 512 ddev32M.0 ddev32M.2
  check_result $? "consolidation test 1b."
}

consolidation_test2()
{
  echo "##################### Consolidation test ####################"
  cp ddev32M ddev32M.0
  local i
  for i in 1 2 3 4; do
    $BIN/wdiff-redo -z ddev32M.0 < ${i}.wdiff
  done
  $BIN/virt-full-cat -i ddev32M -o ddev32M.1 -d 1.wdiff 2.wdiff 3.wdiff 4.wdiff
  $BIN/bdiff -b 512 ddev32M.0 ddev32M.1
  check_result $? "consolidation test 3."
}  

max_io_blocks_test()
{
  echo "#################### MaxIoBlocks test #################### "
  make_zero_image 0 1 2
  local i
  for i in 1 2 3 4; do
    $BIN/wlog-to-wdiff -x  4K < ${i}.wlog > ${i}-4K.wdiff
    $BIN/wlog-to-wdiff -x 16K < ${i}.wlog > ${i}-16K.wdiff
    $BIN/wdiff-redo -z ddev32M.0 < ${i}.wdiff
    $BIN/wdiff-redo -z ddev32M.1 < ${i}-4K.wdiff
    $BIN/wdiff-redo -z ddev32M.2 < ${i}-16K.wdiff
    $BIN/bdiff -b 512 ddev32M.0 ddev32M.1
    check_result $? "maxIoBlocks test ${i}th wdiff 4K"
    $BIN/bdiff -b 512 ddev32M.0 ddev32M.2
    check_result $? "maxIoBlocks test ${i}th wdiff 16K"
  done
}


prepare_test
log_diff_equality_test
full_image_test
consolidation_test1
consolidation_test2
max_io_blocks_test

echo "TEST_SUCCESS"
exit 0
