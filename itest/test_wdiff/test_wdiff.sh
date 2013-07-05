#!/bin/sh

BIN=../../src

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

# Generate wlog/wdiff files for test.
echo "#################### Generate wlog/wdiff files for test ####################"
dd if=/dev/urandom of=./ddev32M bs=1048576 count=32
for i in 1 2 3 4; do
  #$BIN/wlog-gen --nodiscard -s 32M -o ${i}.wlog
  $BIN/wlog-gen -s 32M -z 32M -o ${i}.wlog
  $BIN/wlog-to-wdiff > ${i}.wdiff < ${i}.wlog
done

echo "#################### Log/diff equality test ####################"
for i in 1 2 3 4; do
  make_zero_image 0 1
  $BIN/wlog-redo --zerodiscard ddev32M.0 < ${i}.wlog
  $BIN/wdiff-redo --zerodiscard ddev32M.1 < ${i}.wdiff
  $BIN/bdiff -b 512 ddev32M.0 ddev32M.1
  check_result $? "log/diff equality test ${i}th wlog/wdiff."
done

# Full image test.
echo "#################### Full image test ####################"
$BIN/wdiff-full < ddev32M > 0.wdiff
make_zero_image 0
$BIN/wdiff-redo --zerodiscard ddev32M.0 < 0.wdiff
$BIN/bdiff -b 512 ddev32M ddev32M.0
check_result $? "full image test"

echo "##################### Consolidation test ####################"
make_zero_image 0 1 2
for i in 1 2 3 4; do
  $BIN/wdiff-redo --zerodiscard ddev32M.0 < ${i}.wdiff
  $BIN/wlog-redo --zerodiscard ddev32M.1 < ${i}.wlog
done
$BIN/wdiff-merge 1.wdiff 2.wdiff 3.wdiff 4.wdiff all.wdiff
$BIN/wdiff-redo --zerodiscard ddev32M.2 < all.wdiff
sha1sum ddev32M.0 ddev32M.1 ddev32M.2
$BIN/bdiff -b 512 ddev32M.0 ddev32M.1
check_result $? "consolidation test 1."
$BIN/bdiff -b 512 ddev32M.0 ddev32M.2
check_result $? "consolidation test 2."

echo "TEST_SUCCESS"
exit 0

