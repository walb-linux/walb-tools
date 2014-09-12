#!/bin/sh

#
# crash test of walb module.
#
# 1. load crashblk and walb module.
# 2. prepare two crashblk devices LDEV and DDEV.
# 3. prepare crashblkc, wdevc, and crash-test executable binaries
#    and set WDEVC, CRASHBLKC, and CRASH_TEST variable.
# 4. run this script.
#

WDEVC=./wdevc
CRASHBLKC=./crashblkc
CRASH_TEST=./crash-test

LDEV=/dev/crashblk0
DDEV=/dev/crashblk1
WDEV_NAME=0
NR_THREADS=8

#set -x

WDEV=/dev/walb/$WDEV_NAME

do_write_expr()
{
  local CRASH_DEV=$1
  local mode=$2

  sudo $WDEVC format-ldev $LDEV $DDEV > /dev/null
  sudo $WDEVC create-wdev $LDEV $DDEV -n $WDEV_NAME > /dev/null

  (sudo $CRASH_TEST write -th $NR_THREADS -to 10 $WDEV > write.log) &
  sleep 3
  if [ "$mode" = "crash" ]; then
     sudo $CRASHBLKC crash $CRASH_DEV
  elif [ "$mode" = "write-error" ]; then
    sudo $CRASHBLKC io-error $CRASH_DEV w
  else
    sudo $CRASHBLKC io-error $CRASH_DEV rw
  fi
  wait

  sudo $WDEVC delete-wdev $WDEV
  sudo $CRASHBLKC recover $CRASH_DEV

  sudo $WDEVC create-wdev $LDEV $DDEV > /dev/null

  sudo $CRASH_TEST read -th $NR_THREADS $WDEV > read.log

  sudo $WDEVC delete-wdev $WDEV

  sudo $CRASH_TEST verify write.log read.log
  if [ $? -ne 0 ]; then
    echo TEST_FAILURE write_test $CRASH_DEV $mode
    exit 1
  fi
  echo TEST_SUCCESS write_test $CRASH_DEV $mode
}

do_read_expr()
{
  local CRASH_DEV=$1
  local CAN_READ=$2 # 0 or 1
  local err

  sudo $WDEVC format-ldev $LDEV $DDEV > /dev/null
  sudo $WDEVC create-wdev $LDEV $DDEV -n $WDEV_NAME > /dev/null

  sudo dd if=$WDEV of=/dev/null iflag=direct bs=4096 count=1
  if [ $? -ne 0 ]; then
    echo TEST_FAILURE read_test read must success
    exit 1
  fi

  sudo $CRASHBLKC io-error $CRASH_DEV r

  sudo dd if=$WDEV of=/dev/null iflag=direct bs=4096 count=1
  err=$?
  echo err $err
  if [ $err -eq $CAN_READ ]; then
    echo TEST_FAILURE read_test read must fail
    exit 1
  fi

  sudo $CRASHBLKC recover $CRASH_DEV

  sudo dd if=$WDEV of=/dev/null iflag=direct bs=4096 count=1
  if [ $? -ne 0 ]; then
    echo TEST_FAILURE read_test read must success
    exit 1
  fi

  sudo $WDEVC delete-wdev $WDEV
  echo TEST_SUCCESS read_test $CRASH_DEV $CAN_READ}
}


### main

for mode in crash write-error rw-error; do
  echo write_test $LDEV $mode
  do_write_expr $LDEV $mode
  echo write_test $DDEV $mode
  do_write_expr $DDEV $mode
done

do_read_expr $LDEV 1
do_read_expr $DDEV 0
