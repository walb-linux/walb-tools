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

do_expr()
{
  local CRASH_DEV=$1
  local mode=$2

  sudo $WDEVC format-ldev $LDEV $DDEV > /dev/null
  sudo $WDEVC create-wdev $LDEV $DDEV -n $WDEV_NAME > /dev/null

  (sudo $CRASH_TEST write -th $NR_THREADS -to 10 $WDEV > write.log) &
  sleep 3
  if [ "$mode" = "crash" ]; then
     sudo $CRASHBLKC make-crash $CRASH_DEV
  elif [ "$mode" = "write-error" ]; then
    sudo $CRASHBLKC io-error $CRASH_DEV w
  else
    sudo $CRASHBLKC io-error $CRASH_DEV rw
  fi
  wait

  sudo $WDEVC delete-wdev $WDEV
  sleep 1
  sudo $CRASHBLKC recover $CRASH_DEV

  sudo $WDEVC create-wdev $LDEV $DDEV > /dev/null

  sudo $CRASH_TEST read -th $NR_THREADS $WDEV > read.log

  sleep 1
  sudo $WDEVC delete-wdev $WDEV

  sudo $CRASH_TEST verify write.log read.log
  if [ $? -ne 0 ]; then
    echo test $CRASH_DEV $mode failed.
    exit 1
  fi
}

for mode in crash write-error rw-error; do
  echo test $LDEV $mode
  do_expr $LDEV $mode
  echo test $DDEV $mode
  do_expr $DDEV $mode
done
