#!/bin/sh

for i in `seq 128`; do
  echo $i
  sudo ../local/bin/iores -b 4K -t 4 -c 5000 -w /dev/walb/1
  sleep 20
done
