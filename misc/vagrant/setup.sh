#!/bin/bash

UNAME_R=`uname -r`
# ex. "4.4.0-78-generic" -> 404
# ex. "3.10-0-514.16.1.el7.x86_64" -> 310
OS_VER=`python -c "import sys; a = map(int, sys.argv[1].split('.')[:2]); print a[0] * 100 + a[1]" ${UNAME_R}`
if [ ${OS_VER} -le 313 ]; then
	BRANCH=for-3.10
elif [ ${OS_VER} -le 402 ]; then
	BRANCH=for-3.14
elif [ ${OS_VER} -le 407 ]; then
	BRANCH=for-4.3
else
	BRANCH=master
fi

BINSRC=~/walb-tools/binsrc
DATA=/mnt/tutorial

# setup lvm
cd
if [ ! -f tutorial-disk ]; then
	echo make tutorial-disk loopback device and setup lvm
	dd if=/dev/zero of=tutorial-disk bs=1M count=100
	sudo losetup /dev/loop0 tutorial-disk
	sudo pvcreate /dev/loop0
	sudo vgcreate tutorial /dev/loop0
	sudo lvcreate -n wdata -L 10m tutorial
	sudo lvcreate -n wlog -L 10m tutorial
	sudo lvcreate -n data -L 20m tutorial
	sudo mkfs.ext4 /dev/tutorial/data
	sudo mkdir -p ${DATA}
	sudo mount /dev/tutorial/data ${DATA}
	sudo mkdir -p ${DATA}/{a0,p0,s0}
fi

# losetup
cd
GREP_LOSETUP=`losetup |grep tutorial-disk`
if [ "${GREP_LOSETUP}" == "" ]; then
	echo losetup
	sudo losetup /dev/loop0 tutorial-disk
	sleep 1
	sudo mount /dev/tutorial/data ${DATA}
fi

# make walb tools core
cd
if [ ! -d walb-tools ]; then
	echo make walb-tools
	git clone --depth 1 https://github.com/walb-linux/walb-tools.git
	cd walb-tools
	make -j4 CC=gcc CXX=g++ core
fi

# make walb driver
cd
if [ ! -d walb-driver ]; then
	echo make walb-driver
	git clone https://github.com/walb-linux/walb-driver.git
	cd walb-driver
	echo BRANCH=${BRANCH}
	if [ ${BRANCH} != "master" ]; then
		git checkout -b ${BRANCH} origin/${BRANCH}
	fi
	cd module
	make
	if [ $? -ne 0 ]; then
		echo "fail to make walb driver"
		exit 1
	fi
fi

# insmod walb
if [ ! -c /dev/walb/control ]; then
	echo insmod walb-mod
	sudo insmod ~/walb-driver/module/walb-mod.ko
fi

# format walb log device(ldev) only *once*
if [ ! -f ~/initialized_walb_device ]; then
	echo format-ldev
	sudo ${BINSRC}/wdevc format-ldev /dev/tutorial/wlog /dev/tutorial/wdata
	if [ $? -eq 0 ]; then
		touch ~/initialized_walb_device
	fi
fi

# create walb device(wdev)

WDEV_NAME=walb-tutorial-device
if [ ! -b /dev/walb/${WDEV_NAME} ]; then
	echo create-wdev
	sleep 1 #wait completion of format-ldev
	sudo ${BINSRC}/wdevc create-wdev /dev/tutorial/wlog /dev/tutorial/wdata -n ${WDEV_NAME}
fi

# start walb server
echo start walb server
sudo pkill walb-
sudo ${BINSRC}/walb-storage -b ${DATA}/s0/ -l ${DATA}/s0.log -archive localhost:10200 -p 10000 -bg 1 -proxy localhost:10100 -fg 2 -id s0 &
sudo ${BINSRC}/walb-proxy -b ${DATA}/p0/ -l ${DATA}/p0.log -p 10100 -bg 1 -fg 2 -id p0 &
sudo ${BINSRC}/walb-archive -b ${DATA}/a0/ -vg tutorial -l ${DATA}/a0.log -p 10200 -fg 2 -id a0 &

# for tutorial.2py
if [ -f tutorial2-disk ]; then
	sudo losetup /dev/loop1 tutorial2-disk
	mkdir ${DATA}/a1
	sudo ${BINSRC}/walb-archive -b ${DATA}/a1/ -vg tutorial2 -l ${DATA}/a1.log -p 10201 -fg 2 -id a1 &
fi
