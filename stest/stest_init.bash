#!/bin/bash

usage() {
    echo -e "Usage: stest_init DISK_DIR\n"
    echo "DISK_DIR: directory that you will create disks"
}

check_sh() {
    if echo "$-" | grep -q "i"; then
        echo 0
    else
        echo 1
    fi
}

check_root() {
    if [ "`whoami`" != "root" ]; then
      echo -e "ERROR: require root privilege\n"
      usage
      exit 1
    fi
}

check_argv() {
    if [ $# -eq 0 ] ;then
        echo -e "ERROR: too few arguments\n"
        usage
        exit 1
    elif [ "$1" = "-h" -o "$1" = "--help" ] ;then
        usage
        exit 1
    elif [ "${1:0:1}" = "-" ] ;then
        echo -e "ERROR: bad arguments\n"
        usage
        exit 1
    fi
}

remove_tmpfiles() {
    rm -rf .stest_init*.tmp
}

all_remove() {
    # SAVE LOOP-BACKS
    rm -rf .stest_init_loopbacks.tmp
    for vgname in "vg0" "vg1" "vg2" "test" ;do
        set +eu
        loop_back="$(pvs | sed -E "s@^ *(/dev/loop[^ ]*) *${vgname}.*@\1@g" | grep -o "^[^ ]*")"
        set -eu
        if [ "$loop_back" ] ;then
            echo $loop_back >> .stest_init_loopbacks.tmp
        fi
    done
    # LVREMOVE
    if [ -e "/dev/test" ] ;then
        echo -e "\n*** lvremove /dev/test/* -y ***"
        lvremove /dev/test/* -y
    fi
    # VGREMOVE
    for vgname in "vg0" "vg1" "vg2" "test" ;do
        set +eu
        local vg="$(vgs | sed -E "s@^ *(${vgname}).*@\1@g" | grep -E "^${vgname}$")"
        set -eu
        if [ "$vg" ] ;then
            echo -e "\n*** vgremove $vgname ***"
            vgremove $vgname
        fi
    done
    # PVREMOVE AND LOSETUP-D
    if [ -e .stest_init_loopbacks.tmp ] ;then
        cat .stest_init_loopbacks.tmp | while read line ;do
            echo -e "\n*** pvremove $line ***"
            pvremove $line
            echo -e "\n*** losetup -d $line ***"
            losetup -d $line
            cat .stest_init_losetup.tmp | grep -v "$line" > .stest_init_losetup.tmp.tmp
            mv .stest_init_losetup.tmp.tmp .stest_init_losetup.tmp
        done
    fi
}

get_lonr_max() {
    local lonr_max=0
    while read var ;do
        if [ $var -gt $lonr_max ] ;then
        lonr_max=$var
        fi
    done
    echo $lonr_max
}

create_4lvm() {
    vg_nr=0
    for nr in $(seq "$1" "$((${1}+3))") ;do
        if [ ! -s disk$nr ] ;then
            echo -e "\n*** dd if=/dev/zero of=disk$nr bs=1M count=300 ***"
            dd if=/dev/zero of=disk$nr bs=1M count=300
        else
            echo -e "\n*** ERROR: file \"disk$nr\" is allready exists ***"
            exit 1
        fi
        if [ ! "$(losetup | grep -o "^/dev/loop$nr")" ] ;then
            echo -e "\n*** losetup /dev/loop$nr disk$nr ***"
            losetup /dev/loop$nr disk$nr
        else
            echo -e "\n*** ERROR: loopback device \"/dev/loop$nr\" is allready exists ***"
            exit 1
        fi
        if [ ! "$(pvs | grep -o "^ */dev/loop$nr")" ] ;then
            echo -e "\n*** pvcreate /dev/loop$nr ***"
            pvcreate /dev/loop$nr
        else
            echo -e "\n*** ERROR: physical volume \"/dev/loop$nr\" is allready exists ***"
            exit 1
        fi
        if [ $nr -eq $1 ] ;then
            if [ ! "$(vgs | grep -o "^ *vg0")" ] ;then
                echo -e "\n*** vgcreate test /dev/loop$nr ***"
                vgcreate test /dev/loop$nr
            else
                echo -e "\n*** ERROR: volume group \"test\" is allready exists ***"
                exit 1
            fi
        else
            if [ ! "$(vgs | grep -o "^ *vg${vg_nr}")" ] ;then
                echo -e "\n*** vgcreate vg$vg_nr /dev/loop$nr ***"
                vgcreate vg$vg_nr /dev/loop$nr
                vg_nr=$((${vg_nr}+1))
            else
                echo -e "\n*** ERROR: volume group \"vg${vg_nr}\" is allready exists ***"
                exit 1
            fi
        fi
    done
}

do_lvcreate() {
    echo -e "\n*** lvcreate -L 12M -n data test ***"
    lvcreate -L 12M -n data test
    echo -e "\n*** lvcreate -L 12M -n data2 test ***"
    lvcreate -L 12M -n data2 test
    echo -e "\n*** lvcreate -L 12M -n data3 test ***"
    lvcreate -L 12M -n data3 test
    echo -e "\n*** lvcreate -L 12M -n log test ***"
    lvcreate -L 12M -n log test
    echo -e "\n*** lvcreate -L 12M -n log2 test ***"
    lvcreate -L 12M -n log2 test
    echo -e "\n*** lvcreate -L 12M -n log3 test ***"
    lvcreate -L 12M -n log3 test
}

main() {
    if [ -d $1 ] ;then
        cd $1
    else
        echo "ERROR: \"$1/\" is not found"
        exit 0
    fi
    echo -e "\n*** STARTING ***"
    losetup > .stest_init_losetup.tmp
    sed -i -e '1,1d' .stest_init_losetup.tmp
    cat .stest_init_losetup.tmp | sed -E "s@.*/dev/loop([^ ]*).*@\1@g" > .stest_init_lonr.tmp
    if [ -s .stest_init_lonr.tmp ] ;then
        lonr_max="$(cat .stest_init_lonr.tmp | get_lonr_max)"
        lonr="$(($lonr_max+1))"
    else
        lonr=0
    fi
    create_4lvm $lonr
    do_lvcreate
    remove_tmpfiles
}

is_sh="$(check_sh "$@")"
if [ $is_sh -eq 0 ] ;then
    echo "ERROR: You can not use \`source\` to this."
    echo "NOTE: You can run the following command: \`./stest_init\`"
else
    check_root
    check_argv $@
    set -eu
    main $@
fi
