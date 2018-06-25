#!/bin/bash

usage() {
    echo "Usage: remove_lvm.bash"
    exit 0
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

all_remove() {
    # SAVE LOOP-BACKS
    rm -rf .remove_lvm_loopbacks.tmp
    for vgname in "vg0" "vg1" "vg2" "test" ;do
        set +eu
        loop_back="$(pvs | sed -E "s@^ *(/dev/loop[^ ]*) *${vgname}.*@\1@g" | grep -o "^[^ ]*")"
        set -eu
        if [ "$loop_back" ] ;then
            echo $loop_back >> .remove_lvm_loopbacks.tmp
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
    if [ -e .remove_lvm_loopbacks.tmp ] ;then
        cat .remove_lvm_loopbacks.tmp | while read line ;do
        echo -e "\n*** pvremove $line ***"
        pvremove $line
        echo -e "\n*** losetup -d $line ***"
        losetup -d $line
    done
fi
}

is_sh="$(check_sh "$@")"
if [ $is_sh -eq 0 ] ;then
    echo "ERROR: You can not use \`source\` to this."
    echo "NOTE: You can run the following command: \`./stest_init\`"
else
    check_root
    set -eu
    all_remove
    rm -rf .remove_lvm*.tmp
fi
