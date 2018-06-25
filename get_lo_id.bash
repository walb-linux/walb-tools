#!/bin/bash

set -eu

get_lonr_max() {
    local lonr_max=0
    while read var ;do
        if [ $var -gt $lonr_max ] ;then
        lonr_max=$var
        fi
    done
    echo $lonr_max
}

losetup > .get_lo_id.tmp
sed -i -e '1,1d' .get_lo_id.tmp
cat .get_lo_id.tmp | sed -E "s@.*/dev/loop([^ ]*).*@\1@g" > .get_lo_id_id.tmp
if [ -s .get_lo_id_id.tmp ] ;then
    lonr_max="$(cat .get_lo_id_id.tmp | get_lonr_max)"
    lonr="$(($lonr_max+1))"
    echo $lonr
else
    echo 0
fi
rm -rf .get_lo_id*.tmp
