../../binsrc/walb-storage -archive localhost:10200 -proxy localhost:10100 -p 10000 -b /mnt/tutorial/data/s0/ -l /mnt/tutorial/data/s0.log -id s0 &
../../binsrc/walb-proxy -p 10100 -b /mnt/tutorial/data/p0/ -l /mnt/tutorial/data/p0.log -id p0 &
../../binsrc/walb-archive -vg tutorial -p 10200 -b /mnt/tutorial/data/a0/ -l /mnt/tutorial/data/a0.log -id a0 &
