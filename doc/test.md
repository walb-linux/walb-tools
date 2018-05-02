# Test

There are three kind of tests: **utest**, **itest** and **stest**.

## Unit tests (utest)

```
> make utest DEBUG=1
> make utest_all
```

## Integration tests (itest)

### wlog test

This test will use `/dev/loop0` and `/dev/loop1`.
The user must have disk group permission.
Build walb-tools before.

```
> cd itest/wlog
> make
```

### wdiff test

Build walb-tools before.

```
> cd itest/wdiff
> make
```

### crash test

This test will check error handling code and crash recoery code are valid.

You need **crashblk** to run this test, which is available 
at [crashblk repository](https://github.com/starpos/crashblk/).

Before running the test, load `crashblk-mod.ko` and `walb-mod.ko` modules.

Then create two crashblk devices. The size 100MB is enough.

```
> sudo crashblkc create 100M
0
> sudo crashblkc create 100M
1
> cat /proc/partitions |grep crashblk
251        0     102400 crashblk0
251        1     102400 crashblk1
```

Before you run the test, check variables `crash-test.py` script.

Finally, run the test:
```
> cd itest/crash
> python2 crash-test.py
```

## Scenario tests (stest)

See `stest/scenario0.py`.

Prepare to run scenario test.
- load walb kernel driver.
- build walb-tools.
- prepare lvm volume groups `vg0`, `vg1`, `vg2`.
- prepare lvm volumes
  `/dev/test/data`,
  `/dev/test/data2`,
  `/dev/test/data3`,
  `/dev/test/log`,
  `/dev/test/log2`,
  `/dev/test/log3`.
  Device sizes are all 12MiB.

A sample command:
```
cd walb
git co -b 3.10 origin/for-3.10 # for ubuntu 14.0
cd module
make
sudo insmod walb-mod.ko
sudo apt-get install pylint # for pylint
# make lvm volumes
dd if=/dev/zero of=disk0 bs=1m count=300
dd if=/dev/zero of=disk1 bs=1m count=300
dd if=/dev/zero of=disk2 bs=1m count=300
dd if=/dev/zero of=disk3 bs=1m count=300

sudo losetup /dev/loop0 disk0
sudo losetup /dev/loop1 disk1
sudo losetup /dev/loop2 disk2
sudo losetup /dev/loop3 disk3

sudo pvcreate test /dev/loop0
sudo pvcreate vg0 /dev/loop1
sudo pvcreate vg1 /dev/loop2
sudo pvcreate vg2 /dev/loop3

sudo vgcreate test /dev/loop0
sudo vgcreate vg0 /dev/loop1
sudo vgcreate vg1 /dev/loop2
sudo vgcreate vg2 /dev/loop3

sudo lvcreate -L 12M -n data test
sudo lvcreate -L 12M -n data2 test
sudo lvcreate -L 12M -n data3 test
sudo lvcreate -L 12M -n log test
sudo lvcreate -L 12M -n log2 test
sudo lvcreate -L 12M -n log3 test
```

This test will use several ports in 10000-30300.
See parameters defined in `stest/scenario0.py`.

Run all tests just one time.
```
> sudo python stest/scenario0.py
```

Run all tests 10 times.
```
> sudo python stest/scenario0.py 10
```

Run the tests n1, e1, e2, e3 only 10 times.
```
> sudo python stest/scenario0.py 10 n1 e1 e2 e3
```

-----
