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

## Scenario tests (stest)

See `stest/scenario0.py`.

Prepare to run scenario test.
- load walb module.
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
