# Manage volume replicas using walb devices

You may want to manage volume replicas to provide a virtual volume with redundancy
using software RAID1 or so.
Walt-tools work well in such situations.

Walb-tools does not provide virtual volume management functionality,
but provides backup target switching functionality.


Assume the following server layout:
```
  +-------+       +-------+
  |  s0   |       |  s1   |
  +-------+       +-------+
    |  |             |  |
    |  |             |  |
    |  +----+   +----+  |
    |       |   |       |
    |       v   v       |
    |     +-------+     |
    |     |  p0   |     |
    |     +-------+     |
    |         |         |
    |         v         |
    |     +-------+     |
    +---->|  a0   |<----+
          +-------+
```

In python:
```python
sLayout = ServerLayout([s0, s1], [p0], [a0])

```

Assume the following walb devices:
```python
wdev0 = Device(iD0, '/dev/ldev0', '/dev/ddev0', wdevcPath, walbc.get_run_remote_command(s0))
wdev1 = Device(iD1, '/dev/ldev1', '/dev/ddev1', wdevcPath, walbc.get_run_remote_command(s1))
```
`wdev0` and `wdev1` exist in the hosts where `s0` and `s1` running respectively.
Assume both `wdev0` and `wdev1` are the replicas of a virtual volume.
Their block device images must be almost the same (except for header blocks or so).

First of all, initialize volumes as follows:
```
python> walbc.init_storage(s0, 'vol0', wdev0.path)
python> walbc.init_storage(s1, 'vol0', wdev1.path)
```
All the volume names of walb-tools volumes must be the same for a virtual volume.

Next, choose primary backup target and run full backup. Here you shall choose `s0`:
```
python> walbc.full_backup(s0, 'vol0')
```

Confirm the volume state at each storage server:
```
python> walbc.get_state(s0, 'vol0')
'Target'
python> walbc.get_state(s1, 'vol0')
'Standby'
```

Now updated data of `wdev0` will be transferred to `a0` continuously,
while that of `wdev1` will be discarded.


When you want to change the backup target from `s0` to `s1`,
You can make the replica of `s0` standby and the one of `s1` target as follows:
```
python> walbc.go_standby(s0, 'vol0')
python> walbc.hash_backup(s1, 'vol0')
```
Use `walbc.clear_vol()` instead of `walbc.go_standby()`
when the corresponding walb device is no more available due to its failure or so.

Confirm the volume state at each storage server:
```
python> walbc.get_state(s0, 'vol0')
'Standby'
python> walbc.get_state(s1, 'vol0')
'Target'
```
Now the backup target for volume 'vol0' is `s1`.

You can use also full backup instead of hash backup,
while full backup is less efficient than hash backup.
If you use full backup, stop and reset the volume before calling full backup:
```
python> walbc.stop(a0, 'vol0')
python> walbc.reset_vol(a0, 'vol0')
python> walb.full_backup(s1, 'vol0')
```

-----
