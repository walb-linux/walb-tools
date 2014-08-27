# Recover from errors or failures

Walb-tools supports various recovery functionalities from errors and failures.

Here we assume two servers for storage/proxy/archive respectively.
```python
sLayout = ServerLayout([s0, s1], [p0, p1], [a0, a1])
```


## Storage server/host down

When a storage host shuts down, just restart the host and new storage server process.
When a storage server process is killed suddenly,
first of all, restart new storage server process.

If a storage server process is down for a long time,
wlogs of its managing walb devices will be accumulated and finally their log devices will overflow.

After a walb device overflow, wlog/wdiff transferring of the corresponding volume
will stop soon and continuous backup of the volume is no more available
without executing hash backup.

You can check overflow of volume `vol` at storage server `sx` as follows:
```
python> isOverflow = walbc.is_overflow(sx, vol)
```
It returns a boolean value.
If the volume overflows, run hash backup as follows:
```
python> walbc.hash_backup(s0, vol, timeoutS)
```


## Proxy server/host down

When a proxy host shuts down or a proxy server process is killed suddenly,
you should restart the host and server process, and kick wlog-transfer tasks of storage servers:
```
python> walbc.kick_all([s0, s1])
```
This is not necessary but recommended for faster invoking wlog-transfer tasks.

After all proxy servers are down, wlogs of each walb device can not be discarded
and free spaces to store wlogs decrease.
Finally, log devices will overflow.


## Archive server/host down

When an archive host shuts down or an archive server process is killed suddenly,
you should restart the host and server process, and kick wdiff-transfer tasks of proxy servers:
```
python> walbc.kick_all([p0, p1])
```
This is necessary because wdiff-transfer tasks in proxy servers may have stopped by timeout.
You can get wdiff-transfer is stopped or not as follows:
```
python> isWdiffSendError = walbc.is_wdiff_send_error(p0, vol, a0)
```
The return value is a boolean value.

If the archive server is unavailable for a long time,
Wdiff files temporary stored in proxy servers will be accumulated and
disk space for proxy serers will be exhaustive.
Such a situation will cause walb devices overflow.


## Data lost at storage server

If data in walb devices have lost, walb-tools can do nothing.

If metadata managed in the volume have lost,
you can recover the system as follows:
```
python> walbc.init_storage(sx, vol, wdev.path)
python> walbc.hash_backup(sx, vol)
```
You must execute the above commands for all all the pairs of `vol, `wdev`.
If replica volumes exists at another storage server,
hash backup is not required. just call `init_storage()` command.


## Data lost at proxy server

If metadata or wdiff files remporarily stored at a proxy servers have lost,
all the volumes that the primary archive server has their archive data
can not continue backup.

```
python> walbc.start_synchronizing(a0, vol)
python> walbc.hash_backup(sx, vol, timeoutS)
```
Assume `a0` is primary archive server.
You must call `start_synchronizing()` for all volumes existing in the backup group.
You must run hash backup for `sx`, `vol` pairs of all the backup targets.

If you are sure that no wdiff has lost for a volume at the proxy server,
you need not run hash backup for the volume.
If there exist lost wdiffs, archive servers can not get the wdiffs and
proxy servers accumulate newly generated wdiffs.

## Data lost at archive server

If persistent data like metadata, base lv, or wdiff files have lost
at an archive server, there are three different cases to recover:

- **case 1**: The archive server is not primary.
- **case 2**: The archive server is primary and there is sedondary archive server and all the volumes
  at the secondary server are in synchronizing mode (condition X).
- **case 3**: The archive server is primary and condition X is not satisfied.

### Case 1

You need do nothing unless you want to replicate volumes again.

### Case 2

Assume the primary archive server is `a0` and the secondary is `a1`.
Now `a1` has (or will have) all the archive data of `a0`.
You need to prepare new primary archive server.
You can make `a1` the new primary archive server as follows:

```
python> for vol in allVolumes:
...         walbc.stop_synchronizing(a0, vol)
```
Then restart all storage servers
changing `-archive` command line argument from host info of `a0` to one of `a1`.


### Case 3

Your archive data has lost from the world.
You need to re-run full backup for all volumes.

```
python> for sx, vol in allPairsOfStorageServerAndVolume:
...         walbc.full_backup(sx, vol, timeoutS)
```

-----
