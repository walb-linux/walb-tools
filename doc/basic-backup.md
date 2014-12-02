# Basic backup operations

Assume the minimal layout as follows:
```python
sLayout = ServerLayout([s0], [p0], [a0])
```



## Initialize volumes

Let volume name of `wdev0` be `VOL=vol0`.

You can initialize volumes in your backup group as follows:
```
python> walbc.init_storage(s0, VOL, wdev0.path)
```

REMARK : All states of servers must be `Clear` to run the above command.
check it with `walbc.get_state_all(VOL)`.

Storage servers know just the relationship of volume name and walb device path.
You must manage relationship of volumes and walb devices by yourself.

After initialization, you can execute full backup of the initialized volumes.



## Full backup

```
python> gid = walbc.full_backup(s0, VOL)
```
`gid` is generation id of the clean snapshot of the backup.
You can use the `gid` to restore the snapshot.
After full backup finished, the volume will be in **synchronizing** mode,
where generated wlogs will be automatically transferred to the primary archive server `a0`.
In general, this behavior is called continuous data protection (CDP) or asynchronous replication.

This is a wrapper of `walbc full-bkp` command.
It is **asynchronous** (non-blocking) command while the python wrapper is **synchronous** (blocking).
If you want asynchronous behavior, you can make `block` argument `False`.



## Take a snapshot

```
python> gid = walbc.snapshot(s0, VOL, [a0])
```

`gid` is the generation id of the taken snapshot. The snapshot is clean.
`walbc.snapshot()` is synchronous command and will wait for the time
when the snapshot is restorable at `a0`. It will take some time where
the corresponding wlogs are converted to wdiffs and transferred to the primary archive server.
If you like asynchronous behavior, use `walbc.snapshot_nbk()` instead.

Snapshot command will fail if log device overflows or full/hash backup was not performed.
See [Recover from errors or failures](recover.md) document.

Due to limitation of walb algorithm,
The taken snapshot may be lost because of data lost of the corresponding
wlogs/wdiffs data before arriving at archive servers.



## Restore

You can get restorable clean snapshots (as gid list) as follows:
```
python> gidL = walbc.get_restorable(a0, VOL)
```

**TODO**: add option to get_restorable() that shows timestamp of snapshots.

You can restore a clean snapshot as follows:
```
python> walbc.restore(a0, VOL, gid)
```
This may take much time, you can use `timeoutS` argument.

The restored path is determined automatically, you can get it by calling
`walbc.get_restored_path(a0, VOL, gid)`.

You can get all the restored volumes as follows:
```
python> gidL = walbc.get_restored(a0, VOL)
```

You can delete a restored volume as follows:
```
python> walbc.del_restored(a0, VOL, gid)
```



## Get status

You can get human-readable status of servers or their volumes.
```
python> walbc.status(self, sL=[], vol=None)
```
Specify server list to `sL`, volume name to `vol`.
If volume name is not specified, summary of all volumes will be put.

**TODO**: describe status output format.

There are several getter functions for scripts:
- `walbc.get_alive_server()`
- `walbc.get_archive_info_list()`
- `walbc.get_host_type()`
- `walbc.get_server()`
- `walbc.get_state()`
- `walbc.get_uuid()`
- `walbc.print_diff_list()`



## Resize

This command resizes a walb device.
Before calling this, you must resize the underlying data device.
Currently **shrinking** is not supported.

```
python> walbc.resize(VOL, sizeMb)
```

Do not call `wdevc.resize()` directly,
because metadata of servers contains device size information.

The original size can be obtained by calling `wdevc.get_size_mb()` or `wdevc.get_size_lb()`.

There are more detailed functions for test/debug/troubleshooting:
`walbc.resize_storage()` and `walbc.resize_archive()`.



## Clear

You can clear a volume at a server. All related data for the specified volume
will be removed and the state of the volume will be `Clear`.
You must call the function at all the servers to delete data of volumes completely from your backup group.

```
python> for s in [s0, p0, a0]:
...         walbc.clear_vol(s, VOL)
```


## Shutdown servers

You can use the following functions to shutdown servers:
```
python> walbc.shutdown(s, mode='graceful')
python> walbc.shutdown_list(sL, mode='graceful')
python> walbc.shutdown_all(mode='graceful')
```

`shutdown()` is used for single server.
`shutdown_list()` is used for multiple servers.
`shutdown_all()` is used for all servers in the backup group.

The `mode` can be one of `graceful`, `force`, and `empty`.
`empty` is only available for proxy servers and used in spacial cases.

If servers are killed suddenly by kill comand or so,
what you need to do is just restarting server process.
Managed data will be recovered automatically.

-----
