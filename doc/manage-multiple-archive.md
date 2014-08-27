# Manage multiple archive servers

Here we describe to mange multiple archive servers in a backup group.

Assume the following server layout:
```
   +----+
+--| s0 |
|  +----+
|    |
|    v
|  +----+
|  | p0 |
|  +----+
|    |
|    v
|  +----+    +----+
+->| a0 |    | a1 |
   +----+    +----+
```

`a0` is primary archive server.
You want to get replica of archive data in `a1`.
You can use replicate command in such situations to keep track of the latest walb device state.


## Replicate

Run the following command to replicate once form `a0` to `a1`.
```
python> walbc.replicate_once(a0, vol, a1, timeoutS)
```

Then you will get a replica of the volume at `a1`.
All the snapshots of the volume at `a0` can be restored at `a1` also.

Alternatively, you can use `walbc.replicate()` function
with `synchronizing` argument `False`.

This is synchronous function.
Use `replicate_once_nbk()` for asynchronous behavior.

The original archive data at `a0` will be updated by additional wdiffs.
You can invoke `replicate_once()` periodically to keep track of the latest update.
Alternatively, you can make the volume in `a1` **synchronizing** mode.


## Synchronizing mode

To make a volume at `a1` synchronizing mode,
you just run the following command:
```
python> walbc.synchronize(a0, vol, a1, timeoutS)
```

After becoming synchronizing mode, update wdiffs of the corresponding update of the walb device
will be automatically transferred to `a1`. It means that `a1` keeps track of the latest archive data
without running `walbc.replicate_once()` periodically.

`synchronize()` function will do the following operations internally:

1. stop all the proxy servers.
1. add the archive info of `a1` for the volume to all the proxy servers.
1. call `replicate_once()` to replicate data completely.
1. start all the proxy servers.

If you want to make proxy servers down time shorter,
call `replicate_once()` before calling `synchronize()`.
Alternatively, you can use `walbc.replicate()` function
with `synchronizing` argument `True`.

After synchronizing, we denote the layout as follows:
```
   +----+
+--| s0 |
|  +----+
|    |
|    v
|  +----+
|  | p0 |------+
|  +----+      |
|    |         |
|    v         v
|  +----+    +----+
+->| a0 |    | a1 |
   +----+    +----+
```

The primary archive server is always synchronizing mode after full backup.
Now the difference of `a0` and `a1` is just the full/hash backup commands at storage servers
still use `a0` as the backup destination automatically.

You can stop synchronizing of a volume at `a1` as follows:
```
python> walbc.stop_synchronizing(a1, vol)
```

`stop_synchronizing()` function will do the following operations internally:

1. stop all the proxy servers.
1. delete the archive info of `a1` for the volume from all the proxy servers.
1. start all the proxy servers.

The archive data for the volume at `a1` are kept but will not updated without
explicit calling of `replicate_once()`.


You can confirm a volume `vol` at an archive server `ax` is in synchronizing mode or not as follows:
```
python> isSynchronizing = walbc.is_synchronizing(ax, vol)
```
It returns a boolean value.

-----
