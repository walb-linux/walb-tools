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

Its definition in python is here:
```python
sLayout = ServerLayout([s0], [p0], [a0, a1])
```

`a0` is primary archive server.
You want to get replica of archive data in `a1`.
You can use replicate command in such situations to keep track of the latest walb device image.


## Replicate

Run the following command to replicate form `a0` to `a1` once.
```
python> walbc.replicate_once(a0, vol, a1, timeoutS)
```

This function checks what type of replication is required from the source to the destination,
then executes replication with the chosen type.
The types of replication are full, hash, or diff replication,
which are almost the same protocols as full/hash backup, and wdiff-transfer protocol.

After the function finished, you will get a replica of the volume at `a1`.
All the explicit snapshots of the volume at `a0` can be restored at `a1` also.
Implicit snapshots may be removed by replication by dynamic merging of wdiffs.

Alternatively, you can use `walbc.replicate()` function
with `synchronizing` argument `False`.

This is synchronous function.
Use `replicate_once_nbk()` for asynchronous behavior.

The original archive data at `a0` may be updated by additional wdiffs during replication.
You can invoke `replicate_once()` periodically to keep track of the latest update.
Alternatively, you can make the volume in `a1` **synchronizing** mode.


## Synchronizing mode

To make a volume at `a1` synchronizing mode,
you just run the following command:
```
python> walbc.synchronize(a0, vol, a1, timeoutS)
```

After changing to synchronizing mode, updated wdiffs generated from the walb device
will be automatically transferred to not only `a0` but also `a1`.
It means that `a1` keeps track of the latest archive data
without running `walbc.replicate_once()` periodically.

`synchronize()` function will do the following operations internally:

1. stop all the proxy servers.
1. add the archive info of `a1` for the volume to all the proxy servers.
1. call `replicate_once()` to replicate data completely (because proxy servers are stopped).
1. start all the proxy servers.

If you want to make downtime of proxy servers shorter,
call `replicate_once()` before calling `synchronize()`.
Alternatively, you can use `walbc.replicate()` function
with `synchronizing` argument `True`.

After synchronizing, the data transfer paths will change as follows:
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

The primary archive server will be synchronizing mode automatically after full backup.
Now the difference of `a0` and `a1` is just the full/hash backup commands at storage servers
still use `a0` as the backup destination automatically.

You can stop synchronizing mode of a volume at `a1` as follows:
```
python> walbc.stop_synchronizing(a1, vol)
```

`stop_synchronizing()` function will do the following operations internally:

1. stop all the proxy servers.
1. delete the archive info of `a1` for the volume from all the proxy servers.
1. start all the proxy servers.

The archive data for the volume at `a1` are kept but will not updated without
explicit calling of `replicate_once()`.


You can confirm volume `vol` at archive server `ax` is in synchronizing mode or not as follows:
```
python> isSynchronizing = walbc.is_synchronizing(ax, vol)
```
It returns a boolean value.

-----
