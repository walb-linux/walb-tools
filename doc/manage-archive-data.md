# Manage archive data


## Apply wdiffs

Archive data for a volume consist of a full image stored in a base lv and wdiff files.
Disk usage increases as the corresponding walb device is updated by write IOs.
You must **explicitly** call the following function to reduce disk usage
by removing older snapshots.

```
python> walbc.apply_diff(ax, vol, gid)
```

All the snapshots older than the snapshot of `gid` will be removed
and the corresponding wdiff files will be removed also.
Before removing the wdiff files, its base lv is updated by applying them.
A snapshot has a timestamp so you can choose appropriate `gid`.


## Merge wdiffs

While merging wdiffs ordinary occur at proxy servers automatically,
any wdiff files will not merged automatically at archive servers.
You must call merge functions explicitly at archive servers if necessary.

It is useful to consolidate overlapped blocks in several contiguous wdiff files to reduce disk usage.
Implicit snapshots will be removed by merging wdiffs.
Archive servers never remove snapshots taken explicitly,
avoiding mergeing their corresponding wdiff files.

The command line is here:
```
python> walbc.merge_diff(ax, vol, gidB, gidE)
```
`gidB` and `gidE` is gid range to try to merge. `gidB < gidE` must be satisfied.

**TODO**: describe parameters for merging.


-----
