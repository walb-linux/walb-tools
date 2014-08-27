# Manage archive data


## Apply wdiffs

Archive data for a volume consist of a full image stored in its base lv and wdiff files.
Disk usage will increase as the corresponding walb device will be written.
You must **explicitly** call this function to reduce disk usage
by removing older snapshots.

```
python> walbc.apply_diff(ax, vol, gid)
```

All the snapshots older than the snapshot of `gid` will be removed
and the corresponding wdiff files will be removed also.
Before removing the wdiff files, its base lv is updated by applying them.
A snapshot has timestamp so you can choose appropriate `gid`.


## Merge wdiffs

While merging wdiffs ordinary occur at proxy servers automatically,
you must call merge functions explicitly at archive servers if necessary.

It is useful to consolidate overlapped blocks in several wdiff files to reduce disk usage.
Of course (hidden) snapshots will be removed by merging wdiffs.
Clean snapshots taken explicitly will not be removed,
avoiding mergeing the corresponding wdiff files.

The command line is here:
```
python> walbc.merge_diff(ax, vol, gidB, gidE)
```
`gidB` and `gidE` is gid range to try to merge.

**TODO**: describe parameters for merging.


-----
