# Replace hosts

You can replace a host of a walb backup group.


## Replace storage host

Assume the following things:

- There are two storage servers `s0` and `s1` at distinct hosts.
- All the volumes have replicas at both `s0` and `s1`.
- You want to replace `s0` with `s2`.
- `targetVolumes` is volume list of backup target at `s0`.
- `targetWdevs` is wdev list of backup target at `s2`.
- `newServerLayout` is the new server layout, excluding `s0` and including `s2`.

The procedure:

- Prepare `s2` and make walb devices for replica volumes if necessary. (ex. add to a md device.)
- Make all the target volumes standby and set new server layout.
```
python> for vol in targetVolumes:
...         walbc.set_standby_storage(s0, vol)
python> walbc.set_server_layout(newServerLayout)
```
- Initialize volumes at `s2`:
```
python> for vol, wdev in alPairsOfVolAndWdev:
...         walbc.init_storage(s2, vol, wdev.path)
```
- Run hash backup of all the backup target volumes:
```
python> for vol in targetVolumes:
...         walbc.hash_backup(s2, vol)
```
- Now `s0` does not have backup target volumes and all the volumes are backup target at `s1` or `s2`.
  You can safely detatch `s0` from the backup group.


## Replace proxy host

Assume the following things:

- There are two proxy servers `p0` and `p1` at distinct hosts.
- You want to replace `p0` with `p2`.
- `newServerLayout` is the new server layout, excluding `p0` and including `p2`.

The procedure:

- Prepare `p2` and copy archive info of all the volumes:
```
python> for vol in allVolumes:
...         walbc.copy_archive_info(p0, vol, p2)
```
-  Empty wdiff files at `p0`:
```
python> for vol in allVolumes:
...         walbc.stop_nbk(p0, vol, 'empty')
python> for vol in allVolumes:
...         walbc.wait_for_stoped(p0, vol)
...         walbc.clear_vol(p0, vol)
```
- Set new server layout:
```
python> walbc.set_server_layout(newServerLayout)
```
- Restart all the storage servers changing `-proxy` command-line argument.
- Now `p0` does not have temporary wdiff files and all the storage servers
  will transfer wlogs to `p1` or `p2`.
  You can safely detatch `p0` from the backup group.


## Replace archive host (primary archive server)

Assume the following things:

- There are one archive server `a0`.
- You want to replace `a0` with `a2`.
- `newServerLayout` is the new server layout, excluding `a0` and including `a2`.

The procedure:

- Prepare `a2`.
- Replicate all the volumes and make them synchronizing mode:
```
python> for vol in allVolumes:
...         walbc.replicate(a0, vol, a2, True)
```
- Restart all the storage servers, changing `-archive` command-line argument.
  Specify host info of `a2` instead of `a0`.
- Stop synchronizing mode at `a0`:
```
python> for vol in allVolumes:
...         walbc.stop_synchronizing(a0, vol)
```
- Set new server layout:
```
python> walbc.set_server_layout(newServerLayout)
```
- Now `a0` does not have no volume in synchronizing mode
  and all the volumes are in synchronizing mode at `a2` and
  full/hash backup of all the storage servers will use `a2`.
  You can safely detatch `a0` from the backup group.


## Replace archive host (secondary archive server)

Assume the following things:

- There are two archive servers `a0`, and `a1`.
- The primary archive server is `a0`.
- You want to replace `a1` with `a2`.
- `newServerLayout` is the new server layout, excluding `a1`, and including `a2`.

The procedure:

- Prepare `a2`.
- Replicate all the volumes at `a1` to `a2`:
```
python> for vol in allVolumes:
...         isSync = walbc.is_synchronizing(a1, vol)
...         walbc.replicate(a1, vol, a2, isSync)
```
- Stop synchronizing mode at `a1`:
```
python> for vol in allVolumes:
...         if walbc.is_synchronizing(a1, vol):
...             walbc.stop_synchronizing(a1, vol)
```
- Now `a1` does not have no volume in synchronizing mode and all the archive data
  and synchronizing mode settings have been copied to `a2`.
  You can safely detatch `a1` from the backup group.


-----
