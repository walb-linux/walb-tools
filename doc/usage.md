# Usage

## Design your backup/replication system

First of all, you must determine server layout where walb-tools work for your system.


### Requirements

You have a set of hosts that contains storage volumes to backup/replicate,
and another set of hosts that have has storages to store backup/replication archive data.
We call the former storage hosts, the latter archive hosts.

A `walb-storage` server process is required in each storage host,
and a `walb-archive` server process is required in each archive host.
In addition, one or more `walb-proxy` server processes are required
in some hosts.

- A `walb-storage` server process requires walb devices in the same host,
  and a directory to manage metadata.
- A `walb-proxy` server process requires a directory to store
  metadata and wdiff files temporarily.
- A `walb-archive` server process requires a LVM volume group where
  it manages LVM logical volumes as full archive images of volumes.
  It also requires a directory to store metadata and
  wdiff files permanently. Of course older snapshots must be useless for you
  and you can save storage by applying and removing corresponding wdiff files.

You need to determine running what server processes in which hosts.
We call a set of walb-tools servers with a certain layout **backup group**.

We recommend you to keep number of servers in backup groups as small as possible.
If you have many hosts/servers and many volumes, you should separate them to
many backup groups of small number of hosts/servers.


### How to control server processes

You can send commands to server processes  using `walbc` executable directly to control them,
and they communicate each other automatically.

It is often required that many commands execution with `walbc` to achieve
operations that system administrators usually want to do.
We recommend to use the `walb.py` which is python library/script to
controll a whole walb system using more conventional APIs.

In this document, we do not use `walbc` directly but
python APIs to control walb systems.


### Duplicates for several purpuses

You may require more hosts/servers to achieve the following functionalities:
- Two storage servers for software RAID1 system using walb devices.
- Two or more proxy servers for availability of continuous wlog transferring.
- Two or more archive servers to achieve both local backup and remote replication.


### Example: the simplest layout

- one storage: `s0`
- one proxy: `p0`
- one archive: `a0`

```
+-----------------+
|       s0        | -+
+-----------------+  |
  |                  |
  | wlog transfer    |
  v                  |
+-----------------+  |
|       p0        |  | full/hash backup
+-----------------+  |
  |                  |
  | wdiff transfer   |
  v                  |
+-----------------+  |
|       a0        | <+
+-----------------+
```

This is a minimal layout for walb-tools.
`s0` manages storage volumes, `p0` converts wlog data to wdiff data,
and `a0` manages archive data.
The host of `s0` and that of `a0` usually differs, while
The host of `p0` and the host of `a0` may be the same for simple systems.


### Example: the all duplicated layout

- two storages: `s0`, `s1`
- two proxies: `p0`, `p1`
- two archives: `a0`, `a1`

```
      +-----------------+          +-----------------+
 +----|       s0        |     +----|       s1        |
 |    +-----------------+     |    +-----------------+
 |             |              |             |
 +----------------------------+             |
 |             |                            |
 |             +----------------------------+
 |             |                            |
 |             v                            v
 |    +-----------------+          +-----------------+
 |    |       p0        |          |       p1        |
 |    +-----------------+          +-----------------+
 |             |                            |
 |             |                            |
 |             +----------------------------+
 |             |
 |             v
 |    +-----------------+          +-----------------+
 +--->|       a0        |--------->|       a1        |
      +-----------------+          +-----------------+
```

A backup group must have one primary archive server.
Assume `a0` is primary archive here.
All storage servers will connect to primary archive server `a0` and
`a0` deals with full/hash backup protocols.
You can change primary archive server dynamically.

Storages have priorities of proxies.
All storage servers will prefer `p0` to `p1` here.
While `p0` is down, storage servers use `p1` alternatively.
Two or more proxies reduces overflow risks of log spaces of each walb device.

A volume can be duplicated in `s0` and `s1`,
which belong to software RAID1 block devices.
This achieves availability of volumes even if walb-kernel-driver fails.
It is also useful for replacing storage hosts.
A volume is managed as master mode in one storage server and as slave mode in another.
In master mode, generated wlogs will be transferred to archive servers
through proxy servers. In slave mode, generated wlogs will be just removed.
Hash backup will be used to switch primary storage for volumes.

Archive data can be also duplicated in `a0` and `a1` using replicate command.
You can periodically replicate archive data from `a0` to `a1` to track the latest changes.
You can also automatically replicate archive data by making `a1` synchronizing mode.


### Other layouts

- You can use one archive server to backup all the volumes in many storage servers.
- You may use more proxy and archive servers for further redundancy.
- You can use multiple archive servers in a single host to utilize hardware resource
  while you want to keep serer layout simple.



## Server processes configuration

Each server process need a port (`PORT`) to listen TCP/IP connections,
and a directory (`BASE_DIR`) to store metadata and temporary/persistent data.
Identifier `ID` is optional but useful to identify servers.
Log output path `LOG_PATH` is also useful.

For storage servers,
the primary archive server information must be specified.
Proxy list must be also specified.
Let `HOST_INFO` be IP address and port concatinated by colon `:` like '192.168.1.1:10000'.
Let `PRIMARY_ARCHIVE_INFO` be a `HOST_INFO` for the primary archive server.
Let `PROXY_INFO_LIST` be `HOST_INFO` list separated by comma `,` to specify proxy servers.

For archive servers, LVM volume group `VG` is required for each archive.
The volume group must exists, and servers will create
logical volumes and snapshots in the volume group.

The following is typical command-line options for each kind of servers:
```
> sudo walb-archive -p PORT -b BASE_DIR -id ID -l LOG_PATH -vg VG &
> sudo walb-proxy -p PORT -b BASE_DIR -id ID -l LOG_PATH &
> sudo walb-storage -p PORT -b BASE_DIR -id ID -l LOG_PATH -archive PRIMARY_ARCHIVE_INFO -proxy PROXY_INFO_LIST &
```

For detail options, use `-h` option of server executables,
or see `get_server_args()` function of `stest/walb.py`.

You should start server process in order of archives, proxies, and storages.
Since proxy servers will connect archive servers in the background,
storage servers will connect proxy servers in the background.


### Server process configuration example

Let `hostS0`, `hostP0`, `hostA0` be hosts for your backup group,
all ports of servers be `10000`,
base directory of servers be `/var/walb/ID`,
log path of each servers be `/var/walb/ID.log`,
and LVM volume group for the archive serer be `vg0`.

Specify the following commands to start server processes:
```
@hostA0
> sudo mkdir -p /var/walb/a0
> sudo walb-archive -p 10000 -b /var/walb/a0 -id a0 -l /var/walb/a0.log -vg vg0 &
@hostP0
> sudo mkdir -p /var/walb/p0
> sudo walb-proxy -p 10000 -b /var/walb/p0 -id p0 -l /var/walb/p0.log &
@hostS0
> sudo mkdir -p /var/walb/s0
> sudo walb-storage -p 10000 -b /var/walb/s0 -id s0 -l /var/walb/s0.log \
-archive hostS0:10000 -proxy hostP0:10000 &
```

Each server executables are not daemon.
If you want to daemonize them, use daemontools, upstart, or so.


### Server layout configuration in python

This is example setting of a simple backup group with
a simple server layout and two volumes
Modify and save it as `walb-config0.py`.
This will be used to control your backup group also.

```python
from walb.walb import *

binDir = '/usr/local/bin/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'

def dataPath(s):
  return '/var/walb/%s/' % s

def logPath(s):
  return '/var/walb/%s.log' % s

s0 = Server('s0', 'hostS0', 10000, K_STORAGE, binDir, dataPath('s0'), logPath('s0'))
p0 = Server('p0', 'hostP0', 10000, K_PROXY,   binDir, dataPath('p0'), logPath('p0'))
a0 = Server('a0', 'hostA0', 10000, K_ARCHIVE, binDir, dataPath('a0'), logPath('a0'), 'vg0')

sLayout = ServerLayout([s0], [p0], [a0])
walbc = Controller(walbcPath, sLayout, isDebug=True)
```

You can load the configuration in a python shell as follows:
```
python> execfile('walb-config0.py')
```

You can get examples of each server command-line arguments as follows:
```
python> print ' '.join(get_server_args(s0, sLayout))
...
python> print ' '.join(get_server_args(p0, sLayout))
...
python> print ' '.join(get_server_args(a0, sLayout))
...
```

If you want use more detailed server layout, define additional servers and create a server layout
```python
sLayout = ServerLayout([s0, s1], [p0, p1], [a0, a1])
```
Again, former items have high priority in the proxy list (2nd argument), and
the first item of the archive list (3rd argument) will be primary archive server.



## Prepare walb devices

You can manage walb devices using `wdevc` executable.
While it is not so complex, you can use also python wrapper.
The following sections assume that appropriate server processes are running at each host.

### Device configuration

Assume you want to manage two walb devices at `s0` host.
Add the following code to `walb-config1.py`:

```python
runCommand = walbc.get_run_remote_command(s0)
wdev0 = Device(0, '/dev/ldev0', '/dev/ddev0', wdevcPath, runCommand)
wdev1 = Device(1, '/dev/ldev1', '/dev/ddev1', wdevcPath, runCommand)
```

- 1st argument is walb device iD. THe walb device path will `/dev/walb/iD`.
- 2nd argument is underlying log device path.
- 3rd argument is underlying data device path.
- Underlying devices must exist.
- `runCommand` runs executables in a remote host using walb-tools servers.
  If your python script is running on the host where you manage walb devices,
  use `run_local_command` instead `walbc.get_run_remote_command()`.

You can load the settings after loading `walb-config0.py`:
```
python> execfile('walb-config1.py')
```

You can use several member functions like
`format_ldev()`, `create()`, `delete()`, `exists()`, `resize()`, `reset()`
to manage walb devices.
For details of `Device` class, see [python library](python.md) document.

For example, you can format and create `wdev0` as follows:
```
python> wdev0.format_ldev()
python> wdev0.create()
```

**TODO**: support more detailed options for `format_ldev()` and `create()`.



## Basic backup operations

### Initailize volumes

Let volume names of `wdev0` and `wdev1` be `vol0` and `vol1` respectively.
You can initialize volumes in your backup group as follows:
```
python> walbc.init_storage(s0, 'vol0', wdev0.path)
python> walbc.init_storage(s0, 'vol1', wdev1.path)
```

After initialization, you can execute full backup of volumes.


### Full backup

```
python> gid = walbc.full_backup(s0, 'vol0')
```

`gid` is generation id of the clean snapshot of the backup.
You can use the `gid` to restore the snapshot.
After backup finished, the volume will be **synchronizing** mode, where
generated wlogs will be automatically transferred to the primary archive server `a0`.


### Take a snapshot

```
python> gid = walbc.snapshot(s0, 'vol0', [a0])
```

`gid` is the generation id of the taken snapshot. The snapshot must be clean.
`walbc.snapshot()` is synchronous command and will wait for the time
when the snapshot is restorable at `a0`. It will take some time.
If you like asynchronous behavior, use `walbc.snapshot_nbk()` instead.

Due to limitation of walb algorithm,
The taken snapshot may be lost because of data lost of the corresponding
wlogs/wdiffs data before arriving at archive servers.


### Restore

You can get restorable clean snapshots (as gid list) as follows:
```
python> walbc.get_restorable(a0, 'vol0', opt='')
```

You can restore a clean snapshot as follows:
```
python> walbc.restore(a0, 'vol0', gid)
```

Restored path is determined automatically, you can get it by calliing
`walbc.get_restored_path(a0, 'vol0', gid)`.

You can get restored volumes as follows:
```
python> walbc.get_restored(a0, 'vol0')
```

You can delete a restored volume as follows:
```
python> walbc.del_restored(a0, 'vol0', gid)
```


### Get status

You can get human-readable status of servers or their volumes.
```
python> walbc.status(self, sL=[], vol=None)
```

**TODO**: describe stauts output format.

There are also several *getter* functions:
- `walbc.get_alive_server()`
- `walbc.get_archive_info_list()`
- `walbc.get_host_type()`
- `walbc.get_server()`
- `walbc.get_state()`
- `walbc.get_uuid()`


### Resize

This command resizes a walb device.
Before calling this, you must resize the underlying data device.
Currently **shrinking** is not supported.

```
python> walbc.resize('vol0', sizeMb, doZeroClear=False)
```

doZeroClear may be not used. It is just for test.

There are more detailed functions for test/debug/troubleshooting:
`walbc.resize_storage()` and `walbc.resize_archive()`.


### Clear

You can clear a volume at a server. All related data for the specified volume
will be removed and the state of the volume will be `Clear`.
You must call at all servers to delete data for volumes completely from your backup group.

```
python> walbc.clear_vol(s, vol)
```

### Shutdown servers

YOu can use the following functions to shutdown servers:
```
python> walbc.shutdown(s, mode='graceful')
python> walbc.shutdown_all(mode='graceful')
python> walbc.shutdown_list(sL, mode='graceful')
```



## Manage archive data


### Apply wdiffs

Archive data for a volume consist of a base lv and wdiff files.
Disk usage must increase as the corresponding walb device will be written.
You must **explicitly** call this function to reduce disk usage
by removing older snapshots.

```
python> walbc.apply_diff(ax, vol, gid)
```

All the snapshots older than the snapshot of `gid` will be removed
and the corresponding wdiff files will be removed also.
A snapshot has timestamp so you can choose appropriate `gid`.


### Merge wdiffs

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





QQQ

-----
-----




walbc.hash_backup(sx, vol, timeoutS=100, block=True)


walbc.is_overflow(sx, vol)
walbc.is_synchronizing(ax, vol)
walbc.is_wdiff_send_error(px, vol, ax)

walbc.kick_all(sL)
walbc.kick_all_storage()




walbc.verify_not_overflow(sx, vol)
walbc.verify_not_restorable(ax, vol, gid, waitS, msg)
walbc.verify_state(s, vol, state)
walbc.wait_for_replicated(ax, vol, gid, timeoutS=100)
walbc.wait_for_restorable(ax, vol, gid, timeoutS=100)
walbc.wait_for_restored(ax, vol, gid, timeoutS=100)
walbc.wait_for_stopped(s, vol, prevSt=None)




## Miscellaneous stuffs

```
python> walbc.start(s, vol)
python> walbc.stop(s, vol, mode='graceful')
python> walbc.stop_nbk(s, vol, mode='graceful')
```






QQQ




QQQ


## Manage volumes at storage server

QQQ

```
walbc.set_slave_storage(sx, vol)
```


## Replicate

QQQ

```
walbc.replicate(aSrc, vol, aDst, synchronizing, timeoutS=100)
walbc.replicate_once(aSrc, vol, aDst, timeoutS=100)
walbc.replicate_once_nbk(aSrc, vol, aDst)
walbc.start_synchronizing(ax, vol)
walbc.stop_synchronizing(ax, vol)
walbc.synchronize(aSrc, vol, aDst, timeoutS=100)
walbc.add_archive_to_proxy(px, vol, ax, doStart=True)
walbc.del_archive_from_proxy(px, vol, ax)
walbc.copy_archive_info(pSrc, vol, pDst)
```


## Trouble shooting

QQQ

```
python> walbc.reset_vol(s, vol)
```

**TODO**




## Change server layout

walbc.set_server_layout(sLayout)


QQQ






-----
-----
-----

# Under construction

```
python> walbc.full_backup(s0, wdev0, timeoutS=100, block=True)
```


## Replicate a volume

## Make a volume synchronizing to an archive server

## Recovery from an error

## Replace hosts



-----
