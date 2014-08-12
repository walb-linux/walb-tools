# Usage

## Design your backup/replication system

First of all, you must determine server layout where walb-tools work.


### Requirements

You have a set of hosts that contains storage volumes to backup/replicate,
and another set of hosts that have storages to store backup/replication archive data.
We call the former storage hosts, the latter archive hosts.

A `walb-storage` server process is required in each storage host,
and a `walb-archive` server process is required in each archive host.
In addition, one or more `walb-proxy` server processes are required
in some hosts.

A walb-storage server process requires walb devices in the same host,
and a directory to manage metadata.

A walb-proxy server process requires a directory to store
metadata and wdiff files temporarily.

A walb-archive server process requires a LVM volume group where
it manages the full image of each volume as a LVM logical volume.
A walb-archive requires a directory to store metadata and
wdiff files permanently. Of course older snapshots must be useless for you
and you can save storage by applying and removing corresponding wdiff files.


### How to control server processes

You can send commands to server processes  using `walbc` executable directly to control them,
and they communicate each other automatically.

It is often required that many commands execution with `walbc` to achieve
an operation that system administrators usually wants to do.
We recommend to use the `walb.py` which is python library/script to
controll a whole walb system using more conventional APIs.

In this document, we do not use `walbc` directly but
python APIs to control walb systems.


### Make backup groups small

We also recommend to keep a set of servers as small as possible.
If you have many hosts and many volumes, you should separate them to
many groups of small number of hosts.
We consider such a small group of hosts here, called **backup group**.


### Duplicates for several purpuses

You may require more hosts to achieve the following functionalities:
- Two storage servers for software RAID1 system using walb devices.
- Two or more proxy servers for availability of continuous wlog transferring.
- Two or more archive servers to achieve both local backup and remote replication.


### Example: the simplest layout

- one storage: `s0`
- one proxy: `p0`
- one archive: `a0`


### Example: the all duplicated layout

- two storages: `s0`, `s1`
- two proxies: `p0`, `p1`
- two archives: `a0`, `a1`

A backup group must have one primary archive server.
Assume `a0` is primary archive here.

Storages have priorities of proxies.
All storage servers will prefer `p0` to `p1` here.

A volume can be duplicated in `s0` and `s1`,
which belong to software RAID1 block devices.
This achieves availability of volumes
even if walb-kernel-driver fails and make a walb device stopped.


## Server processes configuration

Each server need a port (`PORT`) to listen TCP/IP connections,
and a directory (`BASE_DIR`) to store metadata or temporary/persistent data.
Identifier `ID` is optional but useful to identify servers.
Log output path `LOG_PATH` is also useful.

For storage,
the primary archive information must be specified.
proxy list must be also specified.
Let `HOST_INFO` be IP address and port concatinated by colon `:` like '192.168.1.1:10000'.
Let `PRIMARY_ARCHIVE_INFO` be a `HOST_INFO`.
Let `PROXY_INFO_LIST` be `HOST_INFO` list separated by comma `,`.

For archives, LVM volume group `VG` is required for each archive.
The volume group must exists, and servers will create
logical volumes and snapshots in the volume group.

```
> sudo walb-archive -p PORT -b BASE_DIR -id ID -l LOG_PATH -vg VG &
> sudo walb-proxy -p PORT -b BASE_DIR -id ID -l LOG_PATH &
> sudo walb-storage -p PORT -b BASE_DIR -id ID -l LOG_PATH -archive PRIMARY_ARCHIVE_INFO -proxy PROXY_INFO_LIST &
```

For detail options, use `-h` option of server executables,
or see get_server_args() function of `stest/walb.py`.


### Server process configuration example

Let `hostS0`, `hostP0`, `hostA0` be hosts for your backup group,
all ports of servers be `10000.

Specify the following commands to start server processes.
```
@hostA0
> sudo walb-archive -p 10000 -b /var/walb/a0 -id a0 -l /var/walb/a0.log -vg vg0 &
@hostP0
> sudo walb-proxy -p 10000 -b /var/walb/p0 -id p0 -l /var/walb/p0.log &
@hostS0
> sudo walb-storage -p 10000 -b /var/walb/s0 -id s0 -l /var/walb/s0.log \
-archive hostS0:10000 -proxy hostP0:10000 &
```

Each server executables are not daemon.
If you want to daemonize them, use daemontools, upstart, or so on.


### Server layout configuration in python

This is example setting of a simple backup group with
a simple server layout and two volumes
Modify and save it as `walb-config.py`.
This will be used to control your backup group also.

```python
from walb import *

binDir = /usr/local/bin/
wdevcPath = binDir + 'wdevc'

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
python> execfile('walb-config.py')
```

You can get examples of each server command-line arguments as follows:
```
python> ' '.join(get_server_args(s0, sLayout))
python> ' '.join(get_server_args(p0, sLayout))
python> ' '.join(get_server_args(a0, sLayout))
```



## Prepare walb devices

You can manage walb devices using `wdevc` executable.
It is still not so complex, but you can use also python wrapper.

The following sections assume that appropriate server processes are running at each host.

### Device configuration

Assume you want to manage two walb devices at `s0` host.
Add the following code to `walb-config.py`:

```python
runCommand = walbc.get_run_remote_command(s0)
wdev0 = Device(0, '/dev/ldev0', '/dev/ddev0', wdevcPath, runCommand)
wdev1 = Device(1, '/dev/ldev1', '/dev/ddev1', wdevcPath, runCommand)
```

You can use several member functions like
`format_ldev()`, `create()`, `delete()`, `exists()`, `resize()`, reset()`.
For details of `Device` class, see [python library](python.md) document.


-----
# Under construction

## Initial full backup


You can

```
python> walbc.full_backup(s0, wdev0, timeoutS=100, block=True)
```


## Replicate a volume

## Make a volume synchronizing to an archive server

## Recovery from an error

## Replace hosts



-----
