# Design your backup/replication system

First of all, you must determine server layout where walb-tools work for your system.


## Requirements

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


## How to control server processes

You can send commands to server processes  using `walbc` executable directly to control them,
and they communicate each other automatically.

It is often required that many commands execution with `walbc` to achieve
operations that system administrators usually want to do.
We recommend to use the `walb.py` which is python library/script to
controll a whole walb system using more conventional APIs.

In this document, we do not use `walbc` directly but
python APIs to control walb systems.


## Duplicates for several purpuses

You may require more hosts/servers to achieve the following functionalities:
- Two storage servers for software RAID1 system using walb devices.
- Two or more proxy servers for availability of continuous wlog transferring.
- Two or more archive servers to achieve both local backup and remote replication.


## Example: the minimal layout

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


## Example: the all duplicated layout

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


## Other layouts

- You can use one archive server to backup all the volumes in many storage servers.
- You may use more proxy and archive servers for further redundancy.
- You can use multiple archive servers in a single host to utilize hardware resource
  while you want to keep serer layout simple.

-----
