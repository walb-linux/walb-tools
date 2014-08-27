# Server processes configuration

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


## Server process configuration example

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


## Server layout configuration in python

This is example setting of a simple backup group with
a simple server layout and two volumes
Modify and save it as `walb-config.py`.
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
python> execfile('walb-config.py')
```

For details of `Server`, `ServerLayout`, and `Controller` class,
see [python library](python.md) document.

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

-----
