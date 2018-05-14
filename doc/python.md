# Python library

You can manage backup groups using python library `walb.py`
much easier than using `walbc` and `wdevc` directly.


## Classes

| Name             | Description                 |
|:-----------------|:----------------------------|
| **Device**      | control a walb device       |
| **Server**       | manage a server information |
| **ServerLayout** | manage a server layout      |
| **Controller**   | control a backup group      |


You can control a walb device by creating `Device` object.
Its constructor is here:
```python
def __init__(self, iD, ldev, ddev, wdevcPath, runCommand=run_local_command):
    '''
    iD :: int               - walb device id.
    ldev :: str             - underlying log block device path.
    ddev :: str             - underlying data block device path.
    wdevcPath :: str        - wdevc path.
    runCommand:: RunCommand - function that run commands.
    '''
```

You can control a backup group by creating `Controller` object.
Its constructor is here:
```python
def __init__(self, controllerPath, sLayout, isDebug=False):
    '''
    controllerPath :: str   - walb controller executable path.
    sLayout :: ServerLayout - server layout.
    isDebug :: bool
    '''
```

ServerLayout constructor is here:
```python
def __init__(self, storageL, proxyL, archiveL):
    '''
    storageL :: [Server] - storage server list.
    proxyL :: [Server]   - proxy server list.
                           Before items have high priority.
    archiveL :: [Server] - archive server list. The head is primary server.
    '''
```

Server constructor is here:
```python
def __init__(self, name, address, port, kind, binDir, dataDir,
             logPath=None, vg=None):
    '''
    name :: str            - daemon identifier in the system.
    address :: str         - host name
    port :: int            - port number.
    kind :: int            - K_STORAGE, K_PROXY, or K_ARCHIVE.
    binDir :: str          - directory path containing server executable
                             at the host.
    dataDir :: str         - persistent data directory path.
    logPath :: str or None - log path. None means default.
    vg :: str              - volume group name.
                             This is required by archive server only.
    '''
```


## Remote command execution

Several functions and classes supports remote command execution using `RunCommand` interface. However, the default configuration does not permit remote command execution. If you want to execute remote command, you should run `make` with `ENABLE_EXEC_PROTOCOL=1` .

See `RunCommand` type in `walb.py`.

```python
'''
RunCommand type is function with arguments type (args, isDebug).
  args :: [str]   - command line arguments.
  isDebug :: bool - True to put debug messages.
  return :: str   - stdout of the command.

'''
```

RunCommand type is the same as `run_local_command()`.
A run command is expected to execute an executable specified as full path
with command-line arguments at a host and return stdout as a string,
if exit code of the execution is 0, then an Exception will be raised.

You can define arbitorary run command as you like.
Walb-tools controller provides run command generator:
`Controller.get_run_remote_command()`.
This assumes a walb-XXX server process is running at the target host.
It will use `walbc exec` and forward command-line arguments
to the target host through walb-tools exec protocol
in order to execute it at the target host.

Except for startup of server processes at their target hosts,
any command execution at each host are achieved using walb-tools exec protocol
in `walb.py`.

----
