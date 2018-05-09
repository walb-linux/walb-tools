# Prepare walb devices

You can manage walb devices using `wdevc` executable.
While it is not so complex, you can use also python wrapper.
The following sections assume that appropriate server processes are running at each host.

## Device configuration

Assume you want to manage two walb devices at `s0` host.
Add the following code to `walb-config.py`:

```python
runCommand = walbc.get_run_remote_command(s0)
wdev0 = Device('vol0', '/dev/ldev0', '/dev/ddev0', wdevcPath, runCommand)
```

- 1st argument is walb device name. THe walb device path will `/dev/walb/name`.
- 2nd argument is underlying log device path.
- 3rd argument is underlying data device path.
- Underlying devices must exist.
- `runCommand` runs executables in a remote host using walb-tools servers.
  If your python script is running on the host where you manage walb devices,
  use `run_local_command` instead `walbc.get_run_remote_command()`.

You can use several member functions like
`format_ldev()`, `create()`, `delete()`, `exists()`, `resize()`, `reset()`
to manage walb devices.
For details of `Device` class, see [python library](python.md) document.

For example, you can format and create `wdev0` as follows:
```
python> wdev0.format_ldev()
python> wdev0.create()
```
Verify the existence of `/dev/walb/0`.

**TODO**: support more detailed options for `format_ldev()` and `create()`.

Indeed, `Device` class is a wrapper of `wdevc` command.
You can use `wdevc` directly.


-----
