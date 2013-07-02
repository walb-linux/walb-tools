## What are walb-tools?

Backup and replication software for walb devices.
WalB kernel device driver is available here: [WalB repository](https://github.com/starpos/walb/).

### Executable binaries

- walb-client: control backup and replication.
- walb-worker: monitor and wlog extraction daemon.
- walb-proxy: forward wlog data from workers to servers.
- walb-server: manage backup data.

### Requirements for build

- C++11 compiler.
- Header files of WalB kernel driver.
- libaio.

## License

GPLv2 or 3

