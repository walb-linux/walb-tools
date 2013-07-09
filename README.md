## What are walb-tools?

Backup and replication software for walb devices.
WalB kernel device driver is available here: [WalB repository](https://github.com/starpos/walb/).

### Executable binaries

- walb-client: control backup and replication.
- walb-worker: monitor and wlog extraction daemon.
- walb-proxy: forward wlog data from workers to servers.
- walb-server: manage backup data.

- wlog-XXX: tools for walb log data.
- wdiff-XXX: tools for walb diff data.

### Requirements for build

- C++11 compiler.
- libaio.
- libsnappy.
- Header files of WalB kernel driver.
- cybozulib [GitHub repository](https://github.com/herumi/cybozulib/).

## License

GPLv2 or 3

