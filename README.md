## What are walb-tools?

Backup and replication software for walb devices.
WalB kernel device driver is available here:
[walb repository](https://github.com/starpos/walb/)

For details, see [doc/REAMDE.md].

## Directories

- doc: documents.
- binsrc: source files of executables.
- include: header files.
- src: source files.
- utest: unit test code.
- itest: integration test code.
- stest: scenario test code.

## Requirements for build

- C++11 compiler.
- libaio.
- Compression libraries: libsnappy, liblzma, libz.
- Header files of WalB kernel driver.
- cybozulib. [GitHub repository](https://github.com/herumi/cybozulib/)

## License

GPLv2 or 3
