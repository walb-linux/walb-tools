[![Build Status](https://travis-ci.org/walb-linux/walb-tools.png)](https://travis-ci.org/walb-linux/walb-tools)
## What are walb-tools?

Backup and replication software for walb devices.
WalB kernel device driver is available here:
[walb repository](https://github.com/walb-linux/walb-driver/)

For details, see [doc/README.md](doc/README.md).

## Directories

- doc: documents.
- binsrc: source files of executables.
- include: header files.
- src: source files.
- utest: unit test code.
- itest: integration test code.
- stest: scenario test code.
- walb: header files from walb kernel driver.
- cybozulib: header files from cybozulib. [GitHub repository](https://github.com/herumi/cybozulib/)

## Requirements for build

- C++11 compiler.
- libaio.
- Compression libraries: libsnappy, liblzma, libz.

## Copyright

(C) 2013 Cybozu Labs, Inc.

## License

GPLv2 or 3
