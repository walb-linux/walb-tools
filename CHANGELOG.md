# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security

## [1.0.13] - 2018-03-08
### Changed
- **CAUSION**: internal protocols were changed and renamed.
  - `dirty-full-sync` --> `dirty-full-sync2`
  - `dirty-hash-sync` --> `dirty-hash-sync2`
  - `repl-sync` --> `repl-sync2`
- walb-storage and walb-archive support `-sync-cmpr` option
  to select compression type for full/hash backup
  and full/hash/resync replication.
- walb-storage and walb-proxy support `-maxdelay` option to achieve
  exponential retry intervals.
### Fixed
- bugfix: avoid `bad state:WlogRecv` error due to timing.
- bugfix: can not use spare proxy when walb-proxy reject transfer
  such as `bad state:WlogRecv`.
- fix: improve TaskQueue.
- bugfix: level setting of zstd compression was ignored.

## [1.0.12] - 2018-02-27
### Changed
- merge task can start during restore/replication client running.
### Fixed
- bugfix: exceptions thrown in MultiThreadedServer::run() when long tasks are running cause
  the state the reject commands via TCP connections until the tasks finish.
- fix: forget to specify `DIRECT_IO`.

## [1.0.11] - 2018-02-15
### Changed
- walb-archive supports `-apply-sleep-pct` option.
- apply/restore tasks use aio instead of noral read/write systemcalls.
### Fixed
- regression: 1.0.9: data lost of dirty full sync server.

## [1.0.10] - 2018-02-08
### Fixed
- bugfix: full replication resume will discard *all* the area of the base image and previous transferred data will be updated by zeroes.

## [1.0.9] - 2018-02-05
### Changed
- change default parameters.
- using TCP backlogs of OS instead of self-managed queues.
### Fixed
- bugfix: connection timeout from walb-storage to walb-proxy causes connection failure to spare proxy.
- full backup/replication uses aio to write base images for performance.

## [1.0.8] - 2018-01-16
### Changed
- force resync if the option is specified in replication command.

## [1.0.7] - 2017-11-17
### Changed
- update cybozulib.
### Fixed
- bugfix: proxy: meta diff garbage may remain and try to send non-existing wdiff files forever.
- bugfix: forget to clean up removing cold snapshots.

## [1.0.6] - 2017-09-14
### Changed
- lvm: use `-K` option for newer lvm tools to activate dm-thin volumes explicitly.
### Fixed
- bugfix: meta: applicable diff searching may fail even if they exist.

## [1.0.5] - 2017-06-18
### Changed
- **CAUSION**: you need not get cybozulib and walb-driver additionaly to build walb-tools now.
  - import cybozulib headers to the walb-tools repository directly.
  - import header files of walb kernel driver to the walb-tools repository directly.
- walb-worker supports `general.timeout` parameter.
- add `get base-all` command.
- support Travis CI.
- fix documents.
### Fixed
- archive: apply/restore can work even if too many wdiff files are required to apply.
- avoid unnecessary read-ahead of log devices.
- improve algorithm of applicable diff list generation.
- and many fixes.

## [1.0.4] - 2017-04-11
### Added
- supports new compression type for wdiffs: lz4 and zstd.
### Chagned
- support indexed wdiff format to improve wlog-transfer performance (proxy only).
  privous format is called sorted wdiff format.
- reduce memcpy calls.

## [1.0.3] - 2017-04-03
### Changed
- walb-worker supports `-vv` option.
### Fixed
- bugfix: gzip uncompress failed.

## [1.0.2] - 2017-03-15
### Added
- add `get handler-stat` command.
- add `-maxconn` option.
- add `-keep-one-cold-snap` option.
### Changed
- move much code from hpp files to cpp files.
### Fixed
- bugfix: `tryChange is not called` log messages and wdif transfer stops.

## [1.0.1] - 2017-02-28
### Changed
- improve performance.
### Fixed
- non-heavy fixes.

## [1.0.0-b32] - 2016-09-28
### Fixed
- bugfix: walb-worker: replication target selection does not work well.

## [1.0.0-b31] - 2016-09-28
## [1.0.0-b30] - 2016-09-26
## [1.0.0-b29] - 2016-09-12
### Chagned
- **CAUSION**: wdiff format is changed to version 2.
  - in order to support 32bit IO size instead 16bit due to Linux kernel 4.3 change.

## [1.0.0-b28] - 2016-09-07
### Changed
- change behavior of `get latest-snap` and `get ts-delta` command for monitoring.

## [1.0.0-b27] - 2016-08-26
## [1.0.0-b26] - 2016-08-25
## [1.0.0-b25] - 2016-08-09
### Added
- commands for monitoring
  - `get latest-snap`
  - `get log-usage`
  - `get ts-delta`

## [1.0.0-b24] - 2016-06-14
## [1.0.0-b23] - 2016-06-09
## [1.0.0-b22] - 2016-05-27
## [1.0.0-b21] - 2016-04-25
## [1.0.0-b20] - 2016-03-29
## [1.0.0-b19] - 2016-03-15
## [1.0.0-b18] - 2016-02-05
## [1.0.0-b17] - 2015-12-11
## [1.0.0-b16] - 2015-11-20
## [1.0.0-b15] - 2015-11-19
## [1.0.0-b14] - 2015-11-12
## [1.0.0-b13] - 2015-11-04
## [1.0.0-b12] - 2015-10-09
## [1.0.0-b11] - 2015-10-01
## [1.0.0-b10] - 2015-09-11
## [1.0.0-b9] - 2015-09-02
## [1.0.0-b8] - 2015-08-21
## [1.0.0-b7] - 2015-08-12
## [1.0.0-b6] - 2015-08-12
## [1.0.0-b5] - 2015-07-30
## [1.0.0-b4] - 2015-07-27
## [1.0.0-b3] - 2015-06-30
## [1.0.0-b2] - 2015-06-24
## [1.0.0-b1] - 2015-06-24
