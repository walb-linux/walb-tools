# Walb-tools Overview

## What are walb-tools?

Backup and replication software for walb devices.
WalB kernel device driver is available here:
[walb repository on GitHub](https://github.com/starpos/walb/)

Walb-tools mainly consists of three servers storage/proxy/archive and
their controller walbc, and walb device controller wdevc.

## Executables

- **wdevc**: control walb devices.
- **walbc**: control daemons.
- servers:
  - **walb-storage**: monitor and extract wlogs. execute full/hash backup.
  - **walb-proxy**: forward wlogs from storages to archives.
  - **walb-archive**: manage backup data and replicate them to another archive host.

There are also many undocumented executables in `binsrc/`.
They are used mainly for test and debug.

## Functionalities

- Backup and asynchronous replication of walb devices.
- Utilization of log spaces of walb devices using proxies.
- Archive data management with lvm volumes and snapshots.
- Diff data consolidation and compression.
  - automatically remove the logical overlapped IOs.
  - automatically compress/uncompress the diff data.

## Architecture

Several server layouts are available for several purposes for:

- data availability.
- system availability.
- operations of both backup and remote repication together.

## Protocols

- Original protocols on TCP/IP.
- **There is no security feature**.
  Do not use walb-tools on untrusted network.

-----
