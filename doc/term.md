# Terminology

## For block devices

- **block**: a fixed-size binary data (512B or 4KiB). A block device consists of a number of blocks. Each block is identified by logical block address.
- **logical block**: block in units of 512B.
- **physical block**: block in units of physical block size, which is 512B or 4KiB, depending on underlying block devices. All block devices support atomicity of reading/writing a physical block.
- **IO**: input/output from/to a block device. An IO has its address, size, read or write, and its read/written data.
- **logical duplicate**: IO data of the overlapped address range of two IOs.
- **logical compression**: eliminating logical duplicates.
- **volume**: block devices, or virtual block devices managed by walb-tools.


## For snapshot/diff semantics

- **gid**: generation id of a volume.
  Gid increases monotonically. A clean snapshot is expressed as a gid.
  A dirty snapshot is expressed as a gid range.
- **clean snapshot**: logical point-in-time copies of volumes. All blocks with the same time point are copied. The time point is expressed as a gid.
- **dirty snapshot**: not point-in-time but all copied blocks must be in a finite time range.
  The time range is expressed as a gid range.
  We can get a clean snapshot using a dirty snapshot data and its corresponding wlogs.
- **diff**: A set of written IO data that is difference between a snapshot and another snapshot.
  Applying a diff to a snapshot will produce another snapshot.
  IO data are sorted by address and there is no logical duplicate.
- **clean diff**: A clean diff is expressed as two clean snapshots.
  THese diffs are generated from wlogs, sorting IOs by address and removing logical duplicates.
  These diffs will be stored as wdiff files in proxy/archive servers.
- **dirty diff**: A dirty diff is expressed as two snapshots one or both of which are dirty.
  These diffs are generated from hash-backup or hash-replication.
  These diffs will be stored as wdiff files in archive servers.


## For walb data

- **wdev**: a walb device. It acts as an ordinal block device except for having capability of storing and extracting wlogs. It consists two underlying block devices, `ldev` and `ddev`.
- **wldev**: walb log devices. They are special block device for walb-tools to extract wlogs from. This is thin wrapper of its `ldev`.
- **ldev**: underling log device of a walb device. The format is original and wlogs will be stored in the ring buffer that is almost all of the log device capacity.
- **ddev**: underlying data device of a walb device. The format is the same as the walb device.
- **lsid**: log sequence id. Virtual physical block address of a infinite log device.
  Finite-sized log storage will be used as a ring buffer in fact.
- **logpack**: contiguous several write IOs with a header physical block.
- **wlog**: walb log data which are contiguous logpacks, stored by walb kernel driver and extracted from walb-storage server to transfer. Walb log files with additional small metadata.
- **wdiff**: walb diff data or files. A representation of `diff`. Several compression format of write IO data are supported.
- **backup**: copying a snapshot of a volume from a storage server to the primary archive server.
  - **full-backup**: copying all blocks a volume in a storage server and they will be stored to its corresponding LVM volume called `base-lv` in the primary archive server.
  If the volume data are changed by write IOs during copy, the copied data will be dirty and the corresopnding wlogs will be required to restore a clean snapshot.
  - **hash-backup**: copying blocks of a volume in a storage server,
  comparing hash values of chunks (a fixed number of blocks) with the same chunks in the primary archive server.
  The copied blocks will be converted to dirty wdiff files.
- **replication**: copying a backuped data from an archive server to another archive server.
  - **full-repl**: copying blocks of the base-lv of a volume from an archive server to another.
  - **hash-repl**: copying blocks like hash-backup from an archive server to another.
  - **diff-repl**: copying wdiff files from an archive server to another.
- **base-lv**: a LVM logical volume to store full image of a dirty/clean snapshot of a volume in an archive server.
- **apply wlogs/wdiffs**: execute write IOs of wdiff files on a base-lv of a volume.
- **merge wlogs/wdiffs**: merge several wdiff files to one wdiff file. Logical duplicates will be removed.
- **restore**: restore a clean snapshot of a volume in an archive server
  using LVM writable snapshots.
- **overflow**: the situation that older wlogs will be overwritten in the ring buffer of a walb device.
  Wlogs have been lost and hash-backup is required to continue backup.
  You must execute `reset-wal` on the device to recover from overflow.
- **reset-wal**: freeze a walb device temporarily and initialize its ring buffer. Overflow will be recovered and all the stored wlogs are lost.
- **freeze**: make a walb device freezed state where write IOs for the walb device are queued and
there is no write IOs issued to the underlying `ldev`/`ddev`.
- **melt**: make a freezed walb device to an normal state.


## For walb system

- **storage**: walb-storage server processes or hosts where they run.
- **proxy**: walb-proxy server processes or hosts where they run.
- **archive**: walb-archive server processes or hosts where they run.
- **walbc**: an executable to control walb server processes.
- **wdevc**: an executable to contorl walb devices.
- **synchronizing**: **QQQ**
- **state**: each volume in each server has a state. Some commands will change the state and each command may be rejected due to the state is not acceptable to run the command.
- **action**: back ground tasks in a server process that is required from the outside using `walbc` tool. If some actions are running, an action may be rejected to run before finishing the running actions due to action dependencies.

-----
