# States and Actions

Each volume has a state at each server.
Each server may run several actions concurrently for a volume.

There are several kinds of tasks to run on a volume.
Tasks are classified to foreground or background.
Several foreground tasks may cause state transition of the volume.
Background tasks are also called action, which tend to take much time.
During running a background task on a volume, the corresponding action counter is incremented
to notify users the task has done or not yet.

There are two kinds of states: static and dynamic.
A volume is in a dynamic state when a foreground task is running on the volume and
the state indicates type of the task.


You can get the state of a volume.
```
> walbc -a HOST -p PORT get state VOLUME
```

Specify volume name at `VOLUME`.
You will get a string from stdout of the command.

In python code:
```
python> walbc.get_state(s, 'VOLUME')
```
This returns a string.


You can get number of actions running on a volume.
```
> walbc -a HOST -p PORT get num-action VOLUME ACTION
```

Specify action name at `ACTION`.
You will get an integer from stdout of the command.

In python code:
```
python> num = walbc.get_num_action(s, 'VOLUME', 'ACTION')
```
This returns a non-negative integer.

Almost python code will check states/actions using the above command internally,
so you need not call `walbc` command directly.

**You need to know requirements to run each command: which state of the volume and which actions running on it.**



## Storage server

| Static state | Desctiption                                      |
|:-------------|:-------------------------------------------------|
| `Clear`      | There is no metadata for the volume.             |
| `SyncReady`  | Initialized and ready to full/hash backup.       |
| `Stopped`    | Wlogs will be preserved in the log device.       |
| `Master`     | Master mode. Try to transfer wlogs to proxies.   |
| `Slave`      | Slave mode. Try to remove wlogs.                 |

- In `Master` mode, wlogs will be transferred and removed automatically,
  so the risk of log device overflow will be minimized.

| State transition                       | Description                                 |
|:---------------------------------------|:--------------------------------------------|
| `Clear` -> `InitVol` -> `SyncReady`    | Invoked by `init-vol` command.              |
| `SyncReady` -> `ClearVol` -> `Clear`   | Invoked by `clear-vol` command.             |
| `SyncReady` -> `StartSlave` -> `Slave` | Invoked by `start slave` command.           |
| `Slave` -> `StopSlave` -> `SyncReady`  | Invoked by `stop` command at slave mode.    |
| `SyncReady` -> `FullSync` -> `Stopped` | Invoked by `full-bkp` command.              |
| `SyncReady` -> `HashSync` -> `Stopped` | Invoked by `hash-bkp` command.              |
| `Stopped` -> `Reset` -> `SyncReady`    | Invoked by `reset-vol` command.             |
| `Stopped` -> `StartMaster` -> `Master` | Invoked by `start` command at master mode.  |
| `Master` -> `StopMaster` -> `Stopped`  | Invoked by `stop` command at master mode.   |

- All the transition pattern is `Static state` -> `dynamic state` -> `static state`.
- If commands failed on a volume, its state will be rollbacked to the previous static state.
- The commands are `walbc` commands.
- While almost `walbc` commands are asynchronous, python wrapper code will make them synchronous.
- `full-bkp` and `hash-bkp` command will invoke `start` command automatically.
- The state transition list shows requirements for several commands.
  Ex. `init-vol` command requires `Clear` state to run, `clear-vol` requires `SyncReady` and so on.
- If there seems to be no task for a volume while the state is dynamic, it must be BUG.


| Action       | Description                                      |
|:-------------|:-------------------------------------------------|
| `WlogSend`   | Transferring wlogs to a proxy (master mode).     |
| `WlogRemove` | Removing wlogs from the log device (slave mode). |

- `WlogSend` and `WlogRemove` actions are invoked by
  walb device monitor implemented in the storage server
  when wlog are generated at the corresponding walb devices.

| Command             | Required states                           |
|:--------------------|:------------------------------------------|
| `init-vol`          | `Clear` (trivial)                         |
| `clear-vol`         | `SyncReady` (trivial)                     |
| `reset-vol`         | `Stopped` (trivial)                       |
| `start master`      | `Stopped` (trivial)                       |
| `start slave`       | `SyncReady` (trivial)                     |
| `stop`              | `Master`, `Slave`                         |
| `full-bkp`          | `SyncReady` (trivial)                     |
| `hash-bkp`          | `SyncReady` (trivial)                     |
| `resize`            | `SyncReady`, `Stopped`, `Master`, `Slave` |
| `snapshot`          | `Master`, `Stopped`                       |
| `get is-overflow`   | not `Clear`                               |
| `get uuid`          | not `Clear`                               |
| `get *` (remaining) | Any                                       |
| `kick`              | Any                                       |
| `status`            | Any                                       |
| `exec`              | Any                                       |

| Action       | Required states                  |
|:-------------|:---------------------------------|
| `WlogSend`   | `Master`, `FullSync`, `HashSync` |
| `WlogRemove` | `Slave`                          |

See `src/storage_constants.hpp` for details.



## Proxy server

| Static state | Description                                                                                   |
|:-------------|:----------------------------------------------------------------------------------------------|
| `Clear`      | There is no metadata of the volume.                                                           |
| `Stopped`    | Neither wlog-transfer nor wdiff-transfer are available. `archive-info` command is available.  |
| `Started`    | Both wlog-transfer and wdiff-transfer are available. `archive-info` command will be rejected. |

| State transition | Description |
|:-----------------|:----------------|
| `Clear` -> `AddArchiveInfo` -> `Stopped`      | Adding the first archive info by `archive-info add` command. |
| `Stopped` -> `AddArchiveInfo` -> `Stopped`    | Adding an archive info by `archive-info add` command. |
| `Stopped` -> `DeleteArchiveInfo` -> `Stopped` | Deleting an archive info by `archive-info del` command. |
| `Stopped` -> `DeleteArchiveInfo` -> `Clear`   | Deleting the last archive info by `archive-info del` command. |
| `Stopped` -> `ClearVol` -> `Clear`            | Invoked by `clear-vol` command. |
| `Stopped` -> `Start` -> `Started`             | Invoked by `start` command. |
| `Started` -> `Stop` -> `Stopped`              | Invoked by `stop graceful` or `stop force` command. |
| `Started` -> `WaitForEmpty` -> `Stopped`      | Invoked by `stop empty` command. |
| `Started` -> `WlogRecv` -> `Started`          | Invoked by wlog-transfer protocol from a storage server. |

- In proxies, archive identifier are used for action name.
  Each action means wdiff-transfer to the corresponding archive server.

| Command or protocol       | Required states                         |
|:--------------------------|:----------------------------------------|
| `start`                   | `Stopped` (trivial)                     |
| `stop`                    | `Started`, `WlogRecv`                   |
| `archive-info add`        | `Clear`, `Stopped` (trivial)            |
| `archive-info del`        | `Stopped` (trivial)                     |
| `clear-vol`               | `Stopped` (trivial)                     |
| wlog transfer             | `Started` (trivial)                     |
| wdiff transfer            | `Started`, `WlogRecv`, `WaitForEmpty`   |
| `get is-wdiff-send-error` | not `Clear`                             |
| `get *` (remaining)       | Any                                     |
| `kick`                    | Any (ignored for not `Started` volumes) |
| `status`                  | Any                                     |
| `exec`                    | Any                                     |

See `src/proxy_constants.hpp` for details.



## Archive server

| Static state | Description                                  |
|:-------------|:---------------------------------------------|
| `Clear`      | There is no metadata and no base lv.         |
| `SyncReady`  | Ready to full/hash backup/replication.       |
| `Archived`   | Dirty or clean snapshots are available.      |
| `Stopped`    | Temporarily wdiff-transfer will be rejected. |

| State transition                               | Description                                      |
|:-----------------------------------------------|:-------------------------------------------------|
| `Clear` -> `InitVol` -> `SyncReady`            | Invoked by `init-vol` command.                   |
| `SyncReady` -> `ClearVol` -> `Clear`           | Invoked by `clear-vol` command.                  |
| `SyncReady` -> `FullSync` -> `Archived`        | Invoked by full backup/replication protocol.     |
| `Archived` -> `HashSync` -> `Archived`         | Invoked by hash backup protocol.                 |
| `Archived` -> `WdiffRecv` -> `Archived`        | Invoked by wdiff-transfer protocol.              |
| `Archived` -> `ReplSyncAsServer` -> `Archived` | Invoked by hash/diff replication protocol.       |
| `Archived` -> `Stop` -> `Stopped`              | Invoked by `stop` command.                       |
| `Stopped` -> `ClearVol` -> `Clear`             | Invoked by `clear-vol` command.                  |
| `Stopped` -> `Start` -> `Archived`             | Invoked by `start` command.                      |
| `Stopped` -> `ResetVol` -> `SyncReady`         | Invoked by `reset-vol` command.                  |

| Action             | Description                                                                                |
|:-------------------|:-------------------------------------------------------------------------------------------|
| `Merge`            | Merging wdiff files. Invoked by `merge` command.                                           |
| `Apply`            | Applying wdiff files to the base lv to remove older snapshots. Invoked by `apply` command. |
| `Restore`          | Restoring a clean snapshot. Invoked by `restore` command.                                  |
| `ReplSyncAsClient` | Replicating archive data to another archive server. Invoked by `replicate` command.        |
| `Resize`           | Resizing the volume. Invoked by `resize` command.                                          |

- At archive servers, all the actions are invoked by user-specified commands.

| Command or protocol     | Required states         | Required actions not running   |
|:------------------------|:------------------------|:-------------------------------|
| `init-vol`              | `Clear` (trivial)       | All |
| `clear-vol`             | `SyncReady`, `Stopped`  | All |
| `reset-vol`             | `Stopped` (trivial)     | All |
| `start`                 | `Stopped` (trivial)     | All |
| `stop`                  | aActive                 | None |
| `restore`               | aActive                 | `Restore`, `Resize`                                       |
| `del-restored`          | aActive                 | `Restore`, `Resize` (to serialize lvm operations)         |
| `replicate` (as client) | aActive                 | `Restore`, `ReplSyncAsClient`, `Apply`, `Merge`, `Resize` |
| `apply`                 | aActive                 | `Restore`, `ReplSyncAsClient`, `Apply`, `Resize`          |
| `merge`                 | aActive                 | `Restore`, `ReplSyncAsClient`, `Apply`, `Merge`, `Resize` |
| `resize`                | aActive, `Stopped`      | `Restore`, `ReplSyncAsClient`, `Apply`, `Resize`          |
| full backup             | `SyncReady` (trivial)   | All |
| hash backup             | `Archived` (trivial)    | All |
| wdiff transfer          | `Archived` (trivial)    | None |
| replicate as server     | `SyncReady`, `Archived` | All |
| `get restored`          | aActive                 | None |
| `get restorable`        | aActive                 | None |
| `get uuid`              | aActive                 | None |
| `get num-action`        | not `Clear`             | None |
| `get *` (remaining)     | Any                     | None |
| `status`                | Any                     | None |
| `exec`                  | Any                     | None |

- aActive is `Archived`, `HashSync`, `WdiffRecv`, `ReplSyncAsServer`.
- Ex. you can invoke `restore` command only when the state of the volume is in aActive, and
  not running `Restore` or `Resize`.

See `src/archive_constants.hpp` for details.

-----
