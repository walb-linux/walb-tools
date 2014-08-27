# Miscellaneous stuffs

There are remaining functions.
See help for details.

## Start/stop

- `walbc.start(s, vol)`
- `walbc.stop(s, vol, mode='graceful')`
- `walbc.stop_nbk(s, vol, mode='graceful')`

## Verify

- `walbc.verify_not_overflow(sx, vol)`
- `walbc.verify_not_restorable(ax, vol, gid, waitS, msg)`
- `walbc.verify_state(s, vol, state)`

## Wait

- `walbc.wait_for_replicated(ax, vol, gid, timeoutS=100)`
- `walbc.wait_for_restorable(ax, vol, gid, timeoutS=100)`
- `walbc.wait_for_restored(ax, vol, gid, timeoutS=100)`
- `walbc.wait_for_stopped(s, vol, prevSt=None)`

-----
