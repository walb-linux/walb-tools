import collections
import os
import threading
import subprocess
import sys
import time
import socket
import errno
import shutil

TIMEOUT_SEC = 100

K_STORAGE = 0
K_PROXY = 1
K_ARCHIVE = 2

Server = collections.namedtuple('Server', 'name port kind vg')
Config = collections.namedtuple(
    'Config', 'debug binDir dataDir storageL proxyL archiveL')
Wdev = collections.namedtuple('Wdev', 'iD path data log sizeMb')


cfg = None

# storage steady states
sClear = "Clear"
sSyncReady = "SyncReady"
sStopped = "Stopped"
sMaster = "Master"
sSlave = "Slave"

# storage temporary states
stInitVol = "InitVol"
stClearVol = "ClearVol"
stStartSlave = "StartSlave"
stStopSlave = "StopSlave"
stFullSync = "FullSync"
stHashSync = "HashSync"
stStartMaster = "StartMaster"
stStopMaster = "StopMaster"
stReset = "Reset"

# storage actions
saWlogSend = "WlogSend"
saWlogRemove = "WlogRemove"

# proxy steady states
pClear = "Clear"
pStopped = "Stopped"
pStarted = "Started"

# proxy temporary states
ptStart = "Start"
ptStop = "Stop"
ptClearVol = "ClearVol"
ptAddArchiveInfo = "AddArchiveInfo"
ptDeleteArchiveInfo = "DeleteArchiveInfo"
ptWlogRecv = "WlogRecv"
ptWaitForEmpty = "WaitForEmpty"

# archive steady states
aClear = "Clear"
aSyncReady = "SyncReady"
aArchived = "Archived"
aStopped = "Stopped"

# archive temporary states
atInitVol = "InitVol"
atClearVol = "ClearVol"
atResetVol = "ResetVol"
atFullSync = "FullSync"
atHashSync = "HashSync"
atWdiffRecv = "WdiffRecv"
atReplSync = "ReplSyncAsServer"
atStop = "Stop"
atStart = "Start"

# archive actions
aaMerge = "Merge"
aaApply = "Apply"
aaRestore = "Restore"
aaReplSync = "ReplSyncAsClient"
aaResize = "Resize"

sDuringFullSync = [stFullSync, sStopped, stStartMaster]
sDuringHashSync = [stHashSync, sStopped, stStartMaster]
sDuringStopForMaster = [sMaster, stStopMaster]
sDuringStopForSlave = [sSlave, stStopSlave]
pActive = [pStarted, ptWlogRecv]
pDuringStop = [pStarted, ptWlogRecv, ptStop, ptWaitForEmpty]
aActive = [aArchived, atWdiffRecv, atHashSync, atReplSync]
aAcceptForResize = aActive + [aStopped]
aAcceptForClearVol = [aStopped, aSyncReady]
aDuringReplicate = [atReplSync, atFullSync]
aDuringStop = aActive + [atStop]


import warning


def deprecated(func):
    '''
    decorator of deprecation.
    '''
    warning.warn('deprecated function %s.' % func.__name__,
                 category=warning.DeprecateionWarning,
                 stacklevel=2)

@deprecated
def set_config(config):
    if config.binDir[0] != '/' or config.binDir[-1] != '/':
        raise Exception("binDir must abs path", config.binDir)
    if config.dataDir[0] != '/' or config.dataDir[-1] != '/':
        raise Exception("dataDir must abs path", config.dataDir)
    global cfg
    cfg = config


def make_dir(pathStr):
    if not os.path.exists(pathStr):
        os.makedirs(pathStr)


def to_str(ss):
    return " ".join(ss)


def get_debug_opt():
    if cfg.debug:
        return ["-debug"]
    else:
        return []


def get_host_port(s):
    return "localhost" + ":" + s.port


def get_server_args(s):
    if s.kind == K_STORAGE:
        ret = [cfg.binDir + "storage-server",
               "-archive", get_host_port(cfg.archiveL[0]),
               "-proxy", ",".join(map(get_host_port, cfg.proxyL))]
    elif s.kind == K_PROXY:
        ret = [cfg.binDir + "proxy-server"]
    else:
        assert s.kind == K_ARCHIVE
        ret = [cfg.binDir + "archive-server", "-vg", s.vg]

    ret += ["-p", s.port,
            "-b", cfg.dataDir + s.name,
            "-l", cfg.dataDir + s.name + ".log",
            "-id", s.name] + get_debug_opt()
    return ret


def run_command(args, putMsg=True):
    '''
    run a command.
    args :: [str] - command line arguments.
    putMsg :: bool - put debug message.
    return :: str  - standard output of the command.
    '''
    if putMsg:
        print "run_command:", to_str(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                         stderr=sys.stderr, close_fds=True)
    f = p.stdout
    s = f.read().strip()
    ret = p.wait()
    if ret != 0:
        raise Exception("command error %d\n" % ret)
    if putMsg:
        print "run_command_result:", s
    return s


@deprecated
def run_ctl(server, cmdArgs, putMsg=True):
    ctlArgs = [cfg.binDir + "/controller",
            "-id", "ctrl",
            "-a", "localhost",
            "-p", server.port] + get_debug_opt()
    return run_command(ctlArgs + cmdArgs, putMsg)


def run_daemon(args):
    '''
    Run a daemon.
    args :: [str] - command line arguments for daemon.
    '''
    try:
        pid = os.fork()
        if pid > 0:
            # parent waits for child's suicide
            os.waitpid(pid, 0)
            return
    except OSError, e:
        print >>sys.stderr, "fork#1 failed (%d) (%s)" % (e.errno, e.strerror)
        raise

    # child
    os.chdir("/")
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            # child exits immediately
            os._exit(0)
    except OSError, e:
        print >>sys.stderr, "fork#2 failed (%d) (%s)" % (e.errno, e.strerror)
        os._exit(1)

    # grandchild
    sys.stdin = open('/dev/null', 'r')
    sys.stdout = open('/dev/null', 'w')
    sys.stderr = open('/dev/null', 'w')

    subprocess.Popen(args, close_fds=True).wait()
    os._exit(0)


def wait_for_server_port(s, timeoutS=10):
    '''
    Wait for server port accepts connections.
    s :: Server
    timeoutS :: int - timeout [sec].
    '''
    address = "localhost"
    port = int(s.port)
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        try:
            sock.connect((address, port))
            sock.close()
            return
        except socket.error, e:
            if e.errno not in [errno.ECONNREFUSED, errno.ECONNABORTED, errno.ECONNRESET]:
                raise
            print 'wait_for_server_port:ignored', s, e.errno, os.strerror(e.errno)
        time.sleep(0.3)
    raise Exception('wait_for_server_port:timeout', s)


@deprecated
def get_host_type(s, putMsg=True):
    return run_ctl(s, ['get', 'host-type'], putMsg)


@deprecated
def get_state(server, vol, putMsg=True):
    return run_ctl(server, ['get', 'state', vol], putMsg)


@deprecated
def wait_for_state_cond(server, vol, cond, msg, timeoutS=10):
    """
         cond: st -> bool
    """
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        st = get_state(server, vol)
#        print "c=", server, vol, stateL, c, st
        if cond(st):
            return
        time.sleep(0.3)
    raise Exception("wait_for_state_cond", server, vol, msg)


@deprecated
def wait_for_state(server, vol, stateL, timeoutS=10):
    def cond(st):
        return st in stateL
    wait_for_state_cond(server, vol, cond, 'stateL:' + str(stateL), timeoutS)


@deprecated
def wait_for_not_state(server, vol, stateL, timeoutS=10):
    def cond(st):
        return st not in stateL
    wait_for_state_cond(server, vol, cond, 'not stateL:' + str(stateL), timeoutS)


@deprecated
def wait_for_state_change(s, vol, tmpStateL, goalStateL, timeoutS=10):
    wait_for_not_state(s, vol, tmpStateL, timeoutS)
    st = get_state(s, vol)
    if st not in goalStateL:
        raise Exception('wait_for_state_change:bad goal', s, vol, tmpStateL, goalStateL, st)

@deprecated
def reset_vol(s, vol):
    """
    s :: Server - storage or archive.
    vol :: str  - volume name.
    """
    run_ctl(s, ["reset-vol", vol])
    if s.kind == K_STORAGE:
        wait_for_state(s, vol, [sSyncReady])
    elif s.kind == K_ARCHIVE:
        wait_for_state(s, vol, [aSyncReady])
    else:
        raise Exception('reset_vol:bad server', s)


@deprecated
def set_slave_storage(sx, vol):
    state = get_state(sx, vol)
    if state == sSlave:
        return
    if state == sSyncReady:
        start(sx, vol)
        return
    if state == sMaster:
        stop(sx, vol)
    else:
        raise Exception('set_slave_storage:bad state', state)
    stop_sync(cfg.archiveL[0], vol)
    reset_vol(sx, vol)
    start(sx, vol)


@deprecated
def kick_all(sL):
    """
        sL: list of s. s must be storage or proxy.
    """
    for s in sL:
        run_ctl(s, ["kick"])


@deprecated
def kick_all_storage():
    kick_all(cfg.storageL)


@deprecated
def run_walbctl(cmdArgs):
    run_command([cfg.binDir + 'walbctl'] + cmdArgs)


@deprecated
def create_walb_dev(ldevPath, ddevPath, wdevId):
    run_walbctl(['format_ldev',
                 '--ldev', ldevPath,
                 '--ddev', ddevPath])
    run_walbctl(['create_wdev',
                 '--ldev', ldevPath,
                 '--ddev', ddevPath,
                 '--name', str(wdevId)])


@deprecated
def delete_walb_dev(wdevPath):
    run_walbctl(['delete_wdev', '--wdev', wdevPath])


@deprecated
def is_overflow(sx, vol):
    return int(run_ctl(sx, ['get', 'is-overflow', vol])) != 0


@deprecated
def verify_not_overflow(sx, vol):
    if is_overflow(sx, vol):
        raise Exception('verify_not_overflow', sx, vol)


@deprecated
def is_wdiff_send_error(px, vol, ax):
    return int(run_ctl(px, ['get', 'is-wdiff-send-error', vol, ax.name])) != 0


@deprecated
def reset_wdev(wdev):
    if os.path.exists(wdev.path):
        delete_walb_dev(wdev.path)
    resize_lv(wdev.data, get_lv_size_mb(wdev.data), wdev.sizeMb, False)
    create_walb_dev(wdev.log, wdev.data, wdev.iD)


@deprecated
def cleanup(vol, wdevL):
    for s in cfg.storageL + cfg.proxyL + cfg.archiveL:
        clear_vol(s, vol)
    for wdev in wdevL:
        reset_wdev(wdev)


##################################################################
# user command functions


@deprecated
def status(sL=[], vol=None):
    '''
        print server status.
    '''
    if not sL:
        sL = cfg.storageL + cfg.proxyL + cfg.archiveL
    for s in sL:
        args = ['status']
        if vol:
            args.append(vol)
        print '++++++++++++++++++++', s.name, '++++++++++++++++++++'
        print run_ctl(s, args, False)


def kill_all_servers():
    '''
    for scenario test.
    '''
    for s in ["storage-server", "proxy-server", "archive-server"]:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()
    time.sleep(0.3)


def remove_persistent_data(s):
    '''
    for scenario test.
    call shutdown() before calling this.
    '''
    shutil.rmtree(cfg.dataDir + s.name)
    if s in cfg.archiveL:
        for f in os.listdir('/dev/' + s.vg):
            if f[0:2] == 'i_':
                remove_lv('/dev/' + s.vg + '/' + f)


def startup(s):
    '''
    for scenario test.
    '''
    make_dir(cfg.dataDir + s.name)
    args = get_server_args(s)
    if cfg.debug:
        print 'cmd=', to_str(args)
    run_daemon(args)
    wait_for_server_port(s)


def startup_all():
    '''
    for scenario test.
    '''
    for s in cfg.archiveL + cfg.proxyL + cfg.storageL:
        startup(s)


def _verify_shutdown_mode(mode, msg):
    if mode not in ['graceful', 'force']:
        raise Exception(msg, 'bad mode', mode)


def _verify_stop_mode(mode, msg):
    if mode not in ['graceful', 'force', 'empty']:
        raise Exception(msg, 'bad mode', mode)


@deprecated
def shutdown(s, mode="graceful"):
    '''
    Shutdown a server.
    s :: Server
    mode :: str - 'graceful' or 'force'.
    '''
    verify_type(s, Server)
    verify_type(mode, str)
    _verify_shutdown_mode(mode, 'shutdown')
    run_ctl(s, ["shutdown", mode])
    time.sleep(1)  # shutdown is asynchronous command.


@deprecated
def shutdown_all(mode='graceful'):
    '''
    Shutdown all servers.
    '''
    _verify_shutdown_mode(mode, 'shutdown_all')
    for s in cfg.storageL + cfg.proxyL + cfg.archiveL:
        run_ctl(s, ["shutdown", mode])
    time.sleep(1)


@deprecated
def get_alive_server():
    '''
    Get alive servers.
    return :: [str] - list of server name.
    '''
    ret = []
    for s in cfg.storageL + cfg.proxyL + cfg.archiveL:
        try:
            get_host_type(s, False)
            ret.append(s.name)
        except:
            pass
    return ret


@deprecated
def init(sx, vol, wdevPath):
    '''
    sx :: Server
    vol :: str
    wdevPath :: str
    '''
    run_ctl(sx, ["init-vol", vol, wdevPath])
    start(sx, vol)
    a0 = cfg.archiveL[0]
    if get_state(a0, vol) == aClear:
        run_ctl(a0, ["init-vol", vol])


@deprecated
def clear_vol(s, vol):
    '''
    Clear a volume.
    s :: Server
    vol :: str
    '''
    st = get_state(s, vol)
    if s.kind == K_STORAGE:
        if st == sClear:
            return
        if st in [sMaster, sSlave]:
            stop(s, vol)
            st = get_state(s, vol)
        if st == sStopped:
            reset_vol(s, vol)
            st = get_state(s, vol)
        if st != sSyncReady:
            raise Exception('clear_vol', s, vol, st)
    elif s.kind == K_PROXY:
        if st == pClear:
            return
        if st in pActive:
            stop(s, vol)
            st = get_state(s, vol)
        if st != pStopped:
            raise Exception('clear_vol', s, vol, st)
    else:
        assert s.kind == K_ARCHIVE
        if st == aClear:
            return
        if st == aArchived:
            stop(s, vol)
            st = get_state(s, vol)
        if st not in aAcceptForClearVol:
            raise Exception('clear_vol', s, vol, st)
    run_ctl(s, ['clear-vol', vol])


@deprecated
def get_archive_info_list(px, vol):
    '''
    Get archive list registered to a proxy.
    return :: [str] - list of archive name.
    '''
    st = run_ctl(px, ["archive-info", "list", vol])
    return st.split()


@deprecated
def is_synchronizing(ax, vol):
    '''
    Check whether a volume is synchronizing with an archive server or not.
    ax :: Server - archive server.
    vol :: str
    return :: bool
    '''
    ret = []
    for px in cfg.proxyL:
        ret.append(ax.name in get_archive_info_list(px, vol))
    v = sum(ret)
    if v == len(ret): # all True
        return True
    elif v == 0: # all False
        return False
    raise Exception('is_synchronizing: some are synchronizing, some are not.')


@deprecated
def wait_for_stopped(s, vol, prevSt=None):
    '''
    s :: Server
    vol :: str
    prevSt :: None or str - specify if s is storage server.
    '''
    if s.kind == K_STORAGE:
        if not prevSt:
            raise Exception('wait_for_stopped: prevSt not specified', s, vol)
        if prevSt == sSlave:
            tmpStL = sDuringStopForSlave
            goalSt = sSyncReady
        else:
            tmpStL = sDuringStopForMaster
            goalSt = sStopped
    elif s.kind == K_PROXY:
        tmpStL = pDuringStop
        goalSt = pStopped
    else:
        assert s.kind == K_ARCHIVE
        tmpStL = aDuringStop
        goalSt = aStopped
    wait_for_state_change(s, vol, tmpStL, [goalSt], TIMEOUT_SEC)


@deprecated
def stop(s, vol, mode="graceful"):
    '''
    Stop a volume at a server and wait for it stopped.
    s :: Server
    vol :: str
    mode :: str - 'graceful' or 'force' or 'empty'.
        'empty' is valid only if s is proxy.
    '''
    if mode not in ['graceful', 'empty', 'force']:
        raise Exception('stop:bad mode', mode)
    prevSt = get_state(s, vol)
    run_ctl(s, ["stop", vol, mode])
    wait_for_stopped(s, vol, prevSt)


@deprecated
def start(s, vol):
    '''
    Start a volume at a server and wait for it started.
    s :: Server
    vol :: str

    '''
    if s.kind == K_STORAGE:
        st = get_state(s, vol)
        if st == sSyncReady:
            run_ctl(s, ['start', vol, 'slave'])
            wait_for_state_change(s, vol, [stStartSlave], [sSlave])
        else:
            assert st == sStopped
            run_ctl(s, ['start', vol, 'master'])
            wait_for_state_change(s, vol, [stStartMaster], [sMaster])
    elif s.kind == K_PROXY:
        run_ctl(s, ['start', vol])
        wait_for_state_change(s, vol, [ptStart], pActive)
    else:
        assert s.kind == K_ARCHIVE
        run_ctl(s, ['start', vol])
        wait_for_state_change(s, vol, [atStart], aActive)


@deprecated
def del_archive_from_proxy(px, vol, ax):
    '''
    Delete an archive from a proxy.
    px :: Server - proxy server.
    vol :: str   - voume name.
    ax :: Server - archive server.
    '''
    st = get_state(px, vol)
    if st in pActive:
        stop(px, vol)
    aL = get_archive_info_list(px, vol)
    if ax.name in aL:
        run_ctl(px, ['archive-info', 'delete', vol, ax.name])
    st = get_state(px, vol)
    if st == pStopped:
        start(px, vol)


@deprecated
def add_archive_to_proxy(px, vol, ax, doStart=True):
    '''
    Add an archive to a proxy.
    px :: Server    - proxy server.
    vol :: str      - volume name.
    ax :: server    - archive server.
    doStart :: bool - False if not to start proxy after adding.
    '''
    st = get_state(px, vol)
    if st in pActive:
        stop(px, vol)
    aL = get_archive_info_list(px, vol)
    if ax.name not in aL:
        run_ctl(px, ['archive-info', 'add', vol, ax.name, get_host_port(ax)])
    st = get_state(px, vol)
    if st == pStopped and doStart:
        start(px, vol)


@deprecated
def get_server(name, L):
    '''
    Get only one element from L having name
    name :: str   - server name.
    L :: [Server] - server list.
    '''
    ret = []
    for x in L:
        if x.name == name:
            ret.append(x)
    if len(ret) != 1:
        raise Exception('get_server:not one', ret, name, L)
    return ret[0]


@deprecated
def copy_archive_info(pSrc, vol, pDst):
    '''
    Copy archive info from a proxy to another proxy.
    pSrc :: Server - srouce proxy.
    vol :: str
    pDst :: Server - destination proxy. It must be stopped.
    '''
    for axName in get_archive_info_list(pSrc, vol):
        ax = get_server(axName, cfg.archiveL)
        add_archive_to_proxy(pDst, vol, ax, doStart=False)
    start(pDst, vol)


@deprecated
def stop_sync(ax, vol):
    '''
    Stop synchronization of a volume with an archive.
    ax :: Server - archive server.
    vol :: str   - volume name.
    '''
    for px in cfg.proxyL:
        del_archive_from_proxy(px, vol, ax)
    kick_all_storage()


@deprecated
def start_sync(ax, vol):
    '''
    Start synchronization of a volume with an archive.
    ax :: Server - archive server.
    vol :: str   - volume name.
    '''
    for px in cfg.proxyL:
        add_archive_to_proxy(px, vol, ax)
    kick_all_storage()


@deprecated
def get_gid_list(ax, vol, cmd, optL=[]):
    '''
    Get gid list
    ax :: Server  - archive server.
    vol :: str    - volume name.
    cmd :: str    - 'restorable' or 'restored'.
    optL :: [str] - options.
    '''
    if not cmd in ['restorable', 'restored']:
        raise Exception('get_list_gid : bad cmd', cmd)
    ret = run_ctl(ax, ['get', cmd, vol] + optL)
    return map(int, ret.split())


@deprecated
def list_restorable(ax, vol, opt=''):
    '''
    Get restorable gid list.
    ax :: Server - archive server.
    vol :: str   - volume name.
    opt :: str   - you can specify 'all'.
    '''
    optL = []
    if opt:
        if opt == 'all':
            optL.append(opt)
        else:
            raise Exception('list_restorable:bad opt', opt)
    return get_gid_list(ax, vol, 'restorable', optL)


@deprecated
def list_restored(ax, vol):
    '''
    Get restored gid list.
    ax :: Server - archive server.
    vol :: str   - volume name.
    '''
    return get_gid_list(ax, vol, 'restored')


@deprecated
def wait_for_restorable_any(ax, vol, timeoutS=TIMEOUT_SEC):
    '''
    ax :: Server    - archive server.
    vol :: str      - volume name.
    timeoutS :: int - timeout [sec].
    '''
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = list_restorable(ax, vol)
        if gids:
            return gids[-1]
        time.sleep(0.3)
    return -1


@deprecated
def wait_for_gid(ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
    '''
    Wait for an gid is available.
    ax :: Server    - archive server.
    vol :: str      - volume name.
    cmd :: str      - 'restorable' or 'restored'.
    timeoutS :: int - timeout [sec].
    '''
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, cmd)
        if gid in gids:
            return
        time.sleep(0.3)
    raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)


@deprecated
def wait_for_not_gid(ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
    '''
    Wait for a gid is not available.
    ax :: Server    - archive server.
    vol :: str      - volume name.
    cmd :: str      - 'restorable' or 'restored'.
    timeoutS :: int - timeout [sec].
    '''
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, cmd)
        if gid not in gids:
            return
        time.sleep(0.3)
    raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)


@deprecated
def wait_for_restorable(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    '''
    Wait for a snapshot specified of a gid to be restorable.
    ax :: Server    - archive server.
    vol :: str      - volume name.
    gid :: int      - generation id.
    timeoutS :: int - timeout [sec].
    '''
    wait_for_gid(ax, vol, gid, 'restorable', timeoutS)


@deprecated
def wait_for_restored(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    '''
    Wait for a snapshot specified of a gid to be restored.
    ax :: Server    - archive server.
    vol :: str      - volume name.
    gid :: int      - generation id.
    timeoutS :: int - timeout [sec].
    '''
    wait_for_no_action(ax, vol, 'Restore', timeoutS)
    gids = list_restored(ax, vol)
    if gid in gids:
        return
    raise Exception('wait_for_restored:failed', ax.name, vol, gid, gids)


@deprecated
def wait_for_not_restored(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    '''
    Wait for a restored snapshot specified of a gid to be removed
    ax :: Server    - archive server.
    vol :: str      - volume name.
    gid :: int      - generation id.
    timeoutS :: int - timeout [sec].
    '''
    wait_for_not_gid(ax, vol, gid, 'restored', timeoutS)


@deprecated
def wait_for_applied(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    '''
    Wait for diffs older than a gid to be applied.
    ax :: Server    - archive server.
    vol :: str      - volume name.
    gid :: int      - generation id.
    timeoutS :: int - timeout [sec].
    '''
    wait_for_no_action(ax, vol, aaApply, timeoutS)
    gidL = list_restorable(ax, vol)
    if gidL and gid <= gidL[0]:
        return
    raise Exception('wait_for_applied:failed', ax.name, vol, gid, gidL)


@deprecated
def wait_for_merged(ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
    '''
    Wait for diffs in a gid range to be merged.
    ax :: Server    - archive server.
    vol :: str      - volume name.
    gidB :: int     - begin generation id.
    gidE :: int     - end generation id.
    timeoutS :: int - timeout [sec].
    '''
    wait_for_no_action(ax, vol, aaMerge, timeoutS)
    gidL = list_restorable(ax, vol, 'all')
    pos = gidL.index(gidB)
    if gidL[pos + 1] == gidE:
        return
    raise Exception("wait_for_merged:failed", ax.name, vol, gidB, gidE, pos, gidL)


@deprecated
def wait_for_replicated(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    '''
    Wait for a snapshot is restorable at an archive server.
    ax :: Server    - archive server as a replication server (not client).
    vol :: str      - volume name.
    gid :: int      - generation id.
    timeoutS :: int - timeout [sec].
    '''
    wait_for_not_state(ax, vol, aDuringReplicate, timeoutS)
    gidL = list_restorable(ax, vol, 'all')
    if gidL and gid <= gidL[-1]:
        return
    raise Exception("wait_for_replicated:replicate failed", ax.name, vol, gid, gidL)


@deprecated
def verify_not_restorable(ax, vol, gid, waitS, msg):
    '''
    Verify a snapshot does not become restorable
    at an archive server in a period.
    ax :: Server    - archive server.
    vol :: str      - volume name.
    gid :: int      - generation id.
    waitS :: int    - wait period [sec].
    msg :: str      - message for error.
    '''
    e = False
    try:
        wait_for_restorable(ax, vol, gid, waitS)
        e = True
    except:
        # expect to fail due to timeout.
        pass
    if e:
        raise Exception(msg, 'gid must not be restorable', gid)


@deprecated
def replicate_sync(aSrc, vol, aDst):
    '''
    Copy current (aSrc, vol) to aDst.
    aSrc :: Server - source archive (as a client).
    vol :: str     - volume name.
    aDst :: Server - destination archive (as a server).
    '''
    gidL = list_restorable(aSrc, vol)
    gid = gidL[-1]
    run_ctl(aSrc, ['replicate', vol, "gid", str(gid), get_host_port(aDst)])
    wait_for_replicated(aDst, vol, gid)


@deprecated
def synchronize(aSrc, vol, aDst):
    '''
    Synchronize aDst with (aSrc, vol).
    To reduce proxies stopped period, replicate nosync before calling this.
    aSrc :: Server - source archive (as a client).
    vol :: str     - volume name.
    aDst :: Server - destination archive (as a server).
    '''
    for px in cfg.proxyL:
        st = get_state(px, vol)
        if st in pActive:
            run_ctl(px, ["stop", vol, 'empty'])

    for px in cfg.proxyL:
        wait_for_stopped(px, vol)
        aL = get_archive_info_list(px, vol)
        if aDst.name not in aL:
            run_ctl(px, ["archive-info", "add", vol,
                         aDst.name, get_host_port(aDst)])

    replicate_sync(aSrc, vol, aDst)

    for px in cfg.proxyL:
        start(px, vol)
    kick_all_storage()


@deprecated
def prepare_backup(sx, vol):
    '''
    Prepare backup.
    sx :: Server - storage server.
    vol :: str   - volume name.
    '''
    a0 = cfg.archiveL[0]
    st = get_state(sx, vol)
    if st == sSlave:
        stop(sx, vol)
    elif st == sMaster:
        stop(sx, vol)
        reset_vol(sx, vol)

    for s in cfg.storageL:
        if s == sx:
            continue
        st = get_state(s, vol)
        if st not in [sSlave, sClear]:
            raise Exception("prepare_backup:bad state", s.name, vol, st)

    for ax in cfg.archiveL:
        if is_synchronizing(ax, vol):
            stop_sync(ax, vol)

    start_sync(a0, vol)


@deprecated
def full_backup(sx, vol, timeoutS=TIMEOUT_SEC):
    '''
    Run full backup a volume of a storage server.
    Log transfer to the primary archive server will start automatically.
    This function will return when a clean snapshot is
    sx :: Server    - storage server.
    vol :: str      - volume name.
    timeoutS :: int - timeout [sec].
                      Counter will start after dirty full backup done.
    return :: int   - generation id of a clean snapshot.
    '''
    a0 = cfg.archiveL[0]
    prepare_backup(sx, vol)
    run_ctl(sx, ["full-bkp", vol])
    wait_for_state_change(sx, vol, sDuringFullSync, [sMaster], timeoutS)
    st = get_state(a0, vol)
    if st not in aActive:
        raise Exception('full_backup: sync failed', sx, a0, vol, st)

    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = list_restorable(a0, vol)
        if gids:
            return gids[-1]
        time.sleep(0.3)
    raise Exception('full_backup:timeout', sx, vol, gids)


@deprecated
def hash_backup(sx, vol, timeoutS=TIMEOUT_SEC):
    '''
    Run hash backup a volume of a storage server.
    Log transfer to the primary archive server will start automatically.
    This function will return a gid of a clean snapshot that is
    restorable at the primary archive server.

    sx :: Server    - storage server.
    vol :: str      - volume name.
    timeoutS :: int - timeout [sec].
                      Counter will start after dirty hash backup done.
    return :: int   - generation id of a clean snapshot.
    '''
    a0 = cfg.archiveL[0]
    prepare_backup(sx, vol)
    prev_gids = list_restorable(a0, vol)
    if prev_gids:
        max_gid = prev_gids[-1]
    else:
        max_gid = -1
    run_ctl(sx, ["hash-bkp", vol])
    wait_for_state_change(sx, vol, sDuringHashSync, [sMaster], timeoutS)
    st = get_state(a0, vol)
    if st not in aActive:
        raise Exception('hash_backup: sync failed', sx, a0, vol, st)

    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = list_restorable(a0, vol)
        if gids and gids[-1] > max_gid:
            return gids[-1]
        time.sleep(0.3)
    raise Exception('hash_backup:timeout', sx, vol, max_gid, gids)


def write_random(bdevPath, sizeLb, offset=0, fixVar=None):
    '''
    for scenario test.
    '''
    args = [cfg.binDir + "/write_random_data",
        '-s', str(sizeLb), '-o', str(offset), bdevPath]
    if fixVar:
        args += ['-set', str(fixVar)]
    return run_command(args, False)


def get_sha1(bdevPath):
    '''
    Get sha1sum of a block device by full scan.
    bdevPath :: str - block device path.
    return :: str  - sha1sum string.
    '''
    verify_type(bdevPath, str)
    ret = run_command(['/usr/bin/sha1sum', bdevPath])
    return ret.split(' ')[0]


def flush_bufs(bdevPath):
    '''
    Flush buffer of a block device.
    bdevPath :: str - block device path.
    '''
    verify_type(bdevPath, str)
    run_command(['/sbin/blockdev', '--flushbufs', bdevPath])


def wait_for_lv_ready(lvPath, timeoutS=TIMEOUT_SEC):
    '''
    lvPath :: str   - lvm device path.
    '''
    flush_bufs(lvPath)


@deprecated
def get_restored_path(ax, vol, gid):
    '''
    ax :: Server  - archive server.
    vol :: str    - volume name.
    gid :: int    - generation id.
    return :: str - restored path.
    '''
    return '/dev/' + ax.vg + '/r_' + vol + '_' + str(gid)


@deprecated
def get_lv_path(ax, vol):
    '''
    ax :: Server  - archive server.
    vol :: str    - volume name.
    return :: str - lv path.
    '''
    return '/dev/' + ax.vg + '/i_' + vol


@deprecated
def restore(ax, vol, gid):
    '''
    ax :: Server  - archive server.
    vol :: str    - volume name.
    gid :: int    - generation id.
    '''
    run_ctl(ax, ['restore', vol, str(gid)])
    wait_for_restored(ax, vol, gid)
    wait_for_lv_ready(get_restored_path(ax, vol, gid))


@deprecated
def del_restored(ax, vol, gid):
    '''
    ax :: Server  - archive server.
    vol :: str    - volume name.
    gid :: int    - generation id.
    '''
    wait_for_lv_ready(get_lv_path(ax, vol))
    retryTimes = 3
    for i in xrange(retryTimes):
        try:
            run_ctl(ax, ['del-restored', vol, str(gid)])
            break
        except Exception, e:
            print 'del-restored retry', i, e
            time.sleep(1)
    else:
        raise Exception('del-restored: exceeds max retry times')
    wait_for_not_restored(ax, vol, gid)


@deprecated
def snapshot_async(sx, vol):
    '''
    Take a snaphsot.
    sx :: Server - storage server.
    vol :: str   - volume name.
    '''
    gid = run_ctl(sx, ['snapshot', vol])
    return int(gid)


@deprecated
def snapshot_sync(sx, vol, axs):
    '''
    Take a snaphsot and wait for it to be restorable in archive servers.
    sx :: Server - storage server.
    vol :: str   - volume name.
    '''
    gid = snapshot_async(sx, vol)
    for ax in axs:
        wait_for_restorable(ax, vol, gid)
    return gid


def verify_equal_sha1(msg, md0, md1):
    '''
    Verify two sha1sum equals.
    msg :: str - message for error.
    md0 :: str - sha1sum
    md1 :: str - sha1sum
    '''
    if md0 == md1:
        print msg + ' ok :', md0
    else:
        raise Exception('fail ' + msg, md0, md1)


@deprecated
def restore_and_verify_sha1(msg, md0, ax, vol, gid):
    '''
    Restore a volume and verify sha1sum.
    msg :: str   - message for error.
    md0 :: str   - sha1sum.
    ax :: Server - archive server.
    vol :: str   - volume name.
    gid :: int   - generation id.
    '''
    md1 = get_sha1_of_restorable(ax, vol, gid)
    verify_equal_sha1(msg, md0, md1)


@deprecated
def get_sha1_of_restorable(ax, vol, gid):
    '''
    Get sha1sum of restorable snapshot.
    ax :: Server - archive server.
    vol :: str   - volume name.
    gid :: int   - generation id.
    '''
    restore(ax, vol, gid)
    md = get_sha1(get_restored_path(ax, vol, gid))
    del_restored(ax, vol, gid)
    return md


@deprecated
def verify_equal_list_restorable(msg, ax, ay, vol):
    '''
    msg :: str   - message for error.
    ax :: Server - first archive server.
    ay :: Server - second archive server.
    vol :: str   - volume name.
    '''
    xL = list_restorable(ax, vol)
    yL = list_restorable(ay, vol)
    if cfg.debug:
        print 'list0', xL
        print 'list1', yL
    if xL != yL:
        raise Exception(msg, 'list_restorable differ', xL, yL)


@deprecated
def get_latest_clean_snapshot(ax, vol):
    '''
    ax :: Server - archive server.
    vol :: str   - volume name.
    '''
    xL = list_restorable(ax, vol)
    if xL:
        return xL[-1]
    else:
        raise Exception('get_latest_clean_snapshot:not found')


class RandomWriter():
    def __init__(self, path):
        self.path = path
    def writing(self):
        while not self.quit:
            write_random(self.path, 1)
            time.sleep(0.05)
    def __enter__(self):
        self.quit = False
        self.th = threading.Thread(target=self.writing)
        self.th.setDaemon(True)
        self.th.start()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.quit = True
        self.th.join()


@deprecated
def apply_diff(ax, vol, gid):
    '''
    Apply diffs older than a gid the base lv.
    ax :: Server - archive server
    vol :: str   - volume name.
    gid :: int   - generation id.
    '''
    run_ctl(ax, ["apply", vol, str(gid)])
    wait_for_applied(ax, vol, gid)


@deprecated
def merge_diff(ax, vol, gidB, gidE):
    '''
    Merge diffs in gid ranges.
    ax :: Server - archive server.
    vol :: str   - volume name.
    gidB :: int  - begin gid.
    gidE :: int  - end gid.
    '''
    run_ctl(ax, ["merge", vol, str(gidB), "gid", str(gidE)])
    wait_for_merged(ax, vol, gidB, gidE)


@deprecated
def replicate(aSrc, vol, aDst, synchronizing):
    '''
    Replicate archive data by copying a volume from one archive to another.
    aSrc :: Server        - source archive server (as client).
    vol :: str            - volume name.
    aDst :: Server        - destination archive server (as server).
    synchronizing :: bool - True if you want to make aDst synchronizing.
    '''
    st = get_state(aDst, vol)
    if st == aClear:
        run_ctl(aDst, ["init-vol", vol])

    replicate_sync(aSrc, vol, aDst)
    if synchronizing:
        synchronize(aSrc, vol, aDst)


@deprecated
def wait_for_no_action(s, vol, action, timeoutS=TIMEOUT_SEC):
    '''
    s :: Server     - server.
    vol :: str      - volume name.
    action :: str   - action name.
    timeoutS :: int - timeout [sec].
    '''
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        num = int(run_ctl(s, ['get', 'num-action', vol, action]))
        if num == 0:
            return
        time.sleep(0.3)
    raise Exception("wait_for_no_action", s, vol, action)


def zero_clear(bdevPath, offsetLb, sizeLb):
    '''
    Zero-clear a block device.
    bdevPath :: str - block device path.
    offsetLb :: int - offset [logical block].
    sizeLb :: innt  - size [logical block].
    '''
    run_command(['/bin/dd', 'if=/dev/zero', 'of=' + bdevPath, # 'oflag=direct',
                 'bs=512', 'seek=' + str(offsetLb), 'count=' + str(sizeLb)])
    # flush data to avoid overwriting after walb's writing
    fd = os.open(bdevPath, os.O_RDONLY)
    os.fdatasync(fd)
    os.close(fd)


def resize_lv(lvPath, beforeSizeMb, afterSizeMb, doZeroClear):
    """
    Resize a logical volume.
      This command support shrink also.
    lvPath :: str       - lvm lv path.
    beforeSizeMb :: int - current device size [MiB].
    afterSizeMb :: int  - new device size [MiB].
    doZeroClear :: bool - True to zero-clear the extended area.
    """
    if beforeSizeMb == afterSizeMb:
        return
    run_command(['/sbin/lvresize', '-f', '-L', str(afterSizeMb) + 'm', lvPath])
    # zero-clear is required for test only.
    if beforeSizeMb < afterSizeMb and doZeroClear:
        zero_clear(lvPath, beforeSizeMb * 1024 * 1024 / 512,
                   (afterSizeMb - beforeSizeMb) * 1024 * 1024 / 512)
    wait_for_lv_ready(lvPath)


def remove_lv(lvPath):
    '''
    Remove a logical volume.
    lvPath :: str - lvm lv path.
    '''
    wait_for_lv_ready(lvPath)
    for i in xrange(3):
        try:
            run_command(['/sbin/lvremove', '-f', lvPath])
            return
        except:
            print 'remove_lv failed', i, lvPath
            time.sleep(1)
    else:
        raise Exception('remove_lv:timeout', lvPath)


def get_lv_size_mb(lvPath):
    '''
    Get lv size.
    lvPath :: str - lvm lv path.
    return :: device size [MiB].
    '''
    ret = run_command(['/sbin/lvdisplay', '-C', '--noheadings',
                       '-o', 'lv_size', '--units', 'b', lvPath])
    ret.strip()
    if ret[-1] != 'B':
        raise Exception('get_lv_size_mb: bad return value', ret)
    return int(ret[0:-1]) / 1024 / 1024


@deprecated
def wait_for_resize(ax, vol, sizeMb):
    '''
    Wait for resize done.
    ax :: Server  - archive server.
    vol :: str    - volume name.
    sizeMb :: int - new size [MiB].
    '''
    wait_for_no_action(ax, vol, aaResize)
    curSizeMb = get_lv_size_mb(get_lv_path(ax, vol))
    if curSizeMb != sizeMb:
        raise Exception('wait_for_resize:failed', ax, vol, sizeMb, curSizeMb)


@deprecated
def resize_archive(ax, vol, sizeMb, doZeroClear):
    '''
    Resize archive volume.
    ax :: Server        - archive server.
    vol :: str          - volume name.
    sizeMb :: int       - new size [MiB].
    doZeroClear :: bool - True if zero-clear extended area.
    '''
    st = get_state(ax, vol)
    if st == aClear:
        return
    elif st in aAcceptForResize:
        args = ['resize', vol, str(sizeMb) + 'm']
        if doZeroClear:
            args += ['zeroclear']
        run_ctl(ax, args)
        wait_for_resize(ax, vol, sizeMb)
    else:
        raise Exception('resize_archive:bad state', ax, vol, sizeMb, st)


@deprecated
def resize_storage(sx, vol, sizeMb):
    '''
    Resize storage volume.
    sx :: Server  - storage server.
    vol :: str    - voume name.
    sizeMb :: int - new size [MiB].
    '''
    st = get_state(sx, vol)
    if st == sClear:
        return
    else:
        run_ctl(sx, ['resize', vol, str(sizeMb) + 'm'])


@deprecated
def resize(vol, sizeMb, doZeroClear):
    '''
    Resize volume.
    vol :: str          - volume name.
    sizeMb :: int       - new size [MiB].
    doZeroClear :: bool - True if you want to zero-clear the extended area.
    '''
    for ax in cfg.archiveL:
        resize_archive(ax, vol, sizeMb, doZeroClear)
    for sx in cfg.storageL:
        resize_storage(sx, vol, sizeMb)


@deprecated
def wait_for_log_empty(wdev, timeoutS=TIMEOUT_SEC):
    '''
    wdev :: Wdev    - walb device.
    timeoutS :: int - timeout [sec].
    '''
    def create_key_value(ls):
        ret = []
        for s in ls:
            (k, v) = s.strip().split()
            ret.append((k, int(v)))
        return dict(ret)
    sysName = '/sys/block/walb!%d/walb/lsids' % wdev.iD
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        with open(sysName, 'r') as f:
            kv = create_key_value(f.readlines())
            completed = kv['completed']
            oldest = kv['oldest']
            if completed == oldest:
                return
        print "wait_for_log_empty", oldest, completed
        time.sleep(0.3)
    raise Exception("wait_for_log_empty", wdev)


@deprecated
def get_walb_dev_sizeMb(wdev):
    '''
    Get walb device size.
    wdev :: Wdev -- walb device.
    return :: int -- device size [MiB].
    '''
    sysName = '/sys/block/walb!%d/size' % wdev.iD
    with open(sysName, 'r') as f:
        size = int(f.read().strip()) * 512 / 1024 / 1024
    return size


def write_over_wldev(wdev, overflow=False):
    '''
    Write to a walb device with size larger then log device.
    wdev :: Wdev     - walb device.
    overflow :: bool - True if you want to overflow the device.
    '''
    wldevSizeLb = get_lv_size_mb(wdev.log) * 1024 * 1024 / 512
    wdevSizeLb = get_walb_dev_sizeMb(wdev) * 1024 * 1024 / 512
    print "wldevSizeLb, wdevSizeLb", wldevSizeLb, wdevSizeLb
    # write a little bigger size than wldevSizeLb
    remainLb = wldevSizeLb + 4
    writeMaxLb = min(wldevSizeLb, wdevSizeLb) / 2
    while remainLb > 0:
        writeLb = min(remainLb, writeMaxLb)
        print 'writing %d MiB' % (writeLb * 512 / 1024 / 1024)
        write_random(wdev.path, writeLb)
        if not overflow:
            wait_for_log_empty(wdev)
        remainLb -= writeLb


def verify_type(obj, typeValue):
    '''
    obj - object.
    typeValue - type like int, str, list.

    '''
    if type(obj) != typeValue:
        raise 'invalid type', type(obj), typeValue


def verify_list_type(obj, typeValue):
    '''
    obj - list object.
    typeValue - type like int, str.
    '''
    if type(obj) != list:
        raise 'invalid type', type(obj), list
    for x in obj:
        if type(x) != typeValue:
            raise 'invalid type', type(x), typeValue

def verify_gid_range(gidB, gidE, msg):
    '''
    gidB - begin gid.
    gidE - end gid.
    msg :: str - message for error.
    '''
    verify_type(gidB, int)
    verify_type(gidE, int)
    if gidB > gidE:
        raise Exception(msg, 'bad gid range', gidB, gidE)


class Walb:
    '''
    Walb command set.
    '''
    def __init__(self, cfg):
        '''
        cfg :: Config - Configuration.
        '''
        self.set_config(cfg)

    def set_config(self, cfg):
        ''' Set configuration '''
        verify_type(cfg, Config)
        self.cfg = cfg

    def run_ctl(self, s, cmdArgs, putMsg=True):
        '''
        Run walb-tools controller.
        s :: Server      - a server.
        cmdArgs :: [str] - command arguments.
        putMsg :: bool   - put debug message if True.
        return :: str    - stdout of the control command.
        '''
        verify_type(s, Server)
        verify_list_type(cmdArgs, str)
        ctlArgs = [self.cfg.binDir + "/controller",
                "-id", "ctrl",
                "-a", "localhost",
                "-p", s.port] + self._get_debug_opt()
        return run_command(ctlArgs + cmdArgs, putMsg)

    def get_host_type(self, s, putMsg=True):
        '''
        Get host type.
        s :: Server
        return :: str - 'storage', 'proxy', or 'archive'.
        '''
        verify_type(s, Server)
        return self.run_ctl(s, ['get', 'host-type'], putMsg)

    def get_state(self, s, vol, putMsg=True):
        '''
        Get state of a volume.
        s :: Server
        vol :: str    - volume name.
        return :: str - state.
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        return self.run_ctl(s, ['get', 'state', vol], putMsg)

    def reset_vol(self, s, vol):
        '''
        Reset a volume.
        s :: Server - storage or archive.
        vol :: str  - volume name.
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        self.run_ctl(s, ["reset-vol", vol])
        if s.kind == K_STORAGE:
            self._wait_for_state(s, vol, [sSyncReady]) # QQQ
        elif s.kind == K_ARCHIVE:
            self._wait_for_state(s, vol, [aSyncReady]) # QQQ
        else:
            raise Exception('reset_vol:bad server', s)

    def set_slave_storage(self, sx, vol):
        '''
        Set a volume slave storage in sx.

        sx :: Server - storage server.
        vol :: str   - volume name.
        '''
        verify_type(sx, Server)
        verify_type(vol, str)
        state = get_state(sx, vol)
        if state == sSlave:
            return
        if state == sSyncReady:
            start(sx, vol)
            return
        if state == sMaster:
            stop(sx, vol)
        else:
            raise Exception('set_slave_storage:bad state', state)
        stop_sync(cfg.archiveL[0], vol)
        reset_vol(sx, vol)
        start(sx, vol)

    def kick_all(self, sL):
        '''
        Kick all servers.
        sL :: [Server] - list of servers each of which must be storage or proxy.
        '''
        verify_list_type(sL, Server)
        for s in sL:
            self.run_ctl(s, ["kick"])

    def kick_all_storage(self):
        ''' Kick all storage servers. '''
        self.kick_all(self.cfg.storageL)


    def run_walbctl(self, cmdArgs):
        '''
        Run walbctl command.
        cmdArgs :: [str] - command line arguments.
        '''
        verify_list_type(cmdArgs, str)
        run_command([self.cfg.binDir + 'walbctl'] + cmdArgs)


    def create_walb_dev(self, ldevPath, ddevPath, wdevId):
        '''
        Create a walb device.
        ldevPath :: str - underlying log device path.
        ddevPath :: str - underlying data device path.
        wdevId :: int   - walb device id.
        TODO: support fomrat_ldev options and create_wdev options.
        '''
        verify_type(ldevPath, str)
        verify_type(ddevPath, str)
        verify_type(wdevId, int)
        self.run_walbctl(['format_ldev',
                          '--ldev', ldevPath,
                          '--ddev', ddevPath])
        self.run_walbctl(['create_wdev',
                          '--ldev', ldevPath,
                          '--ddev', ddevPath,
                          '--name', str(wdevId)])

    def delete_walb_dev(self, wdevPath):
        '''
        Delete a walb device.
        wdevPath :: str - walb device path.
        '''
        verify_type(wdevPath, str)
        self.run_walbctl(['delete_wdev', '--wdev', wdevPath])

    def is_overflow(self, sx, vol):
        '''
        Check a storage is overflow or not.
        sx :: Server   - storage server.
        vol :: str     - volume name.
        return :: bool - True if the storage overflows.
        '''
        verify_type(sx, Server)
        verify_type(sx, str)
        return int(run_ctl(sx, ['get', 'is-overflow', vol])) != 0

    def verify_not_overflow(self, sx, vol):
        ''' Verify a volume does not overflow. '''
        if is_overflow(sx, vol):
            raise Exception('verify_not_overflow', sx, vol)

    def is_wdiff_send_error(self, px, vol, ax):
        '''
        Get wdiff-send-error value.
        px :: Server   - proxy.
        vol :: str     - volume name.
        ax :: Server   - archive.
        return :: bool
        '''
        verify_type(px, Server)
        verify_type(vol, str)
        verify_type(ax, Server)
        return int(run_ctl(px, ['get', 'is-wdiff-send-error', vol, ax.name])) != 0

    def reset_wdev(self, wdev):
        '''
        Reset walb device.
        wdev :: Wdev - walb device.
        '''
        verify_type(wdev, Wdev)
        if os.path.exists(wdev.path):
            self.delete_walb_dev(wdev.path)
        resize_lv(wdev.data, get_lv_size_mb(wdev.data), wdev.sizeMb, False)
        self.create_walb_dev(wdev.log, wdev.data, wdev.iD)

    def cleanup(self, vol, wdevL):
        '''
        Cleanup a volume.
        vol :: str       - volume name.
        wdevL :: [wdevL] - list of walb devices.
        '''
        verify_type(vol, str)
        verify_list_type(wdevL, Wdev)
        for s in self.cfg.storageL + self.cfg.proxyL + self.cfg.archiveL:
            self.clear_vol(s, vol)
        for wdev in wdevL:
            self.reset_wdev(wdev)

    def status(self, sL=[], vol=None):
        '''
        print server status.
        sL :: [Server] - server list.
        vol :: str or None - volume name. None means all.
        '''
        verify_list_type(sL, Server)
        if not sL:
            sL = self._get_all_servers()
        for s in sL:
            args = ['status']
            if vol:
                args.append(vol)
            print '++++++++++++++++++++', s.name, '++++++++++++++++++++'
            print self.run_ctl(s, args, False)

    def shutdown(self, s, mode="graceful"):
        '''
        Shutdown a server.
        s :: Server
        mode :: str - 'graceful' or 'force'.
        '''
        verify_type(s, Server)
        verify_type(mode, str)
        _verify_shutdown_mode(mode, 'shutdown')
        self.run_ctl(s, ["shutdown", mode])
        time.sleep(1)  # shutdown is asynchronous command.

    def shutdown_all(self, mode='graceful'):
        '''
        Shutdown all servers.
        '''
        _verify_shutdown_mode(mode, 'shutdown_all')
        for s in self._get_all_servers():
            run_ctl(s, ["shutdown", mode])
        time.sleep(1)

    def get_alive_server(self):
        '''
        Get alive servers.
        return :: [str] - list of server name.
        '''
        ret = []
        for s in self._get_all_servers():
            try:
                self.get_host_type(s, False)
                ret.append(s.name)
            except:
                pass
        return ret

    def init(self, sx, vol, wdevPath):
        '''
        Initialize a volume.
        sx :: Server
        vol :: str
        wdevPath :: str
        '''
        verify_type(sx, Server)
        verify_type(vol, str)
        verify_type(wdevPath, str)
        self.run_ctl(sx, ["init-vol", vol, wdevPath])
        self.start(sx, vol)
        a0 = self.get_primary_archive()
        if self.get_state(a0, vol) == aClear:
            self.run_ctl(a0, ["init-vol", vol])

    def clear_vol(self, s, vol):
        '''
        Clear a volume.
        s :: Server
        vol :: str
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        st = self.get_state(s, vol)
        if s.kind == K_STORAGE:
            if st == sClear:
                return
            if st in [sMaster, sSlave]:
                self.stop(s, vol)
                st = self.get_state(s, vol)
            if st == sStopped:
                self.reset_vol(s, vol)
                st = self.get_state(s, vol)
            if st != sSyncReady:
                raise Exception('clear_vol', s, vol, st)
        elif s.kind == K_PROXY:
            if st == pClear:
                return
            if st in pActive:
                self.stop(s, vol)
                st = self.get_state(s, vol)
            if st != pStopped:
                raise Exception('clear_vol', s, vol, st)
        else:
            assert s.kind == K_ARCHIVE
            if st == aClear:
                return
            if st == aArchived:
                self.stop(s, vol)
                st = self.get_state(s, vol)
            if st not in aAcceptForClearVol:
                raise Exception('clear_vol', s, vol, st)
        self.run_ctl(s, ['clear-vol', vol])

    def get_archive_info_list(self, px, vol):
        '''
        Get archive list registered to a proxy.
        return :: [str] - list of archive name.
        '''
        verify_type(px, Server)
        verify_type(vol, str)
        st = self.run_ctl(px, ["archive-info", "list", vol])
        return st.split()

    def is_synchronizing(self, ax, vol):
        '''
        Check whether a volume is synchronizing with an archive server or not.
        ax :: Server - archive server.
        vol :: str
        return :: bool
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        ret = []
        for px in self.cfg.proxyL:
            ret.append(ax.name in self.get_archive_info_list(px, vol))
        v = sum(ret)
        if v == len(ret): # all True
            return True
        elif v == 0: # all False
            return False
        raise Exception('is_synchronizing: some are synchronizing, some are not.')

    def wait_for_stopped(self, s, vol, prevSt=None):
        '''
        Wait for a volue of a server stopped.
        s :: Server
        vol :: str
        prevSt :: None or str - specify if s is storage server.
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        if prevSt:
            verify_type(prevSt, str)
        if s.kind == K_STORAGE:
            if not prevSt:
                raise Exception('wait_for_stopped: prevSt not specified', s, vol)
            if prevSt == sSlave:
                tmpStL = sDuringStopForSlave
                goalSt = sSyncReady
            else:
                tmpStL = sDuringStopForMaster
                goalSt = sStopped
        elif s.kind == K_PROXY:
            tmpStL = pDuringStop
            goalSt = pStopped
        else:
            assert s.kind == K_ARCHIVE
            tmpStL = aDuringStop
            goalSt = aStopped
        self._wait_for_state_change(s, vol, tmpStL, [goalSt], TIMEOUT_SEC)

    def stop(self, s, vol, mode="graceful"):
        '''
        Stop a volume at a server and wait for it stopped.
        s :: Server
        vol :: str
        mode :: str - 'graceful' or 'force' or 'empty'.
            'empty' is valid only if s is proxy.
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        if mode not in ['graceful', 'empty', 'force']:
            raise Exception('stop:bad mode', mode)
        prevSt = self.get_state(s, vol)
        self.run_ctl(s, ["stop", vol, mode])
        self.wait_for_stopped(s, vol, prevSt)

    def start(self, s, vol):
        '''
        Start a volume at a server and wait for it started.
        s :: Server
        vol :: str
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        if s.kind == K_STORAGE:
            st = self.get_state(s, vol)
            if st == sSyncReady:
                self.run_ctl(s, ['start', vol, 'slave'])
                self._wait_for_state_change(s, vol, [stStartSlave], [sSlave])
            else:
                assert st == sStopped
                self.run_ctl(s, ['start', vol, 'master'])
                self._wait_for_state_change(s, vol, [stStartMaster], [sMaster])
        elif s.kind == K_PROXY:
            self.run_ctl(s, ['start', vol])
            self._wait_for_state_change(s, vol, [ptStart], pActive)
        else:
            assert s.kind == K_ARCHIVE
            self.run_ctl(s, ['start', vol])
            self._wait_for_state_change(s, vol, [atStart], aActive)

    def del_archive_from_proxy(self, px, vol, ax):
        '''
        Delete an archive from a proxy.
        px :: Server - proxy server.
        vol :: str   - voume name.
        ax :: Server - archive server.
        '''
        verify_type(px, Server)
        verify_type(vol, str)
        verify_type(ax, Server)
        st = self.get_state(px, vol)
        if st in pActive:
            self.stop(px, vol)
        aL = self.get_archive_info_list(px, vol)
        if ax.name in aL:
            self.run_ctl(px, ['archive-info', 'delete', vol, ax.name])
        st = self.get_state(px, vol)
        if st == pStopped:
            self.start(px, vol)

    def add_archive_to_proxy(self, px, vol, ax, doStart=True):
        '''
        Add an archive to a proxy.
        px :: Server    - proxy server.
        vol :: str      - volume name.
        ax :: server    - archive server.
        doStart :: bool - False if not to start proxy after adding.
        '''
        verify_type(px, Server)
        verify_type(vol, str)
        verify_type(ax, Server)
        verify_type(doStart, bool)
        st = get_state(px, vol)
        if st in pActive:
            stop(px, vol)
        aL = get_archive_info_list(px, vol)
        if ax.name not in aL:
            run_ctl(px, ['archive-info', 'add', vol, ax.name, get_host_port(ax)])
        st = get_state(px, vol)
        if st == pStopped and doStart:
            start(px, vol)

    def get_server(self, name, L):
        '''
        Get only one element from L having name
        name :: str   - server name.
        L :: [Server] - server list.
        return :: Server
        '''
        verify_type(name, str)
        verify_list_type(L, Server)
        ret = []
        for x in L:
            if x.name == name:
                ret.append(x)
        if len(ret) != 1:
            raise Exception('get_server:not one', ret, name, L)
        return ret[0]

    def copy_archive_info(self, pSrc, vol, pDst):
        '''
        Copy archive info from a proxy to another proxy.
        pSrc :: Server - srouce proxy.
        vol :: str
        pDst :: Server - destination proxy. It must be stopped.
        '''
        verify_type(pSrc, Server)
        verify_type(vol, str)
        verify_type(pDst, Server)
        for axName in self.get_archive_info_list(pSrc, vol):
            ax = self.get_server(axName, cfg.archiveL)
            self.add_archive_to_proxy(pDst, vol, ax, doStart=False)
        self.start(pDst, vol)

    def stop_sync(self, ax, vol):
        '''
        Stop synchronization of a volume with an archive.
        ax :: Server - archive server.
        vol :: str   - volume name.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        for px in self.cfg.proxyL:
            self.del_archive_from_proxy(px, vol, ax)
        self.kick_all_storage()

    def start_sync(self, ax, vol):
        '''
        Start synchronization of a volume with an archive.
        ax :: Server - archive server.
        vol :: str   - volume name.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        for px in self.cfg.proxyL:
            self.add_archive_to_proxy(px, vol, ax)
        self.kick_all_storage()

    def get_gid_list(self, ax, vol, cmd, optL=[]):
        '''
        Get gid list
        ax :: Server    - archive server.
        vol :: str      - volume name.
        cmd :: str      - 'restorable' or 'restored'.
        optL :: [str]   - options.
        return :: [int] - gid list.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        if not cmd in ['restorable', 'restored']:
            raise Exception('get_list_gid : bad cmd', cmd)
        verify_list_type(optL, str)
        ret = run_ctl(ax, ['get', cmd, vol] + optL)
        return map(int, ret.split())

    def list_restorable(self, ax, vol, opt=''):
        '''
        Get restorable gid list.
        ax :: Server - archive server.
        vol :: str   - volume name.
        opt :: str   - you can specify 'all'.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(opt, str)
        optL = []
        if opt:
            if opt == 'all':
                optL.append(opt)
            else:
                raise Exception('list_restorable:bad opt', opt)
        return self.get_gid_list(ax, vol, 'restorable', optL)

    def list_restored(self, ax, vol):
        '''
        Get restored gid list.
        ax :: Server - archive server.
        vol :: str   - volume name.
        '''
        return self.get_gid_list(ax, vol, 'restored')

    def wait_for_restorable_any(self, ax, vol, timeoutS=TIMEOUT_SEC):
        '''
        ax :: Server    - archive server.
        vol :: str      - volume name.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(timeoutS, int)
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = list_restorable(ax, vol)
            if gids:
                return gids[-1]
            time.sleep(0.3)
        return -1

    def wait_for_gid(self, ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
        '''
        Wait for an gid is available.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        cmd :: str      - 'restorable' or 'restored'.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)
        verify_type(timeoutS, int)
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = self.get_gid_list(ax, vol, cmd)
            if gid in gids:
                return
            time.sleep(0.3)
        raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)

    def wait_for_not_gid(self, ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a gid is not available.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        cmd :: str      - 'restorable' or 'restored'.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)
        verify_type(timeoutS, int)
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = get_gid_list(ax, vol, cmd)
            if gid not in gids:
                return
            time.sleep(0.3)
        raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)

    def wait_for_restorable(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot specified of a gid to be restorable.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        self.wait_for_gid(ax, vol, gid, 'restorable', timeoutS)

    def wait_for_restored(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot specified of a gid to be restored.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(gid, int)
        self.wait_for_no_action(ax, vol, 'Restore', timeoutS)
        gids = self.list_restored(ax, vol)
        if gid in gids:
            return
        raise Exception('wait_for_restored:failed', ax.name, vol, gid, gids)

    def wait_for_not_restored(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a restored snapshot specified of a gid to be removed
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        self.wait_for_not_gid(ax, vol, gid, 'restored', timeoutS)

    def wait_for_applied(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for diffs older than a gid to be applied.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(gid, int)
        self.wait_for_no_action(ax, vol, aaApply, timeoutS)
        gidL = self.list_restorable(ax, vol)
        if gidL and gid <= gidL[0]:
            return
        raise Exception('wait_for_applied:failed', ax.name, vol, gid, gidL)

    def wait_for_merged(self, ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
        '''
        Wait for diffs in a gid range to be merged.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gidB :: int     - begin generation id.
        gidE :: int     - end generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_gid_range(gidB, gidE, 'wait_for_merged')
        self.wait_for_no_action(ax, vol, aaMerge, timeoutS)
        gidL = self.list_restorable(ax, vol, 'all')
        pos = gidL.index(gidB)
        if gidL[pos + 1] == gidE:
            return
        raise Exception("wait_for_merged:failed", ax.name, vol, gidB, gidE, pos, gidL)

    def wait_for_replicated(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot is restorable at an archive server.
        ax :: Server    - archive server as a replication server (not client).
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(gid, int)
        self._wait_for_not_state(ax, vol, aDuringReplicate, timeoutS)
        gidL = self.list_restorable(ax, vol, 'all')
        if gidL and gid <= gidL[-1]:
            return
        raise Exception("wait_for_replicated:replicate failed", ax.name, vol, gid, gidL)

    def verify_not_restorable(self, ax, vol, gid, waitS, msg):
        '''
        Verify a snapshot does not become restorable
        at an archive server in a period.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        waitS :: int    - wait period [sec].
        msg :: str      - message for error.
        '''
        verify_type(msg, str)
        e = False
        try:
            self.wait_for_restorable(ax, vol, gid, waitS)
            e = True
        except:
            # expect to fail due to timeout.
            pass
        if e:
            raise Exception(msg, 'gid must not be restorable', gid)

    def replicate_sync(self, aSrc, vol, aDst):
        '''
        Copy current (aSrc, vol) to aDst.
        aSrc :: Server - source archive (as a client).
        vol :: str     - volume name.
        aDst :: Server - destination archive (as a server).
        '''
        gidL = list_restorable(aSrc, vol)
        gid = gidL[-1]
        run_ctl(aSrc, ['replicate', vol, "gid", str(gid), get_host_port(aDst)])
        wait_for_replicated(aDst, vol, gid)

    def synchronize(self, aSrc, vol, aDst):
        '''
        Synchronize aDst with (aSrc, vol).
        To reduce proxies stopped period, replicate nosync before calling this.
        aSrc :: Server - source archive (as a client).
        vol :: str     - volume name.
        aDst :: Server - destination archive (as a server).
        '''
        verify_type(aSrc, Server)
        verify_type(vol, str)
        verify_type(aDst, Server)

        for px in self.cfg.proxyL:
            st = self.get_state(px, vol)
            if st in pActive:
                self.run_ctl(px, ["stop", vol, 'empty'])

        for px in self.cfg.proxyL:
            self.wait_for_stopped(px, vol)
            aL = self.get_archive_info_list(px, vol)
            if aDst.name not in aL:
                self.run_ctl(px, ["archive-info", "add", vol,
                                  aDst.name, get_host_port(aDst)])

        self.replicate_sync(aSrc, vol, aDst)

        for px in self.cfg.proxyL:
            self.start(px, vol)
        self.kick_all_storage()

    def prepare_backup(self, sx, vol):
        '''
        Prepare backup.
        sx :: Server - storage server.
        vol :: str   - volume name.
        '''
        verify_type(sx, Server)
        verify_type(vol, str)

        a0 = self.get_primary_archive()
        st = self.get_state(sx, vol)
        if st == sSlave:
            self.stop(sx, vol)
        elif st == sMaster:
            self.stop(sx, vol)
            self.reset_vol(sx, vol)

        for s in self.cfg.storageL:
            if s == sx:
                continue
            st = self.get_state(s, vol)
            if st not in [sSlave, sClear]:
                raise Exception("prepare_backup:bad state", s.name, vol, st)

        for ax in self.cfg.archiveL:
            if self.is_synchronizing(ax, vol):
                self.stop_sync(ax, vol)

        self.start_sync(a0, vol)

    def full_backup(self, sx, vol, timeoutS=TIMEOUT_SEC):
        '''
        Run full backup a volume of a storage server.
        Log transfer to the primary archive server will start automatically.
        This function will return when a clean snapshot is
        sx :: Server    - storage server.
        vol :: str      - volume name.
        timeoutS :: int - timeout [sec].
                          Counter will start after dirty full backup done.
        return :: int   - generation id of a clean snapshot.
        '''
        verify_type(sx, Server)
        verify_type(vol, str)
        verify_type(timeoutS, int)

        a0 = self.get_primary_archive()
        self.prepare_backup(sx, vol)
        self.run_ctl(sx, ["full-bkp", vol])
        self._wait_for_state_change(sx, vol, sDuringFullSync,
                                   [sMaster], timeoutS)
        st = self.get_state(a0, vol)
        if st not in aActive:
            raise Exception('full_backup: sync failed', sx, a0, vol, st)

        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = self.list_restorable(a0, vol)
            if gids:
                return gids[-1]
            time.sleep(0.3)
        raise Exception('full_backup:timeout', sx, vol, gids)

    def hash_backup(self, sx, vol, timeoutS=TIMEOUT_SEC):
        '''
        Run hash backup a volume of a storage server.
        Log transfer to the primary archive server will start automatically.
        This function will return a gid of a clean snapshot that is
        restorable at the primary archive server.

        sx :: Server    - storage server.
        vol :: str      - volume name.
        timeoutS :: int - timeout [sec].
                          Counter will start after dirty hash backup done.
        return :: int   - generation id of a clean snapshot.
        '''
        verify_type(sx, Server)
        verify_type(vol, str)
        verify_type(timeoutS, int)

        a0 = self.get_primary_archive()
        self.prepare_backup(sx, vol)
        prev_gids = self.list_restorable(a0, vol)
        if prev_gids:
            max_gid = prev_gids[-1]
        else:
            max_gid = -1
        self.run_ctl(sx, ["hash-bkp", vol])
        self._wait_for_state_change(sx, vol, sDuringHashSync,
                                   [sMaster], timeoutS)
        st = self.get_state(a0, vol)
        if st not in aActive:
            raise Exception('hash_backup: sync failed', sx, a0, vol, st)

        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = self.list_restorable(a0, vol)
            if gids and gids[-1] > max_gid:
                return gids[-1]
            time.sleep(0.3)
        raise Exception('hash_backup:timeout', sx, vol, max_gid, gids)

    def get_restored_path(self, ax, vol, gid):
        '''
        ax :: Server  - archive server.
        vol :: str    - volume name.
        gid :: int    - generation id.
        return :: str - restored path.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)
        return '/dev/' + ax.vg + '/r_' + vol + '_' + str(gid)

    def get_lv_path(self, ax, vol):
        '''
        ax :: Server  - archive server.
        vol :: str    - volume name.
        return :: str - lv path.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        return '/dev/' + ax.vg + '/i_' + vol

    def restore(self, ax, vol, gid):
        '''
        Restore a volume.
        ax :: Server  - archive server.
        vol :: str    - volume name.
        gid :: int    - generation id.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)

        self.run_ctl(ax, ['restore', vol, str(gid)])
        self.wait_for_restored(ax, vol, gid)
        wait_for_lv_ready(get_restored_path(ax, vol, gid)) # QQQ

    def del_restored(self, ax, vol, gid):
        '''
        Delete a restored volume.
        ax :: Server  - archive server.
        vol :: str    - volume name.
        gid :: int    - generation id.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)

        wait_for_lv_ready(get_lv_path(ax, vol))
        retryTimes = 3
        for i in xrange(retryTimes):
            try:
                self.run_ctl(ax, ['del-restored', vol, str(gid)])
                break
            except Exception, e:
                print 'del-restored retry', i, e
                time.sleep(1)
        else:
            raise Exception('del-restored: exceeds max retry times')
        self.wait_for_not_restored(ax, vol, gid)

    def snapshot_async(self, sx, vol):
        '''
        Take a snaphsot.
        sx :: Server  - storage server.
        vol :: str    - volume name.
        return :: int - gid of the taken snapshot.
        '''
        verify_type(sx, Server)
        verify_type(vol, str)
        gid = run_ctl(sx, ['snapshot', vol])
        return int(gid)

    def snapshot_sync(self, sx, vol, axL):
        '''
        Take a snaphsot and wait for it to be restorable in archive servers.
        sx :: Server    - storage server.
        vol :: str      - volume name.
        axL :: [Server] - archive server list.
        return :: int   - gid of the taken snapshot.
        '''
        verify_list_type(axL, Server)
        gid = self.snapshot_async(sx, vol)
        for ax in axL:
            self.wait_for_restorable(ax, vol, gid)
        return gid

    def apply_diff(self, ax, vol, gid):
        '''
        Apply diffs older than a gid the base lv.
        ax :: Server - archive server
        vol :: str   - volume name.
        gid :: int   - generation id.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)
        self.run_ctl(ax, ["apply", vol, str(gid)])
        self.wait_for_applied(ax, vol, gid)

    def merge_diff(self, ax, vol, gidB, gidE):
        '''
        Merge diffs in gid ranges.
        ax :: Server - archive server.
        vol :: str   - volume name.
        gidB :: int  - begin gid.
        gidE :: int  - end gid.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_gid_range(gidB, gidE, 'merge_diff')
        self.run_ctl(ax, ["merge", vol, str(gidB), "gid", str(gidE)])
        self.wait_for_merged(ax, vol, gidB, gidE)

    def replicate(self, aSrc, vol, aDst, synchronizing):
        '''
        Replicate archive data by copying a volume from one archive to another.
        aSrc :: Server        - source archive server (as client).
        vol :: str            - volume name.
        aDst :: Server        - destination archive server (as server).
        synchronizing :: bool - True if you want to make aDst synchronizing.
        '''
        verify_type(aSrc, Server)
        verify_type(vol, str)
        verify_type(aDst, Server)
        verify_type(synchronizing, bool)

        st = self.get_state(aDst, vol)
        if st == aClear:
            self.run_ctl(aDst, ["init-vol", vol])

        self.replicate_sync(aSrc, vol, aDst)
        if synchronizing:
            self.synchronize(aSrc, vol, aDst)

    def wait_for_no_action(self, s, vol, action, timeoutS=TIMEOUT_SEC):
        '''
        s :: Server     - server.
        vol :: str      - volume name.
        action :: str   - action name.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        verify_type(action, str)
        verify_type(timeoutS, int)

        t0 = time.time()
        while time.time() < t0 + timeoutS:
            num = int(self.run_ctl(s, ['get', 'num-action', vol, action]))
            if num == 0:
                return
            time.sleep(0.3)
        raise Exception("wait_for_no_action", s, vol, action)

    def restore_and_verify_sha1(self, msg, md0, ax, vol, gid):
        '''
        Restore a volume and verify sha1sum.
        msg :: str   - message for error.
        md0 :: str   - sha1sum.
        ax :: Server - archive server.
        vol :: str   - volume name.
        gid :: int   - generation id.
        '''
        verify_type(msg, str)
        verify_type(md0, str)
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)
        md1 = self.get_sha1_of_restorable(ax, vol, gid)
        verify_equal_sha1(msg, md0, md1)

    def get_sha1_of_restorable(self, ax, vol, gid):
        '''
        Get sha1sum of restorable snapshot.
        ax :: Server  - archive server.
        vol :: str    - volume name.
        gid :: int    - generation id.
        return :: str - sha1sum.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(gid, int)
        self.restore(ax, vol, gid)
        md = get_sha1(self.get_restored_path(ax, vol, gid))
        self.del_restored(ax, vol, gid)
        return md

    def verify_equal_list_restorable(self, msg, ax, ay, vol):
        '''
        msg :: str   - message for error.
        ax :: Server - first archive server.
        ay :: Server - second archive server.
        vol :: str   - volume name.
        '''
        verify_type(msg, str)
        verify_type(ax, Server)
        verify_type(ay, Server)
        verify_type(vol, str)

        xL = self.list_restorable(ax, vol)
        yL = self.list_restorable(ay, vol)
        if self.cfg.debug:
            print 'list0', xL
            print 'list1', yL
        if xL != yL:
            raise Exception(msg, 'list_restorable differ', xL, yL)

    def get_latest_clean_snapshot(self, ax, vol):
        '''
        ax :: Server - archive server.
        vol :: str   - volume name.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        xL = self.list_restorable(ax, vol)
        if xL:
            return xL[-1]
        else:
            raise Exception('get_latest_clean_snapshot:not found')

    def wait_for_resize(self, ax, vol, sizeMb):
        '''
        Wait for resize done.
        ax :: Server  - archive server.
        vol :: str    - volume name.
        sizeMb :: int - new size [MiB].
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(sizeMb, int)
        self.wait_for_no_action(ax, vol, aaResize)
        curSizeMb = get_lv_size_mb(self.get_lv_path(ax, vol))
        if curSizeMb != sizeMb:
            raise Exception('wait_for_resize:failed', ax, vol, sizeMb, curSizeMb)

    def resize_archive(self, ax, vol, sizeMb, doZeroClear):
        '''
        Resize archive volume.
        ax :: Server        - archive server.
        vol :: str          - volume name.
        sizeMb :: int       - new size [MiB].
        doZeroClear :: bool - True if zero-clear extended area.
        '''
        verify_type(ax, Server)
        verify_type(vol, str)
        verify_type(sizeMb, int)
        verify_type(doZeroClear, bool)
        st = self.get_state(ax, vol)
        if st == aClear:
            return
        elif st in aAcceptForResize:
            args = ['resize', vol, str(sizeMb) + 'm']
            if doZeroClear:
                args += ['zeroclear']
            self.run_ctl(ax, args)
            self.wait_for_resize(ax, vol, sizeMb)
        else:
            raise Exception('resize_archive:bad state', ax, vol, sizeMb, st)

    def resize_storage(self, sx, vol, sizeMb):
        '''
        Resize storage volume.
        sx :: Server  - storage server.
        vol :: str    - voume name.
        sizeMb :: int - new size [MiB].
        '''
        verify_type(sx, Server)
        verify_type(vol, str)
        verify_type(sizeMb, int)
        st = self.get_state(sx, vol)
        if st == sClear:
            return
        else:
            self.run_ctl(sx, ['resize', vol, str(sizeMb) + 'm'])

    def resize(self, vol, sizeMb, doZeroClear):
        '''
        Resize volume.
        vol :: str          - volume name.
        sizeMb :: int       - new size [MiB].
        doZeroClear :: bool - True if you want to zero-clear the extended area.
        '''
        verify_type(vol, str)
        verify_type(sizeMb, int)
        verify_type(doZeroClear, bool)
        for ax in self.cfg.archiveL:
            self.resize_archive(ax, vol, sizeMb, doZeroClear)
        for sx in self.cfg.storageL:
            self.resize_storage(sx, vol, sizeMb)

    def wait_for_log_empty(self, wdev, timeoutS=TIMEOUT_SEC):
        '''
        Wait for log device becomes empty.
        wdev :: Wdev    - walb device.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(wdev, Wdev)
        verify_type(timeoutS, int)
        def create_key_value(ls):
            ret = []
            for s in ls:
                (k, v) = s.strip().split()
                ret.append((k, int(v)))
            return dict(ret)
        sysName = '/sys/block/walb!%d/walb/lsids' % wdev.iD
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            with open(sysName, 'r') as f:
                kv = create_key_value(f.readlines())
                completed = kv['completed']
                oldest = kv['oldest']
                if completed == oldest:
                    return
            print "wait_for_log_empty", oldest, completed
            time.sleep(0.3)
        raise Exception("wait_for_log_empty", wdev)

    def get_walb_dev_sizeMb(self, wdev):
        '''
        Get walb device size.
        wdev :: Wdev -- walb device.
        return :: int -- device size [MiB].
        '''
        verify_type(wdev, Wdev)
        sysName = '/sys/block/walb!%d/size' % wdev.iD
        with open(sysName, 'r') as f:
            size = int(f.read().strip()) * 512 / 1024 / 1024
        return size

    def get_primary_archive(self):
        '''
        Get primary archive.
        return :: Server
        '''
        return self.cfg.archiveL[0]

    '''
    Privae member functions.
    '''

    def _get_all_servers(self):
        return self.cfg.storageL + self.cfg.proxyL + self.cfg.archiveL

    def _get_debug_opt(self):
        if self.cfg.debug:
            return ["-debug"]
        else:
            return []

    def _get_host_port(self, s):
        return "localhost" + ":" + s.port

    def _wait_for_state_cond(self, s, vol, pred, msg, timeoutS=10):
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            st = self.get_state(s, vol)
    #        print "c=", s, vol, stateL, c, st
            if pred(st):
                return
            time.sleep(0.3)
        raise Exception("wait_for_state_cond", s, vol, msg)

    def _wait_for_state(self, s, vol, stateL, timeoutS=10):
        def pred(st):
            return st in stateL
        self._wait_for_state_cond(
            s, vol, pred, 'stateL:' + str(stateL), timeoutS)

    def _wait_for_not_state(self, s, vol, stateL, timeoutS=10):
        def pred(st):
            return st not in stateL
        self._wait_for_state_cond(
            s, vol, pred, 'not stateL:' + str(stateL), timeoutS)

    def _wait_for_state_change(self, s, vol, tmpStateL,
                               goalStateL, timeoutS=10):
        self._wait_for_not_state(s, vol, tmpStateL, timeoutS)
        st = self.get_state(s, vol)
        if st not in goalStateL:
            raise Exception('wait_for_state_change:bad goal',
                            s, vol, tmpStateL, goalStateL, st)

