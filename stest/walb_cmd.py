import collections
import os
import threading
import subprocess
import sys
import time
import socket
import errno

TIMEOUT_SEC = 20

K_STORAGE = 0
K_PROXY = 1
K_ARCHIVE = 2

Server = collections.namedtuple('Server', 'name port kind vg')
Config = collections.namedtuple(
    'Config', 'debug binDir dataDir storageL proxyL archiveL')

cfg = None


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

    else:  # s.kind == K_ARCHIVE
        ret = [cfg.binDir + "archive-server", "-vg", s.vg]

    ret += ["-p", s.port,
            "-b", cfg.dataDir + s.name,
            "-l", s.name + ".log",
            "-id", s.name] + get_debug_opt()
    return ret


def run_command(args, putMsg=True):
    if cfg.debug and putMsg:
        print "run_command:", to_str(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=sys.stderr)
    f = p.stdout
    s = f.read().strip()
    ret = p.wait()
    if ret != 0:
        raise Exception("command error %d\n" % ret)
    if cfg.debug and putMsg:
        print "run_command_result:", s
    return s


def run_ctl(server, cmdArgs, putMsg=True):
    ctlArgs = [cfg.binDir + "/controller",
            "-id", "ctrl",
            "-a", "localhost",
            "-p", server.port] + get_debug_opt()
    return run_command(ctlArgs + cmdArgs, putMsg)


def run_daemon(args):
    try:
        pid = os.fork()
        if pid > 0:
            return
    except OSError, e:
        print >>sys.stderr, "fork#1 failed (%d) (%s)" % (e.errno, e.strerror)

    os.chdir("/")
    os.setsid()
    os.umask(0)

    sys.stdin = open('/dev/null', 'r')
    sys.stdout = open('/dev/null', 'w')
    sys.stderr = open('/dev/null', 'w')

    subprocess.Popen(args).wait()
    sys.exit(0)


def wait_for_server_port(server):
    address = "localhost"
    port = int(server.port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    for i in range(1, 10):
        try:
            sock.connect((address, port))
            sock.close()
            return
        except socket.error, e:
            if e.errno not in [errno.ECONNREFUSED, errno.ECONNABORTED]:
                raise
        time.sleep(0.1)

#def hostType(server):
#    return run_ctl(server, ["host-type"])


def get_state(server, vol):
    return run_ctl(server, ["get-state", vol])


def set_slave_storage(sx, vol):
    state = get_state(sx, vol)
    if state == 'Slave':
        return
    if state == 'Master' or state == 'WlogSend':
        stop(sx, vol)
    else:
        raise Exception('set_slave_storage:bad state', state)
    stop_sync(cfg.archiveL[0], vol)
    run_ctl(sx, ["reset-vol", vol])
    start(sx, vol)


def kick_heartbeat_all():
    for sx in cfg.storageL:
        run_ctl(sx, ["kick-heartbeat"])


def run_walbctl(cmdArgs):
    run_command([cfg.binDir + 'walbctl'] + cmdArgs)


def create_walb_dev(ldevPath, ddevPath, wdevId):
    run_walbctl(['format_ldev',
                 '--ldev', ldevPath,
                 '--ddev', ddevPath])
    run_walbctl(['create_wdev',
                 '--ldev', ldevPath,
                 '--ddev', ddevPath,
                 '--name', str(wdevId)])


def delete_walb_dev(wdevPath):
    run_walbctl(['delete_wdev', '--wdev', wdevPath])


##################################################################
# user command functions


def wait_for_state(server, vol, stateL, timeoutS=10):
    t0 = time.clock()
    while time.clock() < t0 + timeoutS:
        st = get_state(server, vol)
#        print "c=", server, vol, stateL, c, st
        if st in stateL:
            return
        time.sleep(0.3)
    raise Exception("wait_for_state", server, vol, stateL)


def kill_all_servers():
    for s in ["storage-server", "proxy-server", "archive-server"]:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()
    time.sleep(0.3)


def startup(server):
    make_dir(cfg.dataDir + server.name)
    args = get_server_args(server)
    if cfg.debug:
        print 'cmd=', to_str(args)
    run_daemon(args)
    wait_for_server_port(server)


def startup_all():
    for s in cfg.archiveL + cfg.proxyL + cfg.storageL:
        startup(s)


def shutdown(server, mode="graceful"):
    run_ctl(server, ["shutdown", mode])


def shutdown_all():
    for s in cfg.storageL + cfg.proxyL + cfg.archiveL:
        shutdown(s)


def init(sx, vol, wdevPath):
    run_ctl(sx, ["init-vol", vol, wdevPath])
    start(sx, vol)
    run_ctl(cfg.archiveL[0], ["init-vol", vol])


def is_synchronizing(ax, vol):
    px = cfg.proxyL[0]
    st = run_ctl(px, ["archive-info", "list", vol])
    return ax in st.split()


def stop(s, vol, mode="graceful"):
    """
    stop s vol and wait until state is waitState

    """
    if mode not in ['graceful', 'empty', 'force']:
        raise Exception('stop:bad mode', mode)
    if s.kind == K_STORAGE and get_state(s, vol) == 'Slave':
        waitState = 'SyncReady'
    else:
        waitState = 'Stopped'
    run_ctl(s, ["stop", vol, mode])
    wait_for_state(s, vol, [waitState])


def start(s, vol):
    if s.kind == K_STORAGE:
        st = get_state(s, vol)
        if st == 'SyncReady':
            run_ctl(s, ['start', vol, 'slave'])
            wait_for_state(s, vol, ['Slave'])
        else:
            run_ctl(s, ['start', vol, 'master'])
            wait_for_state(s, vol, ['Master', 'WlogSend'])
    elif s.kind == K_PROXY:
        run_ctl(s, ['start', vol])
        wait_for_state(s, vol, ['Started'])
    else:  # s.kind == K_ARCHIVE
        run_ctl(s, ['start', vol])
        wait_for_state(s, vol, ['Archived'])


def stop_sync(ax, vol):
    for px in cfg.proxyL:
        stop(px, vol)
        run_ctl(px, ["archive-info", "delete", vol, ax.name])
        state = get_state(px, vol)
        if state == 'Stopped':
            start(px, vol)
    kick_heartbeat_all()


def get_gid_list(ax, vol, cmd):
    if not cmd in ['list-restorable', 'list-restored']:
        raise Exception('get_list_gid : bad cmd', cmd)
    ret = run_ctl(ax, [cmd, vol])
    return map(int, ret.split())


def list_restorable(ax, vol, opt=''):
    cmd = ["list-restorable", vol]
    if opt:
        if opt == 'all':
            cmd.append(opt)
        else:
            raise Exception('list_restorable:bad opt', opt)
    ret = run_ctl(ax, cmd)
    return map(int, ret.split())


def list_restored(ax, vol):
    ret = run_ctl(ax, ["list-restored", vol])
    return map(int, ret.split())


def wait_for_restorable_any(ax, vol, timeoutS=TIMEOUT_SEC):
    t0 = time.clock()
    while time.clock() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, 'list-restorable')
        if gids:
            return gids[-1]
        time.sleep(0.3)
    return -1


def wait_for_gid(ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
    t0 = time.clock()
    while time.clock() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, cmd)
        if gid in gids:
            return True
        time.sleep(0.3)
    return False


def wait_for_not_gid(ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
    t0 = time.clock()
    while time.clock() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, cmd)
        if gid not in gids:
            return True
        time.sleep(0.3)
    return False


def wait_for_restorable(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    return wait_for_gid(ax, vol, gid, 'list-restorable', timeoutS)


def wait_for_restored(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    return wait_for_gid(ax, vol, gid, 'list-restored', timeoutS)


def wait_for_not_restored(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    return wait_for_not_gid(ax, vol, gid, 'list-restored', timeoutS)


def wait_for_applied(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    t0 = time.clock()
    while time.clock() < t0 + timeoutS:
        gidL = get_gid_list(ax, vol, 'list-restorable')
        if gidL and gid <= gidL[0]:
            return
        time.sleep(0.3)
    raise Exception("wait_for_applied:timeout", ax, vol)


def wait_for_merged(ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
    t0 = time.clock()
    while time.clock() < t0 + timeoutS:
        gidL = list_restorable(ax, vol, 'all')
        pos = gidL.index(gidB)
        if gidL[pos + 1] == gidE:
            return
        time.sleep(0.3)
    raise Exception("wait_for_merged:timeout", ax, vol, gidB, gidE)


def wait_for_replicated(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    t0 = time.clock()
    while time.clock() < t0 + timeoutS:
        gidL = list_restorable(ax, vol, 'all')
        if gidL and gid <= gidL[-1]:
            return
        time.sleep(0.3)
    raise Exception("wait_for_replicated:timeout", ax, vol, gid)


def add_archive_to_proxy(px, vol, ax):
    st = get_state(px, vol)
    if st == "Started":
        stop(px, vol)
    run_ctl(px, ["archive-info", "add", vol, ax.name, get_host_port(ax)])
    start(px, vol)
    kick_heartbeat_all()


def replicate_sync(aSrc, vol, aDst):
    """
        copy current (aSrc, vol) to aDst
    """
    gidL = list_restorable(aSrc, vol)
    gid = gidL[-1]
    run_ctl(aSrc, ['replicate', vol, "gid", str(gid), get_host_port(aDst)])
    wait_for_replicated(aDst, vol, gid)


def synchronize(aSrc, vol, aDst):
    """
        synchronize aDst with (aSrc, vol)
    """
    for px in cfg.proxyL:
        st = get_state(px, vol)
        print 'px,st0', px, st
        if st == 'Started' or st == 'WlogRecv':
            stop(px, vol, 'empty')
        run_ctl(px, ["archive-info", "add", vol,
                     aDst.name, get_host_port(aDst)])
        st1 = get_state(px, vol)
        print 'px,st1', px, st1
        if st1 != 'Stopped':
            raise Exception('synchronize: must be Stopped state', px, vol, st1)
    replicate_sync(aSrc, vol, aDst)

    for px in cfg.proxyL:
        start(px, vol)
    kick_heartbeat_all()


def prepare_backup(sx, vol):
    a0 = cfg.archiveL[0]
    st = get_state(sx, vol)
    if st == "Slave":
        stop(sx, vol)

    ret = run_ctl(sx, ["is-overflow", vol])
    if ret != "0":
        run_ctl(sx, ["reset-vol", vol])

    for s in cfg.storageL:
        if s == sx:
            continue
        st = get_state(s, vol)
        if st != "Slave" and st != "Clear":
            raise Exception("full_backup : bad state", s.name, vol, st)

    for ax in cfg.archiveL[1:]:
        if is_synchronizing(ax, vol):
            stop_sync(ax, vol)

    for px in cfg.proxyL:
        add_archive_to_proxy(px, vol, a0)


def full_backup(sx, vol):
    a0 = cfg.archiveL[0]
    prepare_backup(sx, vol)
    run_ctl(sx, ["full-bkp", vol])
    wait_for_state(a0, vol, ["Archived"], TIMEOUT_SEC)

    t0 = time.clock()
    while time.clock() < t0 + TIMEOUT_SEC:
        gids = get_gid_list(a0, vol, 'list-restorable')
        if gids:
            return gids[-1]
        time.sleep(0.3)
    raise Exception('full_backup:timeout', sx, vol)


def hash_backup(sx, vol):
    a0 = cfg.archiveL[0]
    prepare_backup(sx, vol)
    prev_gids = get_gid_list(a0, vol, 'list-restorable')
    if prev_gids:
        max_gid = prev_gids[-1]
    else:
        max_gid = -1

    run_ctl(sx, ["hash-bkp", vol])
    wait_for_state(a0, vol, ["Archived"], TIMEOUT_SEC)

    t0 = time.clock()
    while time.clock() < t0 + TIMEOUT_SEC:
        gids = get_gid_list(a0, vol, 'list-restorable')
        if gids and gids[-1] > max_gid:
            return gids[-1]
        time.sleep(0.3)
    raise Exception('hash_backup:timeout', sx, vol)


def write_random(devName, size, offset=0):
    args = [cfg.binDir + "/write_random_data",
        '-s', str(size), '-o', str(offset), devName]
    return run_command(args, False)


def get_sha1(devName):
    ret = run_command(['/usr/bin/sha1sum', devName])
    return ret.split(' ')[0]


def restore(ax, vol, gid):
    run_ctl(ax, ['restore', vol, str(gid)])
    wait_for_restored(ax, vol, gid)


def del_restored(ax, vol, gid):
    run_ctl(ax, ['del-restored', vol, str(gid)])
    wait_for_not_restored(ax, vol, gid)


def get_restored_path(ax, vol, gid):
    return '/dev/' + ax.vg + '/r_' + vol + '_' + str(gid)


def snapshot_async(sx, vol):
    state = get_state(sx, vol)
    if state != 'Master' and state != 'WlogSend':
        raise Exception('snapshot_async', state)
    gid = run_ctl(sx, ['snapshot', vol])
    return int(gid)


def snapshot_sync(sx, vol, axs):
    gid = snapshot_async(sx, vol)
    for ax in axs:
        wait_for_restorable(ax, vol, gid)
    return gid


def verify_equal_sha1(msg, md0, md1):
    if md0 == md1:
        print msg + ' ok :', md0
    else:
        raise Exception('fail ' + msg, md0, md1)


def restore_and_verify_sha1(msg, md0, ax, vol, gid):
    md1 = get_sha1_of_restorable(ax, vol, gid)
    verify_equal_sha1(msg, md0, md1)


def get_sha1_of_restorable(ax, vol, gid):
    restore(ax, vol, gid)
    md = get_sha1(get_restored_path(ax, vol, gid))
    del_restored(ax, vol, gid)
    return md


def verify_equal_list_restorable(msg, ax, ay, vol):
    xL = list_restorable(ax, vol)
    yL = list_restorable(ay, vol)
    if cfg.debug:
        print 'list0', xL
        print 'list1', yL
    if xL != yL:
        raise Exception(msg, 'list_restorable differ', xL, yL)


def get_latest_clean_snapshot(ax, vol):
    xL = list_restorable(ax, vol)
    if xL:
        return xL[-1]
    else:
        raise Exception('get_latest_clean_snapshot:not found')


quitWriting = False


def writing(path):
    while not quitWriting:
        write_random(path, 1)
        time.sleep(0.05)


def startWriting(path):
    global quitWriting
    quitWriting = False
    t = threading.Thread(target=writing, args=(path,))
    t.start()
    return t


def stopWriting(t):
    global quitWriting
    quitWriting = True
    t.join()


def apply_diff(ax, vol, gid):
    run_ctl(ax, ["apply", vol, str(gid)])
    wait_for_applied(ax, vol, gid)


def merge_diff(ax, vol, gidB, gidE):
    run_ctl(ax, ["merge", vol, str(gidB), "gid", str(gidE)])
    wait_for_merged(ax, vol, gidB, gidE)


def replicate(aSrc, vol, aDst, synchronizing):
    """
        copy (aSrc, vol) to aDst
    """
    st = get_state(aDst, vol)
    if st == 'Clear':
        run_ctl(aDst, ["init-vol", vol])

    replicate_sync(aSrc, vol, aDst)
    if synchronizing:
        synchronize(aSrc, vol, aDst)


def resize(vol, sizeMb):
    for ax in cfg.archiveL:
        st = get_state(ax, vol)
        if st == 'Clear':
            continue
        elif st in ['Archived', 'WdiffRecv', 'HashSync', 'Stopped']:
            run_ctl(ax, ['resize', vol, str(sizeMb) + 'm'])
        else:
            raise Exception('resize:bad state', st)
    for sx in cfg.storageL:
        st = get_state(sx, vol)
        if st == 'Clear':
            continue
        else:
            run_ctl(sx, ['resize', vol, str(sizeMb) + 'm'])


def resizeLv(path, sizeMb):
    run_command(['/sbin/lvresize', '-f', '-L', str(sizeMb) + 'm', path])


def getLvSizeMb(path):
    ret = run_command(['/sbin/lvdisplay', '-C', '--noheadings', '-o', 'lv_size', '--units', 'm', path])
    ret.strip()
    if ret[-1] != 'm':
        raise Exception('getLvSizeMb: bad return value', ret)
    return int(float(ret[0:-2]))

