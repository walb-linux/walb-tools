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

aAcceptForResize = ['Archived', 'HashSync', 'WdiffRecv', 'ReplSyncAsServer', 'Stopped']
pAcceptForStop = ['Started', 'WlogRecv']

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


def wait_for_state(server, vol, stateL, timeoutS=10):
    def cond(st):
        return st in stateL
    wait_for_state_cond(server, vol, cond, 'stateL:' + str(stateL), timeoutS)


def wait_for_not_state(server, vol, stateL, timeoutS=10):
    def cond(st):
        return st not in stateL
    wait_for_state_cond(server, vol, cond, 'not stateL:' + str(stateL), timeoutS)


def reset_vol(s, vol):
    """
        s is storage or archive.
    """
    run_ctl(s, ["reset-vol", vol])
    wait_for_state(s, vol, ['SyncReady'])


def set_slave_storage(sx, vol):
    state = get_state(sx, vol)
    if state == 'Slave':
        return
    if state == 'SyncReady':
        start(sx, vol)
        return
    if state == 'Master':
        stop(sx, vol)
    else:
        raise Exception('set_slave_storage:bad state', state)
    stop_sync(cfg.archiveL[0], vol)
    reset_vol(sx, vol)
    start(sx, vol)


def kick_all(sL):
    for s in sL:
        run_ctl(s, ["kick"])


def kick_all_storage():
    kick_all(cfg.storageL)


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


def is_overflow(sx, vol):
    return int(run_ctl(sx, ["is-overflow", vol])) != 0


def verify_not_overflow(sx, vol):
    if is_overflow(sx, vol):
        raise Exception('verify_not_overflow', sx, vol)


##################################################################
# user command functions


def kill_all_servers():
    for s in ["storage-server", "proxy-server", "archive-server"]:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()
    time.sleep(0.3)


def remove_persistent_data(s):
    """
        call shutdown() before calling this.
    """
    shutil.rmtree(cfg.dataDir + s.name)
    if s in cfg.archiveL:
        for f in os.listdir('/dev/' + s.vg):
            if f[0:2] == 'i_':
                remove_lv('/dev/' + s.vg + '/' + f)


def startup(s):
    make_dir(cfg.dataDir + s.name)
    args = get_server_args(s)
    if cfg.debug:
        print 'cmd=', to_str(args)
    run_daemon(args)
    wait_for_server_port(s)


def startup_all():
    for s in cfg.archiveL + cfg.proxyL + cfg.storageL:
        startup(s)


def shutdown(s, mode="graceful"):
    run_ctl(s, ["shutdown", mode])
    time.sleep(1)  # shutdown is asynchronous command.


def shutdown_all(mode='graceful'):
    for s in cfg.storageL + cfg.proxyL + cfg.archiveL:
        run_ctl(s, ["shutdown", mode])
    time.sleep(1)


def init(sx, vol, wdevPath):
    run_ctl(sx, ["init-vol", vol, wdevPath])
    start(sx, vol)
    a0 = cfg.archiveL[0]
    if get_state(a0, vol) == 'Clear':
        run_ctl(a0, ["init-vol", vol])


def get_archive_info_list(px, vol):
    """
        return: [ax.name]
    """
    st = run_ctl(px, ["archive-info", "list", vol])
    return st.split()


def is_synchronizing(ax, vol):
    ret = []
    for px in cfg.proxyL:
        ret.append(ax.name in get_archive_info_list(px, vol))
    v = sum(ret)
    if v == len(ret): # all True
        return True
    elif v == 0: # all False
        return False
    raise Exception('is_synchronizing: some are synchronizing, some are not.')


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
            wait_for_state(s, vol, ['Master'])
    elif s.kind == K_PROXY:
        run_ctl(s, ['start', vol])
        wait_for_state(s, vol, ['Started'])
    else:  # s.kind == K_ARCHIVE
        run_ctl(s, ['start', vol])
        wait_for_state(s, vol, ['Archived'])


def del_archive_from_proxy(px, vol, ax):
    st = get_state(px, vol)
    if st in pAcceptForStop:
        stop(px, vol)
    aL = get_archive_info_list(px, vol)
    if ax.name in aL:
        run_ctl(px, ['archive-info', 'delete', vol, ax.name])
    st = get_state(px, vol)
    if st == 'Stopped':
        start(px, vol)


def add_archive_to_proxy(px, vol, ax):
    st = get_state(px, vol)
    if st in pAcceptForStop:
        stop(px, vol)
    aL = get_archive_info_list(px, vol)
    if ax.name not in aL:
        run_ctl(px, ['archive-info', 'add', vol, ax.name, get_host_port(ax)])
    st = get_state(px, vol)
    if st == 'Stopped':
        start(px, vol)


def stop_sync(ax, vol):
    for px in cfg.proxyL:
        del_archive_from_proxy(px, vol, ax)
    kick_all_storage()


def start_sync(ax, vol):
    for px in cfg.proxyL:
        add_archive_to_proxy(px, vol, ax)
    kick_all_storage()


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
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, 'list-restorable')
        if gids:
            return gids[-1]
        time.sleep(0.3)
    return -1


def wait_for_gid(ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, cmd)
        if gid in gids:
            return
        time.sleep(0.3)
    raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)


def wait_for_not_gid(ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = get_gid_list(ax, vol, cmd)
        if gid not in gids:
            return
        time.sleep(0.3)
    raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)


def wait_for_restorable(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    wait_for_gid(ax, vol, gid, 'list-restorable', timeoutS)


def wait_for_restored(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    wait_for_no_action(ax, vol, 'Restore', timeoutS)
    gids = get_gid_list(ax, vol, 'list-restored')
    if gid in gids:
        return
    raise Exception('wait_for_restored:failed', ax.name, vol, gid, gids)


def wait_for_not_restored(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    wait_for_not_gid(ax, vol, gid, 'list-restored', timeoutS)


def wait_for_applied(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    wait_for_no_action(ax, vol, 'Apply', timeoutS)
    gidL = get_gid_list(ax, vol, 'list-restorable')
    if gidL and gid <= gidL[0]:
        return
    raise Exception('wait_for_applied:failed', ax.name, vol, gid, gidL)


def wait_for_merged(ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
    wait_for_no_action(ax, vol, 'Merge', timeoutS)
    gidL = list_restorable(ax, vol, 'all')
    pos = gidL.index(gidB)
    if gidL[pos + 1] == gidE:
        return
    raise Exception("wait_for_merged:failed", ax.name, vol, gidB, gidE, pos, gidL)


def wait_for_replicated(ax, vol, gid, timeoutS=TIMEOUT_SEC):
    wait_for_not_state(ax, vol, ['ReplSyncAsServer', 'FullSync'], timeoutS)
    gidL = list_restorable(ax, vol, 'all')
    if gidL and gid <= gidL[-1]:
        return
    raise Exception("wait_for_replicated:replicate failed", ax.name, vol, gid, gidL)


def verify_not_restorable(ax, vol, gid, waitS, msg):
    """
       verify a snapshot (vol, gid) does not become restorable
       at ax in period waitS.
    """
    e = False
    try:
        wait_for_restorable(ax, vol, gid, waitS)
        e = True
    except:
        # expect to fail due to timeout.
        pass
    if e:
        raise Exception(msg, 'gid must not be restorable', gid)


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
        if st in pAcceptForStop:
            run_ctl(px, ["stop", vol, 'empty'])

    for px in cfg.proxyL:
        wait_for_state(px, vol, ['Stopped'])
        aL = get_archive_info_list(px, vol)
        if aDst.name not in aL:
            run_ctl(px, ["archive-info", "add", vol,
                         aDst.name, get_host_port(aDst)])

    replicate_sync(aSrc, vol, aDst)

    for px in cfg.proxyL:
        start(px, vol)
    kick_all_storage()


def prepare_backup(sx, vol):
    a0 = cfg.archiveL[0]
    st = get_state(sx, vol)
    if st == 'Slave':
        stop(sx, vol)
    elif st == 'Master':
        stop(sx, vol)
        reset_vol(sx, vol)

    for s in cfg.storageL:
        if s == sx:
            continue
        st = get_state(s, vol)
        if st not in ["Slave", "Clear"]:
            raise Exception("prepare_backup:bad state", s.name, vol, st)

    for ax in cfg.archiveL:
        if is_synchronizing(ax, vol):
            stop_sync(ax, vol)

    start_sync(a0, vol)


def full_backup(sx, vol, timeoutS=TIMEOUT_SEC):
    a0 = cfg.archiveL[0]
    prepare_backup(sx, vol)
    run_ctl(sx, ["full-bkp", vol])
    wait_for_state(a0, vol, ["Archived"], timeoutS)

    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = get_gid_list(a0, vol, 'list-restorable')
        if gids:
            return gids[-1]
        time.sleep(0.3)
    raise Exception('full_backup:timeout', sx, vol, gids)


def hash_backup(sx, vol, timeoutS=TIMEOUT_SEC):
    a0 = cfg.archiveL[0]
    prepare_backup(sx, vol)
    prev_gids = get_gid_list(a0, vol, 'list-restorable')
    if prev_gids:
        max_gid = prev_gids[-1]
    else:
        max_gid = -1

    run_ctl(sx, ["hash-bkp", vol])
    wait_for_state(a0, vol, ["Archived"], timeoutS)

    t0 = time.time()
    while time.time() < t0 + timeoutS:
        gids = get_gid_list(a0, vol, 'list-restorable')
        if gids and gids[-1] > max_gid:
            return gids[-1]
        time.sleep(0.3)
    raise Exception('hash_backup:timeout', sx, vol, max_gid, gids)


def write_random(devName, sizeLb, offset=0, fixVar=None):
    args = [cfg.binDir + "/write_random_data",
        '-s', str(sizeLb), '-o', str(offset), devName]
    if fixVar:
        args += ['-set', str(fixVar)]
    return run_command(args, False)


def get_sha1(devName):
    ret = run_command(['/usr/bin/sha1sum', devName])
    return ret.split(' ')[0]


def get_num_opened_lv(lvPath):
    ret = run_command(['/sbin/dmsetup', '-c', 'info', '-o', 'open',
                       '--noheadings', lvPath])
    ret.strip()
    return int(ret)


def lvm_sleep():
    time.sleep(2)


def wait_for_lv_ready(lvPath, timeoutS=TIMEOUT_SEC):
    lvm_sleep()
    """
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        num = get_num_opened_lv(lvPath)
        if num == 0:
            time.sleep(1) # avoid fail for the bug of lvm
            return
        time.sleep(0.3)
    raise Exception('wait_for_lv_ready:timeout', lvPath)
    """


def get_restored_path(ax, vol, gid):
    return '/dev/' + ax.vg + '/r_' + vol + '_' + str(gid)


def get_lv_path(ax, vol):
    return '/dev/' + ax.vg + '/i_' + vol


def restore(ax, vol, gid):
    run_ctl(ax, ['restore', vol, str(gid)])
    wait_for_restored(ax, vol, gid)
    wait_for_lv_ready(get_restored_path(ax, vol, gid))


def del_restored(ax, vol, gid):
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


def snapshot_async(sx, vol):
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


def wait_for_no_action(s, vol, action, timeoutS=TIMEOUT_SEC):
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        num = int(run_ctl(s, ['get-num-action', vol, action]))
        if num == 0:
            return;
        time.sleep(0.3)
    raise Exception("wait_for_no_action", s, vol, action)


def zero_clear(path, offsetLb, sizeLb):
    run_command(['/bin/dd', 'if=/dev/zero', 'of=' + path, # 'oflag=direct',
                 'bs=512', 'seek=' + str(offsetLb), 'count=' + str(sizeLb)])
    # flush data to avoid overwriting after walb's writing
    fd = os.open(path, os.O_RDONLY)
    os.fdatasync(fd)
    os.close(fd)


def resize_lv(path, beforeSizeMb, afterSizeMb, doZeroClear):
    """
        this support shrink also.
    """
    if beforeSizeMb == afterSizeMb:
        return
    run_command(['/sbin/lvresize', '-f', '-L', str(afterSizeMb) + 'm', path])
    wait_for_lv_ready(path)
    # zero-clear is required for test only.
    if beforeSizeMb < afterSizeMb and doZeroClear:
        zero_clear(path, beforeSizeMb * 1024 * 1024 / 512,
                   (afterSizeMb - beforeSizeMb) * 1024 * 1024 / 512)


def remove_lv(path):
    wait_for_lv_ready(path)
    for i in xrange(3):
        try:
            run_command(['/sbin/lvremove', '-f', path])
            return
        except:
            print 'remove_lv failed', i, path
            time.sleep(1)
    else:
        raise Exception('remove_lv:timeout', path)


def get_lv_size_mb(path):
    ret = run_command(['/sbin/lvdisplay', '-C', '--noheadings',
                       '-o', 'lv_size', '--units', 'b', path])
    ret.strip()
    if ret[-1] != 'B':
        raise Exception('get_lv_size_mb: bad return value', ret)
    return int(ret[0:-1]) / 1024 / 1024


def wait_for_resize(ax, vol, sizeMb):
    wait_for_no_action(ax, vol, 'Resize')
    curSizeMb = get_lv_size_mb(get_lv_path(ax, vol))
    if curSizeMb != sizeMb:
        raise Exception('wait_for_resize:failed', ax, vol, sizeMb, curSizeMb)


def resize_archive(ax, vol, sizeMb, doZeroClear):
    st = get_state(ax, vol)
    if st == 'Clear':
        return
    elif st in aAcceptForResize:
        args = ['resize', vol, str(sizeMb) + 'm']
        if doZeroClear:
            args += ['zeroclear']
        run_ctl(ax, args)
        wait_for_resize(ax, vol, sizeMb)
    else:
        raise Exception('resize_archive:bad state', st)


def resize_storage(sx, vol, sizeMb):
    st = get_state(sx, vol)
    if st == 'Clear':
        return
    else:
        run_ctl(sx, ['resize', vol, str(sizeMb) + 'm'])


def resize(vol, sizeMb, doZeroClear):
    for ax in cfg.archiveL:
        resize_archive(ax, vol, sizeMb, doZeroClear)
    for sx in cfg.storageL:
        resize_storage(sx, vol, sizeMb)


def wait_for_log_empty(wdev, timeoutS=TIMEOUT_SEC):
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
        time.sleep(1)
    raise Exception("wait_for_log_empty", wdev)


def get_walb_dev_sizeMb(wdev):
    sysName = '/sys/block/walb!%d/size' % wdev.iD
    with open(sysName, 'r') as f:
        size = int(f.read().strip()) * 512 / 1024 / 1024
    return size


def write_over_wldev(wdev, overflow=False):
    wldevSizeLb = get_lv_size_mb(wdev.log) * 1024 * 1024 / 512
    wdevSizeLb = get_walb_dev_sizeMb(wdev) * 1024 * 1024 / 512
    print "wldevSizeLb, wdevSizeLb", wldevSizeLb, wdevSizeLb
    # write a little bigger size than wldevSizeLb
    remainLb = wldevSizeLb + 4
    writeMaxLb = 4 * 1024 * 1024 / 512
    while remainLb > 0:
        writeLb = min(remainLb, writeMaxLb)
        print 'writing %d MiB' % (writeLb * 512 / 1024 / 1024)
        write_random(wdev.path, writeLb)
        if not overflow:
            wait_for_log_empty(wdev)
        remainLb -= writeLb
