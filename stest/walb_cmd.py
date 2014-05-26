import collections
import os
import subprocess
import sys
import time
import socket
import errno

Server = collections.namedtuple('Server', 'name port vg')
Config = collections.namedtuple('Config', 'debug binDir dataDir storageL proxyL archiveL')

cfg = None

def setConfig(config):
    if config.binDir[0] != '/' or config.binDir[-1] != '/':
        raise Exception("binDir must abs path", config.binDir)
    if config.dataDir[0] != '/' or config.dataDir[-1] != '/':
        raise Exception("dataDir must abs path", config.dataDir)
    global cfg
    cfg = config

def mkdirP(pathStr):
    if not os.path.exists(pathStr):
        os.makedirs(pathStr)

def toStr(ss):
    return " ".join(ss)

def getDebugOpt():
    if cfg.debug:
        return ["-debug"]
    else:
        return []

def getHostPort(s):
    return "localhost" + ":" + s.port

def getServerArgs(server):
    if server in cfg.storageL:
        ret = [cfg.binDir + "storage-server",
               "-archive", getHostPort(cfg.archiveL[0]),
               "-proxy", ",".join(map(getHostPort, cfg.proxyL))]

    elif server in cfg.proxyL:
        ret = [cfg.binDir + "proxy-server"]

    elif server in cfg.archiveL:
        ret = [cfg.binDir + "archive-server", "-vg", server.vg]
    else:
        raise Exception("Server name %s is not found in all lists" % server.name)
    ret += ["-p", server.port,
            "-b", cfg.dataDir + server.name,
            "-l", server.name + ".log",
            "-id", server.name] + getDebugOpt()
    return ret

def runCommand(args):
    if cfg.debug:
        print "runCommand:", toStr(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=sys.stderr)
    f = p.stdout
    s = f.read().strip()
    ret = p.wait()
    if ret != 0:
        raise Exception("command error %d\n" % ret)
    return s

def runCtl(server, cmdArgs):
    ctlArgs = [cfg.binDir + "/controller",
            "-id", "ctrl",
            "-a", "localhost",
            "-p", server.port] + getDebugOpt()
    return runCommand(ctlArgs + cmdArgs)

def runDaemon(args):
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

def waitForServerPort(server):
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
#    return runCtl(server, ["host-type"])

def getState(server, vol):
    return runCtl(server, ["get-state", vol])

##################################################################
# user command functions

def wait_for_state(server, vol, state, timeoutS = 10):
    for c in xrange(0, timeoutS):
        st = getState(server, vol)
        print "c=", server, vol, state, c, st
        if st == state:
            return
        time.sleep(1)
    raise Exception("wait_for_state", server, vol, state)

def kill_all_servers():
    for s in ["storage-server", "proxy-server", "archive-server"]:
        subprocess.Popen(["/usr/bin/killall", "-9"] + [s]).wait()

def startup(server):
    mkdirP(cfg.dataDir + server.name)
    args = getServerArgs(server)
    if cfg.debug:
        print 'cmd=', args
    runDaemon(args)
    waitForServerPort(server)

def startup_all():
    for s in cfg.archiveL + cfg.proxyL + cfg.storageL:
        startup(s)

def shutdown(server, mode="graceful"):
    runCtl(server, ["shutdown", mode])

def shutdown_all():
    for s in cfg.storageL + cfg.proxyL + cfg.archiveL:
        shutdown(s)

def init(sx, vol, wdevPath):
    runCtl(sx, ["init-vol", vol, wdevPath])
    runCtl(sx, ["start", vol, "slave"])
    wait_for_state(sx, vol, "Slave")
    runCtl(cfg.archiveL[0], ["init-vol", vol])

def is_synchronizing(ax, vol):
    px = cfg.proxyL[0]
    st = runCtl(px, ["archive-info", "list", vol])
    return ax in st.split()

# stop s vol and wait until state is waitState
def stop(s, vol, waitState, mode = "graceful"):
    runCtl(s, ["stop", vol, mode])
    wait_for_state(s, vol, waitState)

def start(s, vol, waitState):
    runCtl(s, ["start", vol])
    wait_for_state(s, vol, waitState)

def stop_sync(ax, vol):
    for px in cfg.proxyL:
        stop(px, vol, "Stopped")
        runCtl(px, ["archive-info", "del", vol, ax.name])
        start(px, vol, "Started")

def get_gid_list(ax, vol, cmd):
    if not cmd in ['list-restorable', 'list-restored']:
        raise Exception('get_list_gid : bad cmd', cmd)
    ret = runCtl(ax, [cmd, vol])
    return map(int, ret.split())

def list_restorable(ax, vol):
    ret = runCtl(ax, ["list-restorable", vol])
    return map(int, ret.split())

def list_restorable(ax, vol):
    ret = runCtl(ax, ["list-restored", vol])
    return map(int, ret.split())

def wait_for_restorable_any(ax, vol, timeoutS = 0x7ffffff):
    for c in xrange(0, timeoutS):
        gids = get_gid_list(ax, vol, 'list-restorable')
        if gids:
            return gids[-1]
        time.sleep(1)
    return -1

def wait_for_gid(ax, vol, gid, cmd, timeoutS = 0x7ffffff):
    for c in xrange(0, timeoutS):
        gids = get_gid_list(ax, vol, cmd)
        if gid in gids:
            return True
        time.sleep(1)
    return False

def wait_for_restorable(ax, vol, gid, timeoutS = 0x7ffffff):
    return wait_for_gid(ax, vol, gid, 'list-restorable', timeoutS)

def wait_for_restored(ax, vol, gid, timeoutS = 0x7fffffff):
    return wait_for_gid(ax, vol, gid, 'list-restored', timeoutS)

def add_archive_to_proxy(px, vol, ax):
    st = getState(px, vol)
    if st == "Started":
        stop(px, vol, "Stopped")
    runCtl(px, ["archive-info", "add", vol, ax.name, getHostPort(ax)])
    start(px, vol, "Started")

def full_backup(sx, vol):
    a0 = cfg.archiveL[0]
    st = getState(sx, vol)
    if st == "Slave":
        stop(sx, vol, "SyncReady")

    ret = runCtl(sx, ["is-overflow", vol])
    if ret != "0":
        runCtl(sx, ["reset-vol", vol])

    for s in cfg.storageL:
        if s == sx:
            continue
        st = getState(s, vol)
        if st != "Slave" and st != "Clear":
            raise Exception("full_backup : bad state", s.name, vol, st)

    for ax in cfg.archiveL[1:]:
        if is_synchronizing(ax, vol):
            stop_sync(ax, vol)

    for px in cfg.proxyL:
        add_archive_to_proxy(px, vol, a0)
    runCtl(sx, ["full-bkp", vol])
    wait_for_state(a0, vol, "Archived", 10)

    gid = wait_for_restorable_any(a0, vol)
    if gid == -1:
        raise Exception("full_backup : bad gid", s.name, vol)
    return gid

def writeRandom(devName, size):
    args = [cfg.binDir + "/write_random_data",
        '-s', str(size), devName]
    return runCommand(args)

def getSha1(devName):
    return runCommand(['/usr/bin/sha1sum', devName])

def restore(ax, vol, gid):
    runCtl(ax, ['restore', vol, str(gid)])
    wait_for_restored(ax, vol, gid)

def getRestoredPath(ax, vol, gid):
    return '/dev/' + ax.vg + '/r_' + vol + '_' + str(gid)

