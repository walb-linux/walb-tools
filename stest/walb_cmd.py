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

def getHostPort(server):
    return "localhost" + ":" + server.port

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
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=sys.stderr)
    f = p.stdout
    s = f.read().strip()
    ret = p.wait()
    if ret != 0:
#        raise Exception(("command error %d args: %s\n" % (ret, toStr(args))))
        raise Exception("command error ", ret, args)
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
    for p in cfg.proxyL:
        runCtl(p, ["init-vol", vol])
    runCtl(cfg.archiveL[0], ["init-vol", vol])

def is_synchronizing(ax, vol):
    p = cfg.proxyL[0]
    st = runCtl(p, ["archive-info", "list", vol])
    return ax in st.split()

def stop(s, vol, mode = "graceful"):
    runCtl(s, ["stop", vol, mode])

def start(s, vol):
    runCtl(s, ["start", vol])

def stop_sync(ax, vol):
    for p in cfg.proxyL:
        stop(p, vol)
        runCtl(p, ["archive-info", "del", vol, ax.name])
        start(p, vol)

def list_restorable(ax, vol):
    ret = runCtl(ax, ["list-restorable", vol])
    return map(int, ret.split())

def wait_for_restorable_any(ax, vol, timeoutS = 0x7ffffff):
    for c in xrange(0, timeoutS):
        gids = list_restorable(ax, vol)
        if gids:
            return gids[-1]
        time.sleep(1)
    return -1

def wait_for_restorable(ax, vol, gid, timeoutS = 0x7ffffff):
    for c in xrange(0, timeoutS):
        gids = list_restorable(ax, vol)
        if gid in gids:
            return True
        time.sleep(1)
    return False

def full_bkp(sx, vol):
    st = getState(sx, vol)
    if st == "Slave":
        runCtl(sx, ["stop", vol])

    ret = runCtl(sx, ["is-overflow", vol])
    if ret != "0":
        runCtl(sx, ["reset-vol", vol])

    for s in cfg.storageL:
        if s == sx:
            continue
        st = getState(s, vol)
        if st != "Slave":
            raise Exception("full_bkp : bad state", s.name, vol, st)

    for a in cfg.archiveL[1:]:
        if is_synchronizing(a, vol):
            stop_sync(a, vol)

    runCtl(sx, ["full-bkp", vol])

    gid = wait_for_restorable_any(cfg.archiveL[0], vol)
    if gid == -1:
        raise Exception("full_bkp : bad gid", s.name, vol)
    return gid


