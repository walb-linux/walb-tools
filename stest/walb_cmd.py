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
        raise Exception("command error %d args: %s\n" % (ret, toStr(args)))
    return s

def runCtrl(server, cmdArgs):
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
#    return runCtrl(server, ["host-type"])

##################################################################
# user command functions

def startup(server):
    mkdirP(cfg.dataDir + server.name)
    args = getServerArgs(server)
    if cfg.debug:
        print 'cmd=', ' '.join(args)
    runDaemon(args)
    waitForServerPort(server)

def startup_all():
    for s in cfg.archiveL + cfg.proxyL + cfg.storageL:
        startup(s)

def shutdown(server, mode="graceful"):
    runCtrl(server, ["shutdown", mode])

def shutdown_all():
    for s in cfg.storageL + cfg.proxyL + cfg.archiveL:
        shutdown(s)

def init(sx, vol, wdevPath):
    runCtrl(sx, ["init-vol", vol, wdevPath])
    runCtrl(sx, ["k
