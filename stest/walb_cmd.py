import collections
import os
import subprocess
import sys
import time

Server = collections.namedtuple('Server', 'name port vg')
Config = collections.namedtuple('Config', 'debug binDir baseDir storageL proxyL archiveL')

cfg = None

def setConfig(config):
    global cfg
    cfg = config

"""
def getDebugOpt():
    if cfg.debug:
        return ["-debug"]
    else:
        return []

def getExePath(exeName):
    return "/".join([cfg.binDir, execName])

def getCtrlArgs(server):
    return ["/".join([BinDir, "controller"])] \
        + ["-id", "ctrl"] \
        + ["-a", "localhost"] \
        + ["-p", server.port] \
        + getDebugOpt()

import config
import subprocess
import run
import socket
import time
import errno
import os

def waitForServerPort(name):
    address = "localhost"
    port = int(cfg.PortM[name])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    for _ in range(1, 10):
        try:
            sock.connect((address, port))
            return
        except socket.error, e:
            if e.errno not in [errno.ECONNREFUSED, errno.ECONNABORTED]:
                raise
        time.sleep(0.1)

def runCtrl(name, cmdArgs):
    return run.runCommand(cfg.getCtrlArgs(name) + cmdArgs)

def hostType(server):
    return runCtrl(server, ["host-type"])

def shutdown(name, mode="graceful"):
    runCtrl(name, ["shutdown", mode])

def runCommand(args):
    p = subprocess.Popen(args, stdout=sys.stdout, stderr=sys.stderr)
    ret = p.wait()
    if ret != 0:
        raise Exception(("command error %d\n" % ret)
                        + "args: " + " ".join(args))

"""

def getBaseDir(server):
    return cfg.baseDir + "/" + server.name

def mkdirP(pathStr):
    if not os.path.exists(pathStr):
        os.makedirs(pathStr)

def getServerCommonArgs(server):
    return ["-p", server.port,
            "-b", getBaseDir(server),
            "-l", server.name + ".log",
            "-id", server.name]

def getHostPort(server):
    return "localhost" + ":" + server.port

def getServerArgs(server):
    if server in cfg.storageL:
        ret = [cfg.binDir + "/storage-server",
               "-archive", getHostPort(cfg.archiveL[0]),
               "-proxy", ",".join(map(getHostPort, cfg.proxyL))]
    elif server in cfg.proxyL:
        ret = [cfg.binDir + "/proxy-server"]
    elif server in cfg.archiveL:
        ret = [cfg.binDir + "/archive-server",
               "-vg", server.vg]
    else:
        raise Exception("Server name %s is not found in all lists" % server.name)
    ret += getServerCommonArgs(server)
    return ret

def runDaemon(runnable):
    try:
        pid = os.fork()
        if pid > 0:
            return
    except OSError, e:
        print >>sys.stderr, "fork#1 failed (%d) (%s)" % (e.errno, e.strerror)

    os.chdir("/")
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        print >>sys.stderr, "fork#1 failed (%d) (%s)" % (e.errno, e.strerror)
        sys.exit(1)

    sys.stdin = open('/dev/null', 'r')
    sys.stdout = open('/dev/null', 'w')
    sys.stderr = open('/dev/null', 'w')

    runnable()

def runServerDaemon(server):
    mkdirP(getBaseDir(server))
    args = getServerArgs(server)
    def runnable():
        subprocess.Popen(args).wait()
    runDaemon(runnable)
    time.sleep(1)

