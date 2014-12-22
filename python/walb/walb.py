#!/usr/bin/env python

import os
import time
import subprocess
import sys
import socket
from contextlib import closing
import errno
import re
import datetime

########################################
# Constants for general purpose.
########################################

TIMEOUT_SEC = 86400
SHORT_TIMEOUT_SEC = 100

Mebi = (1 << 20)  # mebi.
Lbs = (1 << 9)  # logical block size

UINT64_MAX = (1 << 64) - 1


########################################
# Verification functions.
########################################

def verify_type(obj, typeValue, elemType=None):
    '''
    obj       - object.
    typeValue - type like int, str, list.
    elemType  - specify type of elements if typeValue is sequence.

    '''
    if not isinstance(obj, typeValue):
        raise Exception('invalid type', type(obj), typeValue)
    if isinstance(obj, list) and elemType:
        if not all(isinstance(e, elemType) for e in obj):
            raise Exception('invalid list type', type(obj), typeValue, elemType)


def verify_int(obj):
    '''
    obj -- object to verify.
    '''
    if not isinstance(obj, int) and not isinstance(obj, long):
        raise Exception('invalid type', type(obj))


def verify_u64(obj):
    '''
    obj -- object to verify.
    '''
    verify_int(obj)
    if obj < 0 or obj >= 2**64:
        raise Exception('cannot be unsigned 64bit integer', obj)


def verify_function(obj):
    '''
    obj - function object.

    '''
    def f():
        pass
    if type(obj) != type(f):
        raise Exception('not function type', type(obj))


def verify_gid_range(gidB, gidE, msg):
    '''
    gidB - begin gid.
    gidE - end gid.
    msg :: str - message for error.
    '''
    verify_u64(gidB)
    verify_u64(gidE)
    if gidB > gidE:
        raise Exception(msg, 'bad gid range', gidB, gidE)


def verify_size_unit(sizeU):
    '''
    sizeU - Normal non-negative integer with optional unit suffix.
      unit suffix must be one of
      'k', 'm', 'g', 't', 'p',
      'K', 'M', 'G', 'T', 'P'.
    '''
    verify_type(sizeU, str)
    if re.match('[0-9]+(?:[kKmMgGtTpP])?', sizeU) is None:
        raise Exception('bad size unit', sizeU)


########################################
# Utility functions.
########################################

'''
RunCommand type is function with arguments type (args, isDebug).
  args :: [str]   - command line arguments.
  isDebug :: bool - True to put debug messages.
  return :: str   - stdout of the command.

'''


def make_dir(pathStr):
    if not os.path.exists(pathStr):
        os.makedirs(pathStr)


def to_str(ss):
    return " ".join(ss)


def run_local_command(args, putMsg=False):
    '''
    run a command at localhost.
    args :: [str] - command line arguments.
                    The head item must be full-path executable.
    putMsg :: bool - put debug message.
    return :: str  - standard output of the command.
    '''
    verify_type(args, list, str)
    verify_type(putMsg, bool)

    if putMsg:
        print "run_command:", to_str(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                         stderr=sys.stderr, close_fds=True)
    f = p.stdout
    s = f.read().strip()
    ret = p.wait()
    if ret != 0:
        raise Exception("command error %s %d\n" % (args, ret))
    if putMsg:
        print "run_command_result:", s
    return s


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
    sys.stdout.flush()
    sys.stderr.flush()
    stdin = open('/dev/null', 'r')
    stdout = open('/dev/null', 'a+')
    stderr = open('/dev/null', 'a+')
    os.dup2(stdin.fileno(), sys.stdin.fileno())
    os.dup2(stdout.fileno(), sys.stdout.fileno())
    os.dup2(stderr.fileno(), sys.stderr.fileno())

    subprocess.Popen(args, close_fds=True)  # no need to wait for the subprocess.
    os._exit(0)


def wait_for_server_port(address, port, timeoutS=10):
    '''
    Wait for server port accepts connections.
    address :: str  - address or host name.
    port :: int     - port number.
    timeoutS :: int - timeout [sec].
    '''
    verify_type(address, str)
    verify_type(port, int)
    verify_type(timeoutS, int)
    t0 = time.time()
    while time.time() < t0 + timeoutS:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        with closing(sock):
            try:
                sock.connect((address, port))
                sock.close()
                return
            except socket.error, e:
                if e.errno not in [errno.ECONNREFUSED,
                                   errno.ECONNABORTED, errno.ECONNRESET]:
                    raise
                print 'wait_for_server_port:ignored', \
                    address, port, e.errno, os.strerror(e.errno)
        time.sleep(0.3)
    raise Exception('wait_for_server_port:timeout', address, port, timeoutS)


def flush_bufs(bdevPath, runCommand=run_local_command):
    '''
    Flush buffer of a block device.
    bdevPath :: str - block device path.
    runCommand :: RunCommand
    '''
    verify_type(bdevPath, str)
    verify_function(runCommand)
    runCommand(['/sbin/blockdev', '--flushbufs', bdevPath])


def zero_clear(bdevPath, offsetLb, sizeLb, runCommand=run_local_command):
    '''
    Zero-clear a block device.
    bdevPath :: str - block device path.
    offsetLb :: int - offset [logical block].
    sizeLb :: int  - size [logical block].
    runCommand :: RunCommand
    '''
    verify_type(bdevPath, str)
    verify_u64(offsetLb)
    verify_u64(sizeLb)
    runCommand(['/bin/dd', 'if=/dev/zero', 'of=' + bdevPath,
                'bs=512', 'seek=' + str(offsetLb), 'count=' + str(sizeLb),
                'conv=fdatasync'])


def exists_file(path, runCommand=run_local_command):
    '''
    Get file existance using shell.
    path :: str              - file path
    runCommand :: RunCommand
    return :: bool           - true if the file exists, or false.
    '''
    verify_type(path, str)
    cmd = 'if [ -b "%s" ]; ' \
        'then echo 1;' \
        'else echo 0; fi' % path
    res = runCommand(['/bin/sh', '-c', cmd])
    return int(res) != 0


########################################
# Lvm utility functions.
########################################

def resize_lv(lvPath, curSizeMb, newSizeMb, doZeroClear,
              runCommand=run_local_command):
    """
    Resize a logical volume.
      This command support shrink also.
    lvPath :: str       - lvm lv path.
    curSizeMb :: int    - current device size [MiB].
    newSizeMb :: int    - new device size [MiB].
    doZeroClear :: bool - True to zero-clear the extended area.
    runCommand :: RunCommand
    """
    verify_type(lvPath, str)
    verify_u64(curSizeMb)
    verify_u64(newSizeMb)
    verify_type(doZeroClear, bool)
    verify_function(runCommand)

    if curSizeMb == newSizeMb:
        return
    runCommand(['/sbin/lvresize', '-f', '-L', str(newSizeMb) + 'm', lvPath])
    # zero-clear is required for test only.
    if curSizeMb < newSizeMb and doZeroClear:
        zero_clear(lvPath, curSizeMb * 1024 * 1024 / 512,
                   (newSizeMb - curSizeMb) * 1024 * 1024 / 512,
                   runCommand)
    wait_for_lv_ready(lvPath, runCommand)


def remove_lv(lvPath, runCommand=run_local_command):
    '''
    Remove a logical volume.
    runCommand :: RunCommand
    lvPath :: str - lvm lv path.
    '''
    wait_for_lv_ready(lvPath, runCommand)
    retryTimes = 3
    for i in xrange(retryTimes):
        try:
            if i != 0 and not exists_file(lvPath, runCommand):
                return
            runCommand(['/sbin/lvremove', '-f', lvPath])
            return
        except:
            print 'remove_lv failed', i, lvPath
            time.sleep(1)
    else:
        raise Exception('remove_lv:timeout', lvPath)


def get_lv_size(lvPath, runCommand=run_local_command):
    '''
    Get lv size.
    lvPath :: str - lvm lv path.
    runCommand :: RunCommand
    return :: device size [byte].
    '''
    verify_type(lvPath, str)
    verify_function(runCommand)

    ret = runCommand(['/sbin/lvdisplay', '-C', '--noheadings', '-o', 'lv_size', '--units', 'b', lvPath])
    ret.strip()
    if ret[-1] != 'B':
        raise Exception('get_lv_size_mb: bad return value', ret)
    return int(ret[0:-1])


def get_lv_size_mb(lvPath, runCommand=run_local_command):
    '''
    Get lv size.
    lvPath :: str - lvm lv path.
    runCommand :: RunCommand
    return :: device size [MiB].
    '''
    sizeB = get_lv_size(lvPath, runCommand)
    if sizeB % Mebi != 0:
        raise Exception('get_lv_size_mb: not multiple of 1MiB.', sizeB)
    return sizeB / Mebi


def wait_for_lv_ready(lvPath, runCommand=run_local_command):
    '''
    lvPath :: str            - lvm device path.
    runCommand :: RunCommand
    '''
    flush_bufs(lvPath, runCommand)


########################################
# Constants for walb.
########################################

# Server kinds.

K_STORAGE = 0
K_PROXY = 1
K_ARCHIVE = 2

serverKinds = [K_STORAGE, K_PROXY, K_ARCHIVE]


def server_kind_to_str(kind):
    '''
    kind :: int   - K_XXX.
    return :: str - server kind representation.
    '''
    m = {K_STORAGE: 'storage', K_PROXY: 'proxy', K_ARCHIVE: 'archive'}
    return m[kind]


# storage steady states
sClear = "Clear"
sSyncReady = "SyncReady"
sStopped = "Stopped"
sTarget = "Target"
sStandby = "Standby"

# storage temporary states
stInitVol = "InitVol"
stClearVol = "ClearVol"
stStartStandby = "StartStandby"
stStopStandby = "StopStandby"
stFullSync = "FullSync"
stHashSync = "HashSync"
stStartTarget = "StartTarget"
stStopTarget = "StopTarget"
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

sDuringFullSync = [stFullSync, sStopped, stStartTarget]
sDuringHashSync = [stHashSync, sStopped, stStartTarget]
sDuringStopForTarget = [sTarget, stStopTarget]
sDuringStopForStandby = [sStandby, stStopStandby]
pActive = [pStarted, ptWlogRecv]
pDuringStop = [pStarted, ptWlogRecv, ptStop, ptWaitForEmpty]
aActive = [aArchived, atWdiffRecv, atHashSync, atReplSync]
aAcceptForResize = aActive + [aStopped]
aAcceptForClearVol = [aStopped, aSyncReady]
aDuringReplicate = [atReplSync, atFullSync]
aDuringStop = aActive + [atStop]
aDuringFullSync = [atFullSync]
aDuringHashSync = [atHashSync]


# Compression parameters.

CMPR_NONE = 0
CMPR_SNAPPY = 1
CMPR_GZIP = 2
CMPR_LZMA = 3


def printL(ls):
    for x in ls:
        print x

def verify_compress_kind(kind):
    '''
    kind :: int - CMPR_XXX.
    return :: None
    '''
    if kind not in [CMPR_NONE, CMPR_SNAPPY, CMPR_GZIP, CMPR_LZMA]:
        raise Exception('verify_compress_kind: bad value', kind)


def compress_kind_to_str(kind):
    '''
    kind :: int   - CMPR_XXX.
    return :: str - string representation.
    '''
    verify_compress_kind(kind)
    m = {CMPR_NONE: 'none', CMPR_SNAPPY: 'snappy', CMPR_GZIP: 'gzip', CMPR_LZMA: 'lzma'}
    assert kind in m
    return m[kind]


def compress_str_to_kind(s):
    '''
    s :: str      - input string.
    return :: int - CMPR_XXX
    '''
    verify_type(s, str)
    m = {'none': CMPR_NONE, 'snappy': CMPR_SNAPPY, 'gzip': CMPR_GZIP, 'lzma': CMPR_LZMA}
    if s not in m:
        raise Exception('compress_str_to_kind: bad kind', s)
    return m[s]


def str_to_datetime(s):
    return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')


def datetime_to_str(ts):
    return ts.strftime('%Y-%m-%dT%H:%M:%S')


class Snapshot:
    '''
    Snapshot class
    '''
    def __init__(self, gidB=0, gidE=UINT64_MAX):
        '''
        gidB :: int
        gidE :: int
        '''
        verify_u64(gidB)
        verify_u64(gidE)
        self.gidB = gidB
        self.gidE = gidE
        verify_gid_range(gidB, gidE, "Snapshot:init")
    def __str__(self):
        if self.gidB == self.gidE:
            return "|%d|" % self.gidB
        else:
            return "|%d,%d|" % (self.gidB, self.gidE)


def create_snapshot_from_str(s):
    '''
    create snapshot from str
    s :: str such as |num| or |num1,num2|
    '''
    verify_type(s, str)
    if s[0] != '|' or s[-1] != '|':
        raise Exception('create_snapshot_from_str:bad format', s)
    pos = s.find(',')
    if pos >= 0:
        gidB = int(s[1:pos])
        gidE = int(s[pos+1:-1])
    else:
        gidB = gidE = int(s[1:-1])
    return Snapshot(gidB, gidE)


class Diff:
    '''
    DIff class
    '''
    def __init__(self, B=Snapshot(), E=Snapshot()):
        '''
        B :: Snapshot
        E :: Snapshot
        '''
        verify_type(B, Snapshot)
        verify_type(E, Snapshot)
        self.B = B
        self.E = E
        self.isMergeable = False
        self.isCompDiff = False
        self.ts = datetime.datetime.now()
        self.dataSize = 0
#        self.verify()

    def verify(self):
        if self.B.gidB >= self.E.gidB or self.B.gidE > self.E.gidE:
            raise Exception('Diff:bad progress', str(self.B), str(self.E))
    def __str__(self):
        if self.isMergeable:
            m = 'M'
        else:
            m = '-'
        if self.isCompDiff:
            c = 'C'
        else:
            c = '-'
        ts_str = datetime_to_str(self.ts)
        return "%s-->%s %s%s %s %d" % (self.B, self.E, m, c, ts_str, self.dataSize)


def create_diff_from_str(s):
    '''
    create diff from str
    s :: str
    '''
    verify_type(s, str)
    p = re.compile(r'(\|[^|]+\|)-->(\|[^|]+\|) ([M-])([C-]) ([^ ]+) (\d+)')
    m = p.match(s)
    if not m:
        raise Exception('create_diff_from_str:bad format', s)
    d = Diff()
    d.B = create_snapshot_from_str(m.group(1))
    d.E = create_snapshot_from_str(m.group(2))
    d.isMergeable = m.group(3) == 'M'
    d.isCompDiff = m.group(4) == 'C'
    d.ts = str_to_datetime(m.group(5))
    d.dataSize = int(m.group(6))
    return d


class GidInfo:
    '''
    gid information
    '''
    def __init__(self, s):
        '''
        s :: str such as '<gid> <datetime>'
        '''
        verify_type(s, str)
        p = s.split()
        if len(p) != 2:
            raise Exception('GidInfo:bad format', s)
        self.gid = int(p[0])
        self.ts = str_to_datetime(p[1])
    def __str__(self):
        return str(self.gid) + " " + datetime_to_str(self.ts)


class CompressOpt:
    '''
    Compress option for archive info and replication.
    '''
    def __init__(self, kind=CMPR_SNAPPY, level=0, nrCpu=1):
        '''
        kind :: int  - CMPR_XXX.
        level :: int - compression level. [0, 9].
        nrCpu :: int - number of CPUs. 1 or more.
        '''
        self.kind = kind
        self.level = level
        self.nrCpu = nrCpu
        self.verify()

    def verify(self):
        verify_compress_kind(self.kind)
        verify_type(self.level, int)
        if self.level < 0 or self.level > 9:
            raise Exception('CompressOpt: bad level', self.kind, self.level, self.nrCpu)
        verify_type(self.nrCpu, int)
        if self.nrCpu == 0:
            raise Exception('CompressOpt: bad nrCpu', self.kind, self.level, self.nrCpu)

    def __str__(self):
        return ':'.join([compress_kind_to_str(self.kind), str(self.level), str(self.nrCpu)])

    def parse(self, s):
        '''
        s :: str - string representation.
        return :: None
        '''
        v = s.split(':')
        if len(v) != 3:
            raise Exception('CompressOpt: parse failed', s)
        self.kind = compress_str_to_kind(v[0])
        self.level = int(v[1])
        self.nrCpu = int(v[2])
        self.verify()


# Synchronization option.

class SyncOpt:
    '''
    Synchronization option.
    This is used for optional arguments of archive-info and replicate command.
    '''
    def __init__(self, cmprOpt=CompressOpt(), delayS=0, maxWdiffMergeSizeU='1G', bulkSizeU='64K'):
        '''
        cmprOpt :: CompressOpt    - compression option.
        delayS :: int             - delay to forward diffs from proxy to archive [sec].
        maxWdiffMergeSizeU :: str - max wdiff merge size [byte].
                                    Unit suffix like '1G' is allowed.
        bulkSizeU :: str          - bulk size [byte]. can be like '64K'.
                                    Unit suffix like '64K' is allowed.
        '''
        verify_type(cmprOpt, CompressOpt)
        verify_type(delayS, int)
        verify_size_unit(maxWdiffMergeSizeU)
        verify_size_unit(bulkSizeU)
        self.cmprOpt = cmprOpt
        self.delayS = delayS
        self.maxWdiffMergeSizeU = maxWdiffMergeSizeU
        self.bulkSizeU = bulkSizeU

    def getArchiveInfoArgs(self):
        '''
        return :: [str]
        '''
        return [str(self.cmprOpt), str(self.delayS)]

    def getReplicateArgs(self):
        '''
        return :: [str]
        '''
        return [str(self.cmprOpt), self.maxWdiffMergeSizeU, self.bulkSizeU]


########################################
# Classes for walb controller.
########################################

class Device:
    '''
    Walb device.

    '''
    def __init__(self, iD, ldev, ddev, wdevcPath, runCommand=run_local_command):
        '''
        iD :: int               - walb device id.
        ldev :: str             - underlying log block device path.
        ddev :: str             - underlying data block device path.
        wdevcPath :: str        - wdevc path.
        runCommand:: RunCommand - function that run commands.
        '''
        verify_type(iD, int)
        verify_type(ldev, str)
        verify_type(ddev, str)
        verify_type(wdevcPath, str)
        verify_function(runCommand)
        self.iD = iD
        self.ldev = ldev
        self.ddev = ddev
        self.wdevcPath = wdevcPath
        self.runCommand = runCommand

    @property
    def path(self):
        return '/dev/walb/' + str(self.iD)

    def __str__(self):
        return ', '.join([str(self.iD), self.ldev, self.ddev, self.wdevcPath])

    def run_wdevc(self, cmdArgs):
        '''
        Run wdevc command.
        cmdArgs :: [str] - command line arguments.
        '''
        verify_type(cmdArgs, list, str)
        self.runCommand([self.wdevcPath] + cmdArgs)

    def exists(self):
        '''
        Check the walb device exists or not.
        return :: bool
        '''
        if 0:
            # This is local only.
            return os.path.exists(self.path)
        else:
            return exists_file(self.path, self.runCommand)

    def format_ldev(self):
        '''
        Format devices for a walb device.
        TODO: support format_ldev options.
        '''
        self.run_wdevc(['format-ldev', self.ldev, self.ddev])

    def create(self):
        '''
        Create a walb device.
        TODO: support create_wdev options.
        '''
        self.run_wdevc(['create-wdev', self.ldev, self.ddev, '-n', str(self.iD)])

    def delete(self):
        '''
        Delete a walb device.
        '''
        self.run_wdevc(['delete-wdev', self.path])

    def resize(self, newSizeMb):
        '''
        Resize a walb device.
        Underlying data device must be resized before calling this function.
        newSizeMb :: int - new size [MiB].
        '''
        newSizeLb = newSizeMb * Mebi / Lbs
        self.run_wdevc(['resize', self.path, str(newSizeLb)])

    def reset(self):
        '''
        Reset a walb device.
        This will remove all logs in the log device.
        You should call this to recover from log overflow.

        '''
        self.run_wdevc(['reset-wal', self.path])

    def get_size_lb(self):
        '''
        Get walb device size.
        This will read sysfs to get size.
        return :: int -- device size [logical block].
        '''
        path = self._get_sys_path() + 'size'
        return int(self.runCommand(['/bin/cat', path]).strip())

    def get_size_mb(self):
        '''
        Get walb device size.
        This will read sysfs to get size.
        return :: int -- device size [MiB].
        '''
        sizeB = self.get_size_lb() * Lbs
        if sizeB % Mebi != 0:
            raise Exception('get_size_mb: not multiple of MiB.', sizeB)
        return sizeB / Mebi

    def wait_for_log_empty(self, timeoutS=TIMEOUT_SEC):
        '''
        Wait for log device becomes empty.
        wdev :: Wdev    - walb device.
        timeoutS :: int - timeout [sec].
        '''
        verify_type(timeoutS, int)

        def create_key_value(ls):
            ret = []
            for s in ls:
                (k, v) = s.strip().split()
                ret.append((k, int(v)))
            return dict(ret)

        path = self._get_sys_path() + 'walb/lsids'
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            out = self.runCommand(['/bin/cat', path]).strip()
            kv = create_key_value(out.split('\n'))
            completed = kv['completed']
            oldest = kv['oldest']
            if completed == oldest:
                return
            print "wait_for_log_empty", oldest, completed
            time.sleep(0.3)
        raise Exception("wait_for_log_empty", self.path)

    '''
    private member functions.
    '''
    def _get_sys_path(self):
        return '/sys/block/walb!%d/' % self.iD


class Server:
    '''
    Server configuration.
    '''
    def __init__(self, name, address, port, kind, binDir, dataDir,
                 logPath=None, vg=None, tp=None):
        '''
        name :: str            - daemon identifier in the system.
        address :: str         - host name
        port :: int            - port number.
        kind :: int            - K_STORAGE, K_PROXY, or K_ARCHIVE.
        binDir :: str          - directory path containing server executable
                                 at the host.
        dataDir :: str         - persistent data directory path.
        logPath :: str or None - log path. None means default.
        vg :: str              - volume group name.
                                 This is required by archive server only.
        tp :: str              - thinpool name.
                                 This is optional and required by archive server only.
        '''
        verify_type(name, str)
        verify_type(address, str)
        verify_type(port, int)
        verify_type(kind, int)
        if kind not in serverKinds:
            raise Exception('Server: wrong server kind', kind)
        verify_type(binDir, str)
        verify_type(dataDir, str)
        if logPath is not None:
            verify_type(logPath, str)
        if kind == K_ARCHIVE or vg is not None:
            verify_type(vg, str)
        if kind == K_ARCHIVE and tp is not None:
            verify_type(tp, str)

        self.name = name
        self.address = address
        self.port = port
        self.kind = kind
        self.binDir = binDir
        self.dataDir = dataDir
        self.logPath = logPath
        if kind == K_ARCHIVE:
            self.vg = vg
            self.tp = tp

    def __str__(self):
        L = [self.name, self.address, str(self.port),
             server_kind_to_str(self.kind), self.binDir, self.dataDir,
             self.logPath]
        if self.kind == K_ARCHIVE:
            L.append(self.vg)
            if self.tp:
                L.append(self.tp)
        return ', '.join(L)

    def get_host_port(self):
        '''
        Get 'address:port' string.
        return :: str
        '''
        return self.address + ":" + str(self.port)


def verify_server_kind(s, kindL):
    '''
    s :: Server
    kindL :: [int]
    '''
    verify_type(s, Server)
    verify_type(kindL, list, int)
    if s.kind not in kindL:
        raise Exception('invalid server type', s.name, s.kind, kindL)


class ServerLayout:
    '''
    Server layout of a backup group.
    '''
    def __init__(self, storageL, proxyL, archiveL):
        '''
        storageL :: [Server] - storage server list.
        proxyL :: [Server]   - proxy server list.
                               Before items have high priority.
        archiveL :: [Server] - archive server list. The head is primary server.
        '''
        verify_type(storageL, list, Server)
        verify_type(proxyL, list, Server)
        verify_type(archiveL, list, Server)

        if len(storageL) == 0:
            raise Exception('server_layout: no storage server')
        if len(proxyL) == 0:
            raise Exception('server_layout: no proxy server')
        if len(archiveL) == 0:
            raise Exception('server_layout: no archive server')

        for kind, sL in [(K_STORAGE, storageL), (K_PROXY, proxyL), (K_ARCHIVE, archiveL)]:
            for s in sL:
                if s.kind != kind:
                    raise Exception('server_layout: invalid server kind:', server_kind_to_str(kind), str(s))

        self.storageL = storageL
        self.proxyL = proxyL
        self.archiveL = archiveL

    def __str__(self):
        return ', '.join(map(lambda s: s.name, self.get_all()))

    def get_primary_archive(self):
        '''
        return :: Server
        '''
        return self.archiveL[0]

    def get_all(self):
        '''
        return :: [Server]
        '''
        return self.storageL + self.proxyL + self.archiveL

    def replace(self, storageL=None, proxyL=None, archiveL=None):
        '''
        Make a copy replacing arguments which are not None.
        storageL :: [Server] or None - if None, storageL will not unchange.
        proxyL :: [Server] or None - if None, archiveL will not unchange.
        archiveL :: [Server] or None - if None, archiveL will not unchange.
        return :: ServerLayout
        '''
        sL = self.storageL
        if storageL:
            sL = storageL
        pL = self.proxyL
        if proxyL:
            pL = proxyL
        aL = self.archiveL
        if archiveL:
            aL = archiveL
        return ServerLayout(sL, pL, aL)

    def to_cmd_string(self):
        '''
        Make command strings to run server
        '''
        for s in self.get_all():
            print ' '.join(get_server_args(s, self))



def get_server_args(s, sLayout, isDebug=False, useRepeater=False):
    '''
    Get walb-tools server arguments.
    s :: Server     - server.
    useRepeater :: Bool - use repeater if True
    return :: [str] - argument list.
    '''
    verify_type(s, Server)
    verify_type(sLayout, ServerLayout)
    verify_type(isDebug, bool)
    verify_type(useRepeater, bool)

    if s.kind == K_STORAGE:
        proxies = ",".join(map(lambda p: p.get_host_port(), sLayout.proxyL))
        ret = [s.binDir + 'walb-storage',
               "-archive", sLayout.get_primary_archive().get_host_port(),
               "-proxy", proxies]
    elif s.kind == K_PROXY:
        ret = [s.binDir + 'walb-proxy']
    else:
        assert s.kind == K_ARCHIVE
        ret = [s.binDir + 'walb-archive', '-vg', s.vg]
        if s.tp is not None:
            ret += ['-tp', s.tp]

    if s.logPath:
        logPath = s.logPath
    else:
        logPath = s.dataDir + '/' + s.name + '.log'
    port = s.port
    if useRepeater:
        port += 10000
    ret += ["-p", str(port),
            "-b", s.dataDir,
            "-l", logPath,
            "-id", s.name]
    if isDebug:
        ret += ['-debug']
    return ret


class Controller:
    '''
    To handle all walb servers in a backup group.

    '''
    def __init__(self, controllerPath, sLayout, isDebug=False):
        '''
        controllerPath :: str   - walb controller executable path.
        sLayout :: ServerLayout - server layout.
        isDebug :: bool
        '''
        verify_type(controllerPath, str)
        verify_type(isDebug, bool)
        self.controllerPath = controllerPath
        self.isDebug = isDebug
        self.set_server_layout(sLayout)

    def __str__(self):
        return ', '.join([self.controllerPath,
                          '[' + str(self.sLayout) + ']', str(self.isDebug)])

    def set_server_layout(self, sLayout):
        '''
        sLayout :: ServerLayout - server layout.
        '''
        verify_type(sLayout, ServerLayout)
        self.sLayout = sLayout

    def run_ctl(self, s, cmdArgs, putMsg=False, timeoutS=0):
        '''
        Run walb-tools controller.
        s :: Server      - a server.
        cmdArgs :: [str] - command arguments.
        putMsg :: bool   - put debug message if True.
        timeoutS :: int  - timeout in sec. 0 means default.
        return :: str    - stdout of the control command.
        '''
        verify_type(s, Server)
        verify_type(cmdArgs, list, str)
        verify_type(putMsg, bool)
        ctlArgs = [self.controllerPath,
                   "-id", "ctl",
                   "-a", s.address,
                   "-p", str(s.port)]
        if self.isDebug:
            ctlArgs += ['-debug']
        if timeoutS:
            ctlArgs += ['-to', str(timeoutS)]
        return run_local_command(ctlArgs + cmdArgs, self.isDebug or putMsg)

    def run_remote_command(self, s, args, putMsg=False, timeoutS=0):
        '''
        Run arbitrary executable at a remote server running walb daemon.
        This will use walb-tools daemons to run commands.

        s :: Server    - server where you want to run command.
        args :: [str]  - command line arguments.
                         The head item must be a full-path executable.
        putMsg :: bool - put debug message if True.
        timeoutS :: int - set timeout sec.
        return :: str  - stdout of the command if the command returned 0.
        '''
        verify_type(s, Server)
        verify_type(args, list, str)
        verify_type(putMsg, bool)
        verify_type(timeoutS, int)
        cmdArgs = ['exec'] + args
        return self.run_ctl(s, cmdArgs, putMsg, timeoutS)

    def get_run_remote_command(self, s):
        '''
        Get run_command function for RPC.
        walbc :: Controller
        s :: Server
        return :: ([str],bool -> str)

        '''
        verify_type(s, Server)

        def func(args, putMsg=False):
            return self.run_remote_command(s, args, putMsg or self.isDebug)

        return func

    def get_host_type(self, s):
        '''
        Get host type.
        s :: Server
        return :: str - 'storage', 'proxy', or 'archive'.
        '''
        verify_type(s, Server)
        return self.run_ctl(s, ['get', 'host-type'])

    def get_state(self, s, vol):
        '''
        Get state of a volume.
        s :: Server
        vol :: str    - volume name.
        return :: str - state.
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        return self.run_ctl(s, ['get', 'state', vol])

    def get_state_all(self, vol):
        '''
        Get all state fo a volume.
        vol :: str - volume name
        '''
        verify_type(vol, str)
        for s in self.sLayout.get_all():
            msg = "%s %s:%d %s" % (s.name, s.address, s.port, server_kind_to_str(s.kind))
            try:
                st= self.get_state(s, vol)
            except:
                st= "err"
            print msg, st

    def verify_state(self, s, vol, state):
        '''
        s :: Server
        vol :: str
        state :: str
        '''
        verify_type(state, str)
        st = self.get_state(s, vol)
        if st != state:
            raise Exception('verify_state: differ', s.name, st, state)

    def get_diff_list(self, ax, vol, gid0=0, gid1=UINT64_MAX):
        '''
        Get wdiff list.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid0 :: int     - range begin.
        gid1 :: int     - range end.
        return :: [Diff] - wdiff information list managed by the archive server.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid0)
        verify_u64(gid1)
        ls = self.run_ctl(ax, ['get', 'diff', vol, str(gid0), str(gid1)])
        return map(create_diff_from_str, ls.split('\n'))

    def print_diff_list(self, ax, vol, gid0=0, gid1=UINT64_MAX):
        '''
        Print wdiff list.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid0 :: int     - range begin.
        gid1 :: int     - range end.
        '''
        printL(self.get_diff_list(ax, vol, gid0, gid1))

    def get_total_diff_size(self, ax, vol, gid0=0, gid1=UINT64_MAX):
        '''
        Get total wdiff size.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid0 :: int     - range begin.
        gid1 :: int     - range end.
        return :: int - total size [byte].
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid0)
        verify_u64(gid1)
        ret = self.run_ctl(ax, ['get', 'total-diff-size', vol, str(gid0), str(gid1)])
        return int(ret)

    def get_num_action(self, s, vol, action):
        '''
        Get number of an action running for a volume.
        s :: Server    - server.
        vol :: str     - volume name.
        action :: str  - action name.
        return :: int  - the number of running
                         the specified action on the volume.
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        verify_type(action, str)
        num = int(self.run_ctl(s, ['get', 'num-action', vol, action]))
        return num

    def reset(self, s, vol, timeoutS=SHORT_TIMEOUT_SEC):
        '''
        Reset a volume.
        s :: Server - storage or archive.
        timeoutS :: int - timeout in sec. 0 means default.
        vol :: str  - volume name.
        '''
        verify_type(s, Server)
        verify_type(vol, str)
        verify_server_kind(s, [K_STORAGE, K_ARCHIVE])
        if s.kind == K_STORAGE:
            state = sSyncReady
        else:
            assert s.kind == K_ARCHIVE
            state = aSyncReady
        self.run_ctl(s, ["reset-vol", vol], timeoutS=timeoutS)  # this is synchronous command.
        self.verify_state(s, vol, state)

    def go_standby(self, sx, vol):
        '''
        change state in sx to Standby from anywhere
        sx :: Server - storage server.
        vol :: str   - volume name.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        state = self.get_state(sx, vol)
        if state == sStandby:
            return
        if state == sSyncReady:
            self._go_standby(sx, vol)
            return
        if state == sTarget:
            self.stop(sx, vol)
            self.stop_synchronizing(self.sLayout.get_primary_archive(), vol)
            self.reset(sx, vol)
            self._go_standby(sx, vol)
            return
        raise Exception('go_standby:bad state', sx.name, vol, state)

    def _go_standby(self, sx, vol):
        '''
        change state in sx from SyncReady to Standby

        sx :: Server - storage server.
        vol :: str   - volume name.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        state = self.get_state(sx, vol)
        if state != sSyncReady:
            raise Exception('_go_standby:bad state', sx.name, vol, state)
        self.run_ctl(sx, ['start', vol, 'standby']) # QQQ
        self._wait_for_state_change(sx, vol, [stStartStandby], [sStandby])

    def kick_all(self, sL):
        '''
        Kick all servers.
        sL :: [Server] - list of servers each of which
                         must be storage or proxy.
        '''
        verify_type(sL, list, Server)
        for s in sL:
            verify_server_kind(s, [K_STORAGE, K_PROXY])
            self.run_ctl(s, ["kick"])

    def kick_storage_all(self):
        ''' Kick all storage servers. '''
        self.kick_all(self.sLayout.storageL)

    def is_overflow(self, sx, vol):
        '''
        Check a storage is overflow or not.
        sx :: Server   - storage server.
        vol :: str     - volume name.
        return :: bool - True if the storage overflows.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        args = ['get', 'is-overflow', vol]
        ret = self.run_ctl(sx, args)
        return int(ret) != 0

    def verify_not_overflow(self, sx, vol):
        ''' Verify a volume does not overflow. '''
        if self.is_overflow(sx, vol):
            raise Exception('verify_not_overflow', sx.name, vol)

    def is_wdiff_send_error(self, px, vol, ax):
        '''
        Get wdiff-send-error value.
        px :: Server   - proxy.
        vol :: str     - volume name.
        ax :: Server   - archive.
        return :: bool
        '''
        verify_server_kind(px, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        args = ['get', 'is-wdiff-send-error', vol, ax.name]
        ret = self.run_ctl(px, args)
        return int(ret) != 0

    def get_uuid(self, s, vol):
        '''
        Get uuid string.
        s :: Server   - storage or archive.
        return :: str - uuid string that regex pattern [0-9a-f]{32}.
        '''
        verify_server_kind(s, [K_STORAGE, K_ARCHIVE])
        verify_type(vol, str)
        return self.run_ctl(s, ['get', 'uuid', vol])

    def status(self, sL=[], vol=None, timeoutS=SHORT_TIMEOUT_SEC):
        '''
        print server status.
        sL :: Server|[Server] - server or server list.
        vol :: str or None - volume name. None means all.
        '''
        if isinstance(sL, Server):
            sL = [sL]
        verify_type(sL, list, Server)
        if not sL:
            sL = self.sLayout.get_all()
        for s in sL:
            args = ['status']
            if vol:
                args.append(vol)
            print '++++++++++++++++++++', s.name, '++++++++++++++++++++'
            print self.run_ctl(s, args, timeoutS=timeoutS)

    def shutdown(self, s, mode="graceful"):
        '''
        Shutdown a server.
        s :: Server
        mode :: str - 'graceful' or 'force'.
        '''
        verify_type(s, Server)
        verify_type(mode, str)
        self._verify_shutdown_mode(mode, 'shutdown')
        self.run_ctl(s, ["shutdown", mode])

    def shutdown_list(self, sL, mode='graceful'):
        '''
        Shutdown listed servers.
        '''
        verify_type(sL, list, Server)
        self._verify_shutdown_mode(mode, 'shutdown_list')
        for s in sL:
            try:
                self.run_ctl(s, ["shutdown", mode])
                print 'shutdown succeeded', s.name
            except Exception, e:
                print 'shutdown failed', s.name, e

    def shutdown_all(self, mode='graceful'):
        '''
        Shutdown all servers.
        '''
        self.shutdown_list(self.sLayout.get_all())

    def get_alive_server(self):
        '''
        Get alive servers.
        return :: [str] - list of server name.
        '''
        ret = []
        for s in self.sLayout.get_all():
            try:
                self.get_host_type(s)
                ret.append(s.name)
            except:
                pass
        return ret

    def init_storage(self, sx, vol, wdevPath):
        '''
        Initialize a volume at storage.
        sx :: Server
        vol :: str
        wdevPath :: str
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        verify_type(wdevPath, str)
        self._init(sx, vol, wdevPath)
        self._go_standby(sx, vol)  # start as standby.

    def _init(self, s, vol, wdevPath=None):
        '''
        Call walb init-vol command.
        s :: Server             - storage or archive
        vol :: str              - volume name.
        wdevPath :: str or None - specify if s is storage.

        '''
        verify_server_kind(s, [K_STORAGE, K_ARCHIVE])
        verify_type(vol, str)
        cmdL = ['init-vol', vol]
        if s.kind == K_STORAGE:
            verify_type(wdevPath, str)
            cmdL.append(wdevPath)
        self.run_ctl(s, cmdL)

    def clear(self, s, vol):
        '''
        Clear a volume.
        s :: Server
        vol :: str
        '''
        verify_server_kind(s, serverKinds)
        verify_type(vol, str)
        st = self.get_state(s, vol)
        if s.kind == K_STORAGE:
            if st == sClear:
                return
            if st in [sTarget, sStandby]:
                self.stop(s, vol)
                st = self.get_state(s, vol)
            if st == sStopped:
                self.reset(s, vol)
                st = self.get_state(s, vol)
            if st != sSyncReady:
                raise Exception('clear', s.name, vol, st)
        elif s.kind == K_PROXY:
            if st == pClear:
                return
            if st in pActive:
                self.stop(s, vol)
                st = self.get_state(s, vol)
            if st != pStopped:
                raise Exception('clear', s.name, vol, st)
        else:
            assert s.kind == K_ARCHIVE
            if st == aClear:
                return
            if st == aArchived:
                self.stop(s, vol)
                st = self.get_state(s, vol)
            if st not in aAcceptForClearVol:
                raise Exception('clear', s.name, vol, st)
        self.run_ctl(s, ['clear-vol', vol])

    def _force_clear(self, vol):
        '''
        force to clear vol at all servers
        '''
        verify_type(vol, str)
        for s in self.sLayout.get_all():
            self.clear(s, vol)

    def get_archive_info_list(self, px, vol):
        '''
        Get archive list registered to a proxy.
        return :: [str] - list of archive name.
        '''
        verify_server_kind(px, [K_PROXY])
        verify_type(vol, str)
        st = self.run_ctl(px, ["archive-info", "list", vol])
        return st.split()

    def get_archive_info(self, px, vol, ax):
        '''
        Get archive info of an archive.
        return :: (str, SyncOpt) - hostPort, syncOpt.
        '''
        verify_server_kind(px, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        v = self.run_ctl(px, ['archive-info', 'get', vol, ax.name]).split()
        if len(v) != 3:
            raise Exception('get_archive_info: bad value', v)
        cmprOpt = CompressOpt()
        cmprOpt.parse(v[1])
        return v[0], SyncOpt(cmprOpt=cmprOpt, delayS=int(v[2]))

    def update_archive_info(self, px, vol, ax, syncOpt):
        '''
        Update archive information.
        return :: None
        '''
        verify_server_kind(px, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(syncOpt, SyncOpt)
        st = self.get_state(px, vol)
        if st != pStopped:
            raise Exception('update_archive_info: bad state', st, px.name, vol, ax.name, str(syncOpt))
        aL = self.get_archive_info_list(px, vol)
        if ax.name not in aL:
            raise Exception('update_archvie_info: not exists', px.name, vol, ax.name, str(syncOpt))
        args = ['archive-info', 'update', vol, ax.name, ax.get_host_port()]
        args += syncOpt.getArchiveInfoArgs()
        self.run_ctl(px, args)

    def is_synchronizing(self, ax, vol):
        '''
        Check whether a volume is synchronizing with an archive server or not.
        ax :: Server - archive server.
        vol :: str
        return :: bool
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        ret = []
        for px in self.sLayout.proxyL:
            ret.append(ax.name in self.get_archive_info_list(px, vol))
        v = sum(ret)
        if v == len(ret):  # all True
            return True
        elif v == 0:  # all False
            return False
        raise Exception('is_synchronizing: '
                        'some proxies are synchronizing, some are not.')

    def wait_for_stopped(self, s, vol, prevSt=None):
        '''
        Wait for a volue of a server stopped.
        s :: Server
        vol :: str
        prevSt :: None or str - specify if s is storage server.
        '''
        verify_server_kind(s, serverKinds)
        verify_type(vol, str)
        if prevSt:
            verify_type(prevSt, str)
        if s.kind == K_STORAGE:
            if not prevSt:
                raise Exception('wait_for_stopped: '
                                'prevSt not specified', s.name, vol)
            if prevSt == sStandby:
                tmpStL = sDuringStopForStandby
                goalSt = sSyncReady
            else:
                tmpStL = sDuringStopForTarget
                goalSt = sStopped
        elif s.kind == K_PROXY:
            tmpStL = pDuringStop
            goalSt = pStopped
        else:
            assert s.kind == K_ARCHIVE
            tmpStL = aDuringStop
            goalSt = aStopped
        self._wait_for_state_change(s, vol, tmpStL, [goalSt], TIMEOUT_SEC)

    def stop_nbk(self, s, vol, mode='graceful'):
        '''
        Stop a volume at a server.
        This is nonblocking command. See stop().
        s :: Server
        vol :: str
        mode :: str - 'graceful' or 'force' or 'empty'.
            'empty' is valid only if s is proxy.
        return :: str - state before running stop.
        '''
        verify_server_kind(s, serverKinds)
        verify_type(vol, str)
        if mode not in ['graceful', 'empty', 'force']:
            raise Exception('stop:bad mode', s.name, vol, mode)
        prevSt = self.get_state(s, vol)
        self.run_ctl(s, ["stop", vol, mode])
        return prevSt

    def stop(self, s, vol, mode='graceful'):
        '''
        Stop a volume at a server and wait for it stopped.
        '''
        prevSt = self.stop_nbk(s, vol, mode)
        self.wait_for_stopped(s, vol, prevSt)

    def start(self, s, vol):
        '''
        Start a volume at a server and wait for it started.
        s :: Server
        vol :: str
        '''
        verify_server_kind(s, serverKinds)
        verify_type(vol, str)
        if s.kind == K_STORAGE:
            self.run_ctl(s, ['start', vol, 'target']) # QQQ
            self._wait_for_state_change(s, vol, [stStartTarget], [sTarget])
        elif s.kind == K_PROXY:
            self.run_ctl(s, ['start', vol])
            self._wait_for_state_change(s, vol, [ptStart], pActive)
        else:
            #s.kind == K_ARCHIVE
            self.run_ctl(s, ['start', vol])
            self._wait_for_state_change(s, vol, [atStart], aActive)

    def del_archive_from_proxy(self, px, vol, ax):
        '''
        Delete an archive from a proxy.
        px :: Server - proxy server.
        vol :: str   - voume name.
        ax :: Server - archive server.
        '''
        verify_server_kind(px, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        st = self.get_state(px, vol)
        if st in pActive:
            self.stop(px, vol)
        aL = self.get_archive_info_list(px, vol)
        if ax.name in aL:
            self.run_ctl(px, ['archive-info', 'delete', vol, ax.name])
        st = self.get_state(px, vol)
        if st == pStopped:
            self.start(px, vol)

    def add_archive_to_proxy(self, px, vol, ax, doStart=True, syncOpt=None):
        '''
        Add an archive to a proxy.
        px :: Server    - proxy server.
        vol :: str      - volume name.
        ax :: server    - archive server.
        doStart :: bool - False if not to start proxy after adding.
        syncOpt :: SyncOpt or None - synchronization option.
        '''
        verify_server_kind(px, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(doStart, bool)
        if syncOpt:
            verify_type(syncOpt, SyncOpt)
        st = self.get_state(px, vol)
        if st in pActive:
            self.stop(px, vol)
        aL = self.get_archive_info_list(px, vol)
        cmd = 'update' if ax.name in aL else 'add'
        args = ['archive-info', cmd, vol, ax.name, ax.get_host_port()]
        if syncOpt:
            args += syncOpt.getArchiveInfoArgs()
        self.run_ctl(px, args)
        st = self.get_state(px, vol)
        if st == pStopped and doStart:
            self.start(px, vol)

    def get_server(self, name, sL):
        '''
        Get only one element from L having name
        name :: str   - server name.
        sL :: [Server] - server list.
        return :: Server
        '''
        verify_type(name, str)
        verify_type(sL, list, Server)
        ret = []
        for x in sL:
            if x.name == name:
                ret.append(x)
        if len(ret) != 1:
            raise Exception('get_server:not one', ret, name, map(lambda x:x.name, sL))
        return ret[0]

    def copy_archive_info(self, pSrc, vol, pDst):
        '''
        Copy archive info from a proxy to another proxy.
        pSrc :: Server - srouce proxy.
        vol :: str
        pDst :: Server - destination proxy. It must be stopped.
        '''
        verify_server_kind(pSrc, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(pDst, [K_PROXY])
        for axName in self.get_archive_info_list(pSrc, vol):
            ax = self.get_server(axName, self.sLayout.archiveL)
            _, syncOpt = self.get_archive_info(pSrc, vol, ax)
            self.add_archive_to_proxy(pDst, vol, ax, doStart=False, syncOpt=syncOpt)
        self.start(pDst, vol)

    def stop_synchronizing(self, ax, vol):
        '''
        Stop synchronization of a volume with an archive.
        ax :: Server - archive server.
        vol :: str   - volume name.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        for px in self.sLayout.proxyL:
            self.del_archive_from_proxy(px, vol, ax)
        self.kick_storage_all()

    def start_synchronizing(self, ax, vol, syncOpt=None):
        '''
        Start synchronization of a volume with an archive.
        ax :: Server - archive server.
        vol :: str   - volume name.
        syncOpt :: SyncOpt or None - synchronization option.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        if syncOpt:
            verify_type(syncOpt, SyncOpt)
        for px in self.sLayout.proxyL:
            self.add_archive_to_proxy(px, vol, ax, doStart=True, syncOpt=syncOpt)
        self.kick_storage_all()

    def get_restorable(self, ax, vol, opt=''):
        '''
        Get restorable gid list.
        ax :: Server - archive server.
        vol :: str   - volume name.
        opt :: str   - you can specify 'all'.
        return :: [GidInfo] - gid info list.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_type(opt, str)
        args = ['get', 'restorable', vol]
        if opt:
            if opt == 'all':
                args.append(opt)
            else:
                raise Exception('get_restorable:bad opt', ax.name, vol, opt)
        ret = self.run_ctl(ax, args)
        if not ret:
            return []
        return map(GidInfo, ret.split('\n'))

    def get_restorable_gid(self, ax, vol, opt=''):
        '''
        Get restorable gid list.
        ax :: Server - archive server.
        vol :: str   - volume name.
        opt :: str   - you can specify 'all'.
        return :: [int] - gid list.
        '''
        return map(lambda x : x.gid, self.get_restorable(ax, vol, opt))

    def print_restorable(self, ax, vol, opt=''):
        '''
        Print restorable gid list.
        ax :: Server - archive server.
        vol :: str   - volume name.
        opt :: str   - you can specify 'all'.
        '''
        printL(self.get_restorable(ax, vol, opt))

    def get_restored(self, ax, vol):
        '''
        Get restored gid list.
        ax :: Server - archive server.
        vol :: str   - volume name.
        return :: [int] - gid list.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        ret = self.run_ctl(ax, ['get', 'restored', vol])
        if not ret:
            return []
        return map(int, ret.split('\n'))

    def wait_for_restorable(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot specified of a gid to be restorable.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        self._wait_for_gid(ax, vol, gid, 'restorable', timeoutS)

    def wait_for_restored(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot specified of a gid to be restored.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_u64(gid)
        self._wait_for_no_action(ax, vol, aaRestore, timeoutS)
        gids = self.get_restored(ax, vol)
        if gid in gids:
            return
        raise Exception('wait_for_restored:failed', ax.name, vol, gid, gids)

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
            raise Exception(msg, 'gid must not be restorable', ax.name, vol, gid)

    def replicate_once_nbk(self, aSrc, vol, aDst, syncOpt=None):
        '''
        Copy current (aSrc, vol) to aDst.
        This does not wait for the replication done.
        Use wait_for_replicated() or replicate_once() to wait for it.
        aSrc :: Server - source archive (as a client).
        vol :: str     - volume name.
        aDst :: Server - destination archive (as a server).
        syncOpt :: SyncOpt or None - synchronization option.
        return :: int  - latest gid to replicate
        '''
        verify_server_kind(aSrc, [K_ARCHIVE])
        verify_type(vol, str)
        verify_server_kind(aDst, [K_ARCHIVE])
        if syncOpt:
            verify_type(syncOpt, SyncOpt)

        st = self.get_state(aDst, vol)
        if st == aClear:
            self._init(aDst, vol)

        gid = self.get_restorable_gid(aSrc, vol)[-1]
        args = ['replicate', vol, "gid", str(gid), aDst.get_host_port()]
        if syncOpt:
            args += syncOpt.getReplicateArgs()
        self.run_ctl(aSrc, args)
        return gid

    def wait_for_replicated(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot is restorable at an archive server.
        ax :: Server    - archive server as a replication server (not client).
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_u64(gid)
        self._wait_for_not_state(ax, vol, aDuringReplicate, timeoutS)
        gidL = self.get_restorable_gid(ax, vol, 'all')
        if gidL and gid <= gidL[-1]:
            return
        raise Exception("wait_for_replicated:replicate failed",
                        ax.name, vol, gid, gidL)

    def replicate_once(self, aSrc, vol, aDst, timeoutS=TIMEOUT_SEC, syncOpt=None):
        '''
        Copy current (aSrc, vol) to aDst.
        This will wait for the replicated done.
        aSrc :: Server - source archive (as a client).
        vol :: str     - volume name.
        aDst :: Server - destination archive (as a server).
        timeoutS :: int - timeout [sec].
        syncOpt :: SyncOpt or None - synchronization option.
        return :: int  - replicated gid.
        '''
        gid = self.replicate_once_nbk(aSrc, vol, aDst, syncOpt)
        self.wait_for_replicated(aDst, vol, gid, timeoutS)
        return gid

    def synchronize(self, aSrc, vol, aDst, timeoutS=TIMEOUT_SEC, syncOpt=None):
        '''
        Synchronize aDst with (aSrc, vol).
        To reduce proxies stopped period, replicate nosync before calling this.
        aSrc :: Server  - source archive (as a client).
        vol :: str      - volume name.
        aDst :: Server  - destination archive (as a server).
        timeoutS :: int - timeout [sec].
        syncOpt :: SyncOpt or None - synchronization option.
        '''
        verify_server_kind(aSrc, [K_ARCHIVE])
        verify_type(vol, str)
        verify_server_kind(aDst, [K_ARCHIVE])
        if syncOpt:
            verify_type(syncOpt, SyncOpt)

        for px in self.sLayout.proxyL:
            st = self.get_state(px, vol)
            if st in pActive:
                self.run_ctl(px, ["stop", vol, 'empty'])

        for px in self.sLayout.proxyL:
            self.wait_for_stopped(px, vol)
            aL = self.get_archive_info_list(px, vol)
            cmd = 'update' if aDst.name in aL else 'add'
            args = ["archive-info", cmd, vol, aDst.name, aDst.get_host_port()]
            if syncOpt:
                args += syncOpt.getArchiveInfoArgs()
            self.run_ctl(px, args)

        self.replicate_once(aSrc, vol, aDst, timeoutS, syncOpt)

        for px in self.sLayout.proxyL:
            self.start(px, vol)
        self.kick_storage_all()

    def full_backup(self, sx, vol, timeoutS=TIMEOUT_SEC, block=True, bulkSizeU=None, syncOpt=None):
        '''
        Run full backup a volume of a storage server.
        Log transfer to the primary archive server will start automatically.
        This function will return when a clean snapshot is
        sx :: Server     - storage server.
        vol :: str       - volume name.
        timeoutS :: int  - timeout [sec].
                           Counter will start after dirty full backup done.
        block :: Bool    - blocking behavior.
        bulkSizeU :: str - bulk size with unit suffix [byte]. '64K' etc.
        syncOpt :: SyncOpt or None - synchronization option.
        return :: int    - generation id of a clean snapshot.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        verify_type(timeoutS, int)
        verify_type(block, bool)
        if bulkSizeU:
            verify_size_unit(bulkSizeU)
        if syncOpt:
            verify_type(syncOpt, SyncOpt)

        a0 = self.sLayout.get_primary_archive()
        self._prepare_backup(sx, vol, syncOpt)
        args = ["full-bkp", vol]
        if bulkSizeU:
            args.append(bulkSizeU)
        self.run_ctl(sx, args)
        if not block:
            return
        self._wait_for_state_change(sx, vol, sDuringFullSync,
                                    [sTarget], timeoutS)
        st = self.get_state(a0, vol)
        if st not in aActive:
            raise Exception('full_backup: sync failed', sx.name, a0.name, vol, st)

        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = self.get_restorable_gid(a0, vol)
            if gids:
                return gids[-1]
            time.sleep(0.3)
        raise Exception('full_backup:timeout', sx.name, vol, gids)

    def hash_backup(self, sx, vol, timeoutS=TIMEOUT_SEC, block=True, bulkSizeU=None, syncOpt=None):
        '''
        Run hash backup a volume of a storage server.
        Log transfer to the primary archive server will start automatically.
        This function will return a gid of a clean snapshot that is
        restorable at the primary archive server.

        sx :: Server     - storage server.
        vol :: str       - volume name.
        timeoutS :: int  - timeout [sec].
                           Counter will start after dirty hash backup done.
        block :: bool    - blocking behavior.
        bulkSizeU :: str or None - bulk size with unit suffix [byte]. '64K' etc.
        syncOpt :: SyncOpt or None - synchronization option.
        return :: int    - generation id of a clean snapshot.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        verify_type(timeoutS, int)
        verify_type(block, bool)
        if bulkSizeU:
            verify_size_unit(bulkSizeU)
        if syncOpt:
            verify_type(syncOpt, SyncOpt)

        a0 = self.sLayout.get_primary_archive()
        self._prepare_backup(sx, vol, syncOpt)
        prev_gids = self.get_restorable_gid(a0, vol)
        if prev_gids:
            max_gid = prev_gids[-1]
        else:
            max_gid = -1
        args = ["hash-bkp", vol]
        if bulkSizeU:
            args.append(bulkSizeU)
        self.run_ctl(sx, args)
        if not block:
            return
        self._wait_for_state_change(sx, vol, sDuringHashSync,
                                    [sTarget], timeoutS)
        st = self.get_state(a0, vol)
        if st not in aActive:
            raise Exception('hash_backup: sync failed', sx.name, a0.name, vol, st)

        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = self.get_restorable_gid(a0, vol)
            if gids and gids[-1] > max_gid:
                return gids[-1]
            time.sleep(0.3)
        raise Exception('hash_backup:timeout', sx.name, vol, max_gid, gids)

    def restore(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Restore a volume.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec]
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)

        path = self.get_restored_path(ax, vol, gid)
        runCommand = self.get_run_remote_command(ax)
        if exists_file(path, runCommand):
            raise Exception('restore: alreay restored', ax.name, vol, gid)
        self.run_ctl(ax, ['restore', vol, str(gid)])
        self.wait_for_restored(ax, vol, gid, timeoutS)
        wait_for_lv_ready(path, runCommand)

    def get_restored_path(self, ax, vol, gid):
        '''
        ax :: Server  - archive server.
        vol :: str    - volume name.
        gid :: int    - generation id.
        return :: str - restored path.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)
        return '/dev/' + ax.vg + '/r_' + vol + '_' + str(gid)

    def del_restored(self, ax, vol, gid):
        '''
        Delete a restored volume.
        ax :: Server  - archive server.
        vol :: str    - volume name.
        gid :: int    - generation id.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)

        runCommand = self.get_run_remote_command(ax)
        path = self._get_lv_path(ax, vol)
        wait_for_lv_ready(path, runCommand)
        retryTimes = 3
        for i in xrange(retryTimes):
            try:
                if i != 0 and gid not in self.get_restored(ax, vol):
                    break
                self.run_ctl(ax, ['del-restored', vol, str(gid)])
                break
            except Exception, e:
                print 'del-restored retry', i, e
                time.sleep(1)
        else:
            raise Exception('del-restored: exceeds max retry times', ax.name, vol, gid)
        self._wait_for_not_restored(ax, vol, gid)

    def _del_restored_all(self, ax, vol):
        '''
        Delete all restored volumes of a volume.
        ax :: Server - archive server.
        vol :: str   - volume name.
        return :: [int] - gid list of deleted snapshots.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        gidL = self.get_restored(ax, vol)
        for gid in gidL:
            self.del_restored(ax, vol, gid)
        return gidL

    def snapshot_nbk(self, sx, vol):
        '''
        Take a snaphsot. This is nonblocking.
        sx :: Server  - storage server.
        vol :: str    - volume name.
        return :: int - gid of the taken snapshot.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        gid = self.run_ctl(sx, ['snapshot', vol])
        return int(gid)

    def snapshot(self, sx, vol, axL, timeoutS=TIMEOUT_SEC):
        '''
        Take a snapshot and wait for it to be restorable in archive servers.
        sx :: Server    - storage server.
        vol :: str      - volume name.
        axL :: [Server] - archive server list.
        return :: int   - gid of the taken snapshot.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        verify_type(axL, list, Server)
        for ax in axL:
            st = self.get_state(ax, vol)
            if st not in aActive:
                raise Exception('snapshot:bad state', ax.name, vol, st)
        gid = self.snapshot_nbk(sx, vol)
        st = self.get_state(sx, vol)
        if st != sTarget:
            raise Exception('snapshot: state is not target and can not wait', sx.name, vol, st)
        for ax in axL:
            self.wait_for_restorable(ax, vol, gid, timeoutS)
        return gid

    def apply(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Apply diffs older than a gid the base lv.
        ax :: Server - archive server
        vol :: str   - volume name.
        gid :: int   - generation id.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)
        gidL = self.get_restorable_gid(ax, vol, 'all')
        if gid not in gidL:
            raise Exception('apply: gid is not restorable', ax.name, vol, gid)
        self.run_ctl(ax, ["apply", vol, str(gid)])
        self._wait_for_applied(ax, vol, gid, timeoutS)

    def _apply_all(self, ax, vol, timeoutS=TIMEOUT_SEC):
        '''
        Apply diffs older than a gid the base lv.
        ax :: Server - archive server
        vol :: str   - volume name.
        return :: int - gid which all previous diffs were applied.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        gidL = self.get_restorable_gid(ax, vol)
        if not gidL:
            raise Exception('_apply_all: there are no diffs to apply', ax.name, vol, timeoutS)
        gid = gidL[-1]
        self.apply(ax, vol, gid, timeoutS=timeoutS)
        return gid

    def merge(self, ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
        '''
        Merge diffs in gid ranges.
        ax :: Server - archive server.
        vol :: str   - volume name.
        gidB :: int  - begin gid.
        gidE :: int  - end gid.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_gid_range(gidB, gidE, 'merge')
        gidL = self.get_restorable_gid(ax, vol, 'all')
        if gidB not in gidL or gidE not in gidL:
            raise Exception("merge: specify exact ranges", ax.name, vol, gidB, gidE)
        self.run_ctl(ax, ["merge", vol, str(gidB), "gid", str(gidE)])
        self._wait_for_merged(ax, vol, gidB, gidE, timeoutS)

    def replicate(self, aSrc, vol, aDst, synchronizing, timeoutS=TIMEOUT_SEC, syncOpt=None):
        '''
        Replicate archive data by copying a volume from one archive to another.
        aSrc :: Server        - source archive server (as client).
        vol :: str            - volume name.
        aDst :: Server        - destination archive server (as server).
        synchronizing :: bool - True if you want to make aDst synchronizing.
        timeoutS :: int       - timeout [sec].
        syncOpt :: SyncOpt or None - synchronization option.
        '''
        verify_server_kind(aSrc, [K_ARCHIVE])
        verify_type(vol, str)
        verify_server_kind(aDst, [K_ARCHIVE])
        verify_type(synchronizing, bool)

        self.replicate_once(aSrc, vol, aDst, timeoutS, syncOpt)
        if synchronizing:
            self.synchronize(aSrc, vol, aDst, timeoutS, syncOpt)

    def get_latest_clean_snapshot(self, ax, vol):
        '''
        ax :: Server - archive server.
        vol :: str   - volume name.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        xL = self.get_restorable_gid(ax, vol)
        if xL:
            return xL[-1]
        else:
            raise Exception('get_latest_clean_snapshot:not found', ax.name, vol)

    def resize_archive(self, ax, vol, sizeMb, doZeroClear):
        '''
        Resize archive volume.
        ax :: Server        - archive server.
        vol :: str          - volume name.
        sizeMb :: int       - new size [MiB].
        doZeroClear :: bool - True if zero-clear extended area.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(sizeMb)
        verify_type(doZeroClear, bool)
        st = self.get_state(ax, vol)
        if st == aClear:
            return
        elif st not in aAcceptForResize:
            raise Exception('resize_archive:bad state', ax.name, vol, sizeMb, st)

        runCommand = self.get_run_remote_command(ax)
        oldSizeMb = get_lv_size_mb(self._get_lv_path(ax, vol), runCommand)
        if oldSizeMb == sizeMb:
            return
        if oldSizeMb > sizeMb:
            raise Exception('resize_archive:shrink is not supported', ax.name, vol, oldSizeMb, sizeMb)
        args = ['resize', vol, str(sizeMb) + 'm']
        if doZeroClear:
            args += ['zeroclear']
        self.run_ctl(ax, args)
        self._wait_for_resize(ax, vol, oldSizeMb, sizeMb)

    def resize_storage(self, sx, vol, sizeMb):
        '''
        Resize storage volume.
        You must resize ddev before calling this.
        sx :: Server  - storage server.
        vol :: str    - voume name.
        sizeMb :: int - new size [MiB].
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        verify_u64(sizeMb)
        st = self.get_state(sx, vol)
        if st == sClear:
            return
        else:
            self.run_ctl(sx, ['resize', vol, str(sizeMb) + 'm'])

    def resize(self, vol, sizeMb, doZeroClear):
        '''
        Resize a volume.
        This will affect all storage servers and archive servers.
        vol :: str          - volume name.
        sizeMb :: int       - new size [MiB].
        doZeroClear :: bool - True if you want to zero-clear the extended area at the archives.
                              Storage's extended areas will not be filled by this function.
        '''
        verify_type(vol, str)
        verify_u64(sizeMb)
        verify_type(doZeroClear, bool)
        for ax in self.sLayout.archiveL:
            self.resize_archive(ax, vol, sizeMb, doZeroClear)
        for sx in self.sLayout.storageL:
            self.resize_storage(sx, vol, sizeMb)

    '''
    Private member functions.

    '''
    def _wait_for_state_cond(self, s, vol, pred, msg, timeoutS=10):
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            st = self.get_state(s, vol)
            if pred(st):
                return
            time.sleep(0.3)
        raise Exception("wait_for_state_cond", s.name, vol, msg, st)

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
                            s.name, vol, tmpStateL, goalStateL, st)

    def _make_get_int_list(self, cmd, ax, vol):
        if cmd == 'restorable':
            return lambda : self.get_restorable_gid(ax, vol)
        elif cmd == 'restored':
            return lambda : self.get_restored(ax, vol)
        else:
            raise Exception('bad cmd', cmd, ax.name, vol)

    def _wait_for_gid(self, ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
        f = self._make_get_int_list(cmd, ax, vol)
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = f()
            if gid in gids:
                return
            time.sleep(0.3)
        raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)

    def _wait_for_not_gid(self, ax, vol, gid, cmd, timeoutS=TIMEOUT_SEC):
        f = self._make_get_int_list(cmd, ax, vol)
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            gids = f()
            if gid not in gids:
                return
            time.sleep(0.3)
        raise Exception('wait_for_gid: timeout', ax.name, vol, gid, cmd, gids)

    def _wait_for_not_restored(self, ax, vol, gid, timeoutS=SHORT_TIMEOUT_SEC):
        '''
        Wait for a restored snapshot specified of a gid to be removed
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        self._wait_for_not_gid(ax, vol, gid, 'restored', timeoutS)

    def _wait_for_applied(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for diffs older than a gid to be applied.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gid :: int      - generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_u64(gid)
        self._wait_for_no_action(ax, vol, aaApply, timeoutS)
        gidL = self.get_restorable_gid(ax, vol)
        if gidL and gid <= gidL[0]:
            return
        raise Exception('wait_for_applied:failed', ax.name, vol, gid, gidL)

    def _wait_for_merged(self, ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
        '''
        Wait for diffs in a gid range to be merged.
        ax :: Server    - archive server.
        vol :: str      - volume name.
        gidB :: int     - begin generation id.
        gidE :: int     - end generation id.
        timeoutS :: int - timeout [sec].
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_gid_range(gidB, gidE, 'wait_for_merged')
        verify_type(timeoutS, int)
        self._wait_for_no_action(ax, vol, aaMerge, timeoutS)
        gidL = self.get_restorable_gid(ax, vol, 'all')
        pos = gidL.index(gidB)
        if gidL[pos + 1] == gidE:
            return
        raise Exception("wait_for_merged:failed",
                        ax.name, vol, gidB, gidE, pos, gidL)

    def _prepare_backup(self, sx, vol, syncOpt=None):
        '''
        Prepare backup.
        sx :: Server               - storage server.
        vol :: str                 - volume name.
        syncOpt :: SyncOpt or None - synchronization option.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        if syncOpt:
            verify_type(syncOpt, SyncOpt)

        st = self.get_state(sx, vol)
        if st == sSyncReady:
            pass
        elif st == sStandby:
            self.stop(sx, vol)
        elif st == sStopped:
            self.reset(sx, vol)
        else:
            raise Exception("prepare_backup:bad state. call stop if target.",
                            sx.name, vol, st)

        for s in self.sLayout.storageL:
            if s == sx:
                continue
            st = self.get_state(s, vol)
            if st not in [sStandby, sClear]:
                raise Exception("prepare_backup:bad state", s.name, sx, vol, st)

        for ax in self.sLayout.archiveL:
            if self.is_synchronizing(ax, vol):
                self.stop_synchronizing(ax, vol)

        # Initialize the volume at the primary archive if necessary.
        a0 = self.sLayout.get_primary_archive()
        st = self.get_state(a0, vol)
        if st == aClear:
            self._init(a0, vol)

        self.start_synchronizing(a0, vol, syncOpt)

    def _get_lv_path(self, ax, vol):
        '''
        ax :: Server  - archive server.
        vol :: str    - volume name.
        return :: str - lv path.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        return '/dev/' + ax.vg + '/i_' + vol

    def _wait_for_no_action(self, s, vol, action, timeoutS=TIMEOUT_SEC):
        '''
        s :: Server     - server.
        vol :: str      - volume name.
        action :: str   - action name.
        timeoutS :: int - timeout [sec].
        '''
        verify_server_kind(s, serverKinds)
        verify_type(vol, str)
        verify_type(action, str)
        verify_type(timeoutS, int)

        t0 = time.time()
        while time.time() < t0 + timeoutS:
            if self.get_num_action(s, vol, action) == 0:
                return
            time.sleep(0.3)
        raise Exception("wait_for_no_action", s.name, vol, action)

    def _wait_for_resize(self, ax, vol, oldSizeMb, sizeMb):
        '''
        Wait for resize done.
        ax :: Server  - archive server.
        vol :: str    - volume name.
        oldSizeMb :: int - old size [MiB].
        sizeMb :: int - new size [MiB].
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(sizeMb)
        self._wait_for_no_action(ax, vol, aaResize, SHORT_TIMEOUT_SEC)
        runCommand = self.get_run_remote_command(ax)
        curSizeMb = get_lv_size_mb(self._get_lv_path(ax, vol), runCommand)
        if curSizeMb == oldSizeMb:
            raise Exception('wait_for_resize:size is not changed(restored vol may exist)',
                            ax.name, vol, oldSizeMb, sizeMb)
        if curSizeMb != sizeMb:
            raise Exception('wait_for_resize:failed',
                            ax.name, vol, oldSizeMb, sizeMb, curSizeMb)

    def _verify_shutdown_mode(self, mode, msg):
        if mode not in ['graceful', 'force']:
            raise Exception(msg, 'bad mode', mode)
