#!/usr/bin/env python

import os
import time
import subprocess
import sys
import re
import _strptime # strptime is not thread safe, so import _strptime before calling it
import datetime
from collections import defaultdict

########################################
# Constants for general purpose.
########################################

TIMEOUT_SEC = 86400
SHORT_TIMEOUT_SEC = 100

Mebi = (1 << 20)  # mebi.
Lbs = (1 << 9)  # logical block size

UINT64_MAX = (1 << 64) - 1

BASE_VOLUME_PREFIX = 'wb_'
RESTORED_VOLUME_PREFIX = 'wr_'
COLD_VOLUME_PREFIX = 'wc_'


########################################
# Verification functions.
########################################

def verify_type(obj, typeValue, elemType=None):
    '''Verify type of an object.

    It raises a TypeError when none of typeValue(s) did match obj.

    obj       - object.
    typeValue - type like int, str, list. or a list of them.
    elemType  - specify type of elements if typeValue is sequence. or a list of them.
    '''
    if obj is None:
        raise TypeError('None type')
    if not isinstance(typeValue, list):
        typeValue = [typeValue]
    if all([not isinstance(obj, t) for t in typeValue]):
        raise TypeError('Invalid object type: {} must be one of [{}]'
                        .format(str(type(obj)), ','.join([str(t) for t in typeValue])))
    if elemType is None:
        return
    if not isinstance(elemType, list):
        elemType = [elemType]
    for elem in obj:
        if all([not isinstance(elem, t) for t in elemType]):
            raise TypeError('Invalid element type: {} must be one of [{}]'
                            .format(str(type(elem)), ','.join([str(t) for t in elemType])))


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


def verify_shutdown_mode(mode, msg):
    if mode not in ['graceful', 'force']:
        raise Exception(msg, 'bad mode', mode)


########################################
# Utility functions.
########################################

'''
RunCommand type is function with arguments type (args, isDebug).
  args :: [str]   - command line arguments.
  isDebug :: bool - True to put debug messages.
  return :: str   - stdout of the command.

'''

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


def get_server(name, sL):
    '''
    Get only one element from L having name
    name :: str   - server name.
    sL :: [ServerParams] - server list.
    return :: ServerParams
    '''
    verify_type(name, str)
    verify_type(sL, list, ServerParams)
    ret = []
    for x in sL:
        if x.name == name:
            ret.append(x)
    if len(ret) != 1:
        raise Exception('get_server:not one', ret, name, [x.name for x in sL])
    return ret[0]


########################################
# Constants for walb.
########################################

# Server kinds.

K_STORAGE = 0
K_PROXY = 1
K_ARCHIVE = 2

serverKinds = [K_STORAGE, K_PROXY, K_ARCHIVE]
serverKindMap = {K_STORAGE: 'storage', K_PROXY: 'proxy', K_ARCHIVE: 'archive'}
serverKindRmap = {v:k for k, v in serverKindMap.items()}


def server_kind_to_str(kind):
    '''
    kind :: int   - K_XXX.
    return :: str - server kind representation.
    '''
    if kind not in serverKindMap.keys():
        raise ValueError('bad server kind', kind)
    return serverKindMap[kind]


def str_to_server_kind(s):
    '''
    s :: int - string.
    return :: int - server kind.
    '''
    if s not in serverKindRmap.keys():
        raise ValueError('bad server kind string', s)
    return serverKindRmap[s]


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
atResync = "Resync"
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
aAcceptForResize = aActive + [aStopped, aSyncReady]
aAcceptForClearVol = [aStopped, aSyncReady]
aDuringReplicate = [atReplSync, atFullSync, atResync]
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


DatetimeFormatPretty = '%Y-%m-%dT%H:%M:%S'
DatetimeFormatDigits = '%Y%m%d%H%M%S'


def str_to_datetime(s, fmt):
    '''
    s :: str
    fmt :: str : DatetimeFormatXXX
    return :: datetime
    '''
    return datetime.datetime.strptime(s, fmt)


def datetime_to_str(ts, fmt):
    '''
    ts :: datetime
    fmt :: str : DatetimeFormatXXX
    return :: str
    '''
    return ts.strftime(fmt)


class Snapshot(object):
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
    def parse(self, s):
        verify_type(s, str)
        if s[0] != '|' or s[-1] != '|':
            raise Exception('Snapshot:bad format', s)
        sp = s[1:-1].split(',')
        if len(sp) == 1:
            self.gidB = self.gidE = int(sp[0])
        elif len(sp) == 2:
            self.gidB = int(sp[0])
            self.gidE = int(sp[1])
        else:
            raise Exception('Snapshot:bad range', s)
    def isDirty(self):
        return self.gidB != self.gidE
    def __eq__(self, rhs):
        return self.gidB == rhs.gidB and self.gidE == rhs.gidE
    def __ne__(self, rhs):
        return not(self == rhs)


def create_snapshot_from_str(s):
    '''
    create snapshot from str
    s :: str such as |num| or |num1,num2|
    '''
    snap = Snapshot()
    snap.parse(s)
    return snap


class Diff(object):
    '''
    DIff class
    '''
    def __init__(self, B=None, E=None):
        '''
        B :: Snapshot
        E :: Snapshot
        '''
        if B is None:
            B = Snapshot()
        if E is None:
            E = Snapshot()
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
        ts_str = datetime_to_str(self.ts, DatetimeFormatPretty)
        return "%s-->%s %s%s %s %d" % (self.B, self.E, m, c, ts_str, self.dataSize)
    def isDirty(self):
        return self.B.isDirty() or self.E.isDirty()
    def __eq__(self, rhs):
        return self.B == rhs.B and self.E == rhs.E and self.isMergeable == rhs.isMergeable and self.isCompDiff == rhs.isCompDiff and self.ts == rhs.ts and self.dataSize == rhs.dataSize
    def __ne__(self, rhs):
        return not(self == rhs)
    def parse(self, s):
        '''
        set by s
        s :: str
        '''
        verify_type(s, str)
        (ss, mc, tsStr, sizeStr) = s.split(' ')
        (bStr, eStr) = ss.split('-->')
        self.B.parse(bStr)
        self.E.parse(eStr)
        if mc[0] == 'M':
            self.isMergeable = True
        elif mc[0] == '-':
            self.isMergeable = False
        else:
            raise Exception('Diff:bad isMergeable', s)
        if mc[1] == 'C':
            self.isCompDiff = True
        elif mc[1] == '-':
            self.isCompDiff = False
        else:
            raise Exception('Diff:bad isCompDiff', s)
        self.ts = str_to_datetime(tsStr, DatetimeFormatPretty)
        self.dataSize = int(sizeStr)


def create_diff_from_str(s):
    '''
    create diff from str
    s :: str
    return :: Diff
    '''
    d = Diff()
    d.parse(s)
    return d


class MetaState(object):
    '''
    Data stored in 'base' files for archive servers.
    '''
    def __init__(self, B=None, E=None):
        '''
        B :: Snapshot
        E :: Snapshot or None.
        '''
        if B is None:
            B = Snapshot()
        verify_type(B, Snapshot)
        if E is not None:
            verify_type(E, Snapshot)
        self.B = B
        self.E = E
        self.ts = datetime.datetime.now()
        self.verify()

    def is_applying(self):
        '''
        return :: bool
        '''
        return self.E is not None

    def verify(self):
        '''
        verify data.
        return :: None
        '''
        if self.E is None:
            return
        if self.B.gidB >= self.E.gidB:
            raise Exception('bad MetaState', self.B, self.E)

    def __str__(self):
        tsStr = datetime_to_str(self.ts, DatetimeFormatDigits)
        if self.E is None:
            return '<%s>-%s' % (str(self.B), tsStr)
        else:
            return '<%s-->%s>-%s' % (str(self.B), str(self.E), tsStr)

    def __eq__(self, rhs):
        if self.B != rhs.B:
            return False
        # E may be None
        if self.E:
            if rhs.E:
                return self.E == rhs.E
            else:
                return False
        else:
            if rhs.E:
                return False
            return True
    def __ne__(self, rhs):
        return not(self == rhs)


def create_meta_state_from_str(s):
    '''
    create MetaState from str.
    s :: str
    return :: MetaState
    '''
    verify_type(s, str)
    p = re.compile(r'<(\|[^|]+\|)(?:-->(\|[^|]+\|))?>-(\d+)')
    m = p.match(s)
    if not m:
        raise Exception('create_meta_state_from_str:bad format', s)
    metaSt = MetaState()
    metaSt.B = create_snapshot_from_str(m.group(1))
    if m.group(2):
        metaSt.E = create_snapshot_from_str(m.group(2))
    else:
        metaSt.E = None
    metaSt.ts = str_to_datetime(m.group(3), DatetimeFormatDigits)
    return metaSt


class GidInfo(object):
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
        self.ts = str_to_datetime(p[1], DatetimeFormatPretty)
    def __str__(self):
        return str(self.gid) + " " + datetime_to_str(self.ts, DatetimeFormatPretty)
    def __eq__(self, rhs):
        return self.gid == rhs.gid and self.ts == rhs.ts
    def __ne__(self, rhs):
        return not(self == rhs)


class CompressOpt(object):
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
    def __eq__(self, rhs):
        return self.kind == rhs.kind and self.level == rhs.level and self.nrCpu == rhs.nrCpu
    def __ne__(self, rhs):
        return not self.__eq__(rhs)


# Synchronization option.

class SyncOpt(object):
    '''
    Synchronization option.
    This is used for optional arguments of archive-info and replicate command.
    '''
    def __init__(self, cmprOpt=None, delayS=0, maxWdiffMergeSizeU='1G', bulkSizeU='64K'):
        '''
        cmprOpt :: CompressOpt or None - compression option.
        delayS :: int             - delay to forward diffs from proxy to archive [sec].
        maxWdiffMergeSizeU :: str - max wdiff merge size [byte].
                                    Unit suffix like '1G' is allowed.
        bulkSizeU :: str          - bulk size [byte]. can be like '64K'.
                                    Unit suffix like '64K' is allowed.
        '''
        if cmprOpt is None:
            cmprOpt = CompressOpt()
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
    def __str__(self):
        return "cmpr={} merge={} bulk={}".format(self.cmprOpt, self.maxWdiffMergeSizeU, self.bulkSizeU)


########################################
# Classes for walb controller.
########################################

class Device(object):
    '''
    Walb device.

    '''
    def __init__(self, name, ldev, ddev, wdevcPath, runCommand=run_local_command):
        '''
        name :: str             - walb device name.
        ldev :: str             - underlying log block device path.
        ddev :: str             - underlying data block device path.
        wdevcPath :: str        - wdevc path.
        runCommand:: RunCommand - function that run commands.
        '''
        verify_type(name, str)
        verify_type(ldev, str)
        verify_type(ddev, str)
        verify_type(wdevcPath, str)
        verify_function(runCommand)
        self.name = name
        self.ldev = ldev
        self.ddev = ddev
        self.wdevcPath = wdevcPath
        self.runCommand = runCommand

    @property
    def path(self):
        return '/dev/walb/' + self.name

    def __str__(self):
        return ', '.join([self.name, self.ldev, self.ddev, self.wdevcPath])

    def run_wdevc(self, cmdArgs):
        '''
        Run wdevc command.
        cmdArgs :: [str] - command line arguments.
        return :: str - command stdout.
        '''
        verify_type(cmdArgs, list, str)
        return self.runCommand([self.wdevcPath] + cmdArgs)

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

    def format_ldev(self, doDiscard=True):
        '''
        Format devices for a walb device.
        doDiscard :: bool - issue discard whole area of the log device before formatting the device.
        '''
        args = ['format-ldev', self.ldev, self.ddev]
        if not doDiscard:
            args.append('-nd')
        self.run_wdevc(args)

    def create(self, maxl=None, maxp=None, minp=None, qp=None, fs=None, fp=None, bp=None, bi=None):
        '''
        Create a walb device.
        maxl :: int - max logpack size [KiB]
        maxp :: int - max pending size [MiB]
        minp :: int - min pending size [MiB]
        qp :: int   - queue stopping period after pending data is full [ms]
        fs :: int   - flush interval size [MiB]
        fp :: int   - flush interval period [ms]
        bp :: int   - number of packs processed at once.
        bi :: int   - number of IOs processed at once.
        None means default value for all the parameters.
        '''
        args = ['create-wdev', self.ldev, self.ddev, '-n', self.name]
        optL = ['-maxl', '-maxp', '-minp', '-qp', '-fs', '-fp', '-bp', '-bi']
        valL = [maxl, maxp, minp, qp, fs, fp, bp, bi]
        for opt, val in zip(optL, valL):
            if val:
                verify_int(val)
                args += [opt, str(val)]
        self.run_wdevc(args)

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

    def get_log_size_lb(self):
        '''
        Get walb log device size.
        This will read sysfs to get size.
        return :: int -- log device size [logical block].
        '''
        path = self._get_wldev_sys_path() + 'size'
        return int(self.runCommand(['/bin/cat', path]).strip())

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
        return '/sys/block/walb!{}/'.format(self.name)

    def _get_wldev_sys_path(self):
        return '/sys/block/walb!L{}/'.format(self.name)


class ServerConnectionParam(object):
    '''Server connection parameters.'''
    def __init__(self, name, address, port, kind):
        '''
        name :: str            - daemon identifier in the system.
        address :: str         - host name
        port :: int            - port number.
        kind :: int            - K_STORAGE, K_PROXY, or K_ARCHIVE.
        '''
        verify_type(name, str)
        verify_type(address, str)
        verify_type(port, int)
        verify_type(kind, int)
        if kind not in serverKinds:
            raise Exception('Server: wrong server kind', kind)

        self.name = name
        self.address = address
        self.port = port
        self.kind = kind

    def _get_as_str_list(self):
        return [self.name, self.address, str(self.port),
             server_kind_to_str(self.kind)]

    def __str__(self):
        return ', '.join(self._get_as_str_list())

    def __eq__(self, rhs):
        return self.name == rhs.name and self.address == rhs.address and self.port == rhs.port and self.kind == rhs.kind

    def get_host_port(self):
        '''
        Get 'address:port' string.
        return :: str
        '''
        return self.address + ":" + str(self.port)


class ServerStartupParam(object):
    '''Server startup parameters.'''
    def __init__(self, connParam, binDir, dataDir, logPath=None, vg=None, tp=None):
        '''
        connParam :: ServerConnectionParam - server connection parameters.
        binDir :: str          - directory path containing server executable
                                 at the host.
        dataDir :: str         - persistent data directory path.
        logPath :: str or None - log path. None means default.
        vg :: str              - volume group name.
                                 This is required by archive server only.
        tp :: str              - thinpool name.
                                 This is optional and required by archive server only.
        '''
        verify_type(connParam, ServerConnectionParam)
        self._connParam = connParam
        verify_type(binDir, str)
        verify_type(dataDir, str)
        if logPath is not None:
            verify_type(logPath, str)
        if self.kind == K_ARCHIVE or vg is not None:
            verify_type(vg, str)
        if self.kind == K_ARCHIVE and tp is not None:
            verify_type(tp, str)

        self.binDir = binDir
        self.dataDir = dataDir
        self.logPath = logPath
        if self.kind == K_ARCHIVE:
            self.vg = vg
            self.tp = tp

    @property
    def name(self):
        return self._connParam.name

    @property
    def address(self):
        return self._connParam.address

    @property
    def port(self):
        return self._connParam.port

    @property
    def kind(self):
        return self._connParam.kind

    def get_host_port(self):
        return self._connParam.get_host_port()

    def _get_as_str_list(self):
        L = self._connParam._get_as_str_list()
        L += [self.binDir, self.dataDir, self.logPath]
        if self.kind == K_ARCHIVE:
            L.append(self.vg)
            if self.tp:
                L.append(self.tp)
        return L

    def __str__(self):
        return ', '.join(self._get_as_str_list())


# Use ServerParams as vaiant type to verify.
ServerParams = [ServerConnectionParam, ServerStartupParam]


def verify_server_kind(s, kindL):
    '''
    s :: ServerParams
    kindL :: [int]
    '''
    verify_type(s, ServerParams)
    verify_type(kindL, list, int)
    if s.kind not in kindL:
        raise Exception('invalid server type', s.name, s.kind, kindL)


class ServerLayout(object):
    '''
    Server layout of a backup group.
    '''
    def __init__(self, storageL, proxyL, archiveL):
        '''
        storageL :: [ServerParams] - storage server list.
        proxyL :: [ServerParams]   - proxy server list.
                               Before items have high priority.
        archiveL :: [ServerParams] - archive server list. The head is primary server.
        '''
        verify_type(storageL, list, ServerParams)
        verify_type(proxyL, list, ServerParams)
        verify_type(archiveL, list, ServerParams)

        for kind, sL in [(K_STORAGE, storageL), (K_PROXY, proxyL), (K_ARCHIVE, archiveL)]:
            for s in sL:
                if s.kind != kind:
                    raise Exception('server_layout: invalid server kind:', server_kind_to_str(kind), str(s))

        self.storageL = storageL
        self.proxyL = proxyL
        self.archiveL = archiveL

    def __str__(self):
        return ', '.join([s.name for s in self.get_all()])

    def get_primary_archive(self):
        '''
        return :: ServerParams
        '''
        self.verify(0, 0, 1)
        return self.archiveL[0]

    def get_all(self):
        '''
        return :: [ServerParams]
        '''
        return self.storageL + self.proxyL + self.archiveL

    def replace(self, storageL=None, proxyL=None, archiveL=None):
        '''
        Make a copy replacing arguments which are not None.
        storageL :: [ServerParams] or None - if None, storageL will not unchange.
        proxyL :: [ServerParams] or None - if None, archiveL will not unchange.
        archiveL :: [ServerParams] or None - if None, archiveL will not unchange.
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

    def verify(self, minNrStorage, minNrProxy, minNrArchive):
        '''
        Verify the number of storage/proxy/archive servers.
        '''
        verify_int(minNrStorage)
        verify_int(minNrProxy)
        verify_int(minNrArchive)
        for kind, minNr, nr in [('storage', minNrStorage, len(self.storageL)),
                                ('proxy', minNrProxy, len(self.proxyL)),
                                ('archive', minNrArchive, len(self.archiveL))]:
            if nr < minNr:
                raise Exception('ServerLayout: more servers required', kind, nr, minNr)

    def to_cmd_string(self):
        '''Make command strings to run server.
        All items must be ServerStartupParam.
        '''
        for s in self.get_all():
            verify_type(s, ServerStartupParam)
            print ' '.join(get_server_args(s, self))


def dict2args(d):
    '''
    Convert dictionary data to arguments.
    '''
    verify_type(d, dict)
    args = []
    for k, v in d.iteritems():
        verify_type(k, str)
        args.append('-{}'.format(k))
        if v is None:
            continue
        if isinstance(v, list):
            args += map(str, v)
        else:
            args.append(str(v))
    return args


def get_server_params(s, sLayout=None, isDebug=False, useRepeater=False, maxFgTasks=2, maxBgTasks=1):
    '''
    Get walb-tools server parameters as a dictionary.
    s :: ServerStartupParam - server.
    sLayout :: ServerLayout - required for storage server.
    useRepeater :: Bool - use repeater if True
    maxFgTasks :: int or None - max number of foreground tasks.
    maxBgTasks :: int or None - max number of background tasks.
    return :: {str: *} - key: option string, value: option argument(s).
    '''
    verify_type(s, ServerStartupParam)
    if s.kind == K_STORAGE:
        verify_type(sLayout, ServerLayout)
    verify_type(isDebug, bool)
    verify_type(useRepeater, bool)
    if maxFgTasks is not None:
        verify_type(maxFgTasks, int)
    if maxBgTasks is not None:
        verify_type(maxBgTasks, int)

    ret = {}
    if s.kind == K_STORAGE:
        ret['archive'] = sLayout.get_primary_archive().get_host_port()
        ret['proxy'] = ','.join([p.get_host_port() for p in sLayout.proxyL])
    elif s.kind == K_ARCHIVE:
        ret['vg'] = s.vg
        if s.tp is not None:
            ret['tp'] = s.tp
    else:
        assert s.kind == K_PROXY
        # do nothing

    if s.logPath:
        logPath = s.logPath
    else:
        logPath = s.dataDir + '/' + s.name + '.log'
    port = s.port
    if useRepeater:
        port += 10000
    ret['p'] = port
    ret['b'] = s.dataDir
    ret['l'] = logPath
    ret['id'] = s.name

    if maxFgTasks is not None:
        ret['fg'] = maxFgTasks
    if maxBgTasks is not None and s.kind != K_ARCHIVE:
        ret['bg'] = maxBgTasks
    if isDebug:
        ret['debug'] = None
    return ret


def get_server_args(s, sLayout=None, isDebug=False, useRepeater=False, maxFgTasks=2, maxBgTasks=1):
    '''
    Get walb-tools server arguments.
    s :: ServerStartupParam - server startup param.
    sLayout :: ServerLayout - required for storage server.
    useRepeater :: Bool - use repeater if True
    maxFgTasks :: int or None - max number of foreground tasks.
    maxBgTasks :: int or None - max number of background tasks.
    return :: [str] - argument list.
    '''
    d = get_server_params(s, sLayout, isDebug, useRepeater, maxFgTasks, maxBgTasks)
    binPath = s.binDir + 'walb-{}'.format(server_kind_to_str(s.kind))
    return [binPath] + dict2args(d)


class Controller(object):
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
        s :: ServerParams  - a server.
        cmdArgs :: [str] - command arguments.
        putMsg :: bool   - put debug message if True.
        timeoutS :: int  - timeout in sec. 0 means default.
        return :: str    - stdout of the control command.
        '''
        verify_type(s, ServerParams)
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

        s :: ServerParams - server where you want to run command.
        args :: [str]   - command line arguments.
                          The head item must be a full-path executable.
        putMsg :: bool  - put debug message if True.
        timeoutS :: int - set timeout sec.
        return :: str   - stdout of the command if the command returned 0.
        '''
        verify_type(s, ServerParams)
        verify_type(args, list, str)
        verify_type(putMsg, bool)
        verify_type(timeoutS, int)
        cmdArgs = ['exec'] + args
        return self.run_ctl(s, cmdArgs, putMsg, timeoutS)

    def get_run_remote_command(self, s):
        '''
        Get run_command function for RPC.
        walbc :: Controller
        s :: ServerParams
        return :: ([str],bool -> str)

        '''
        verify_type(s, ServerParams)

        def func(args, putMsg=False):
            return self.run_remote_command(s, args, putMsg or self.isDebug)

        return func

    def get_host_type(self, s):
        '''
        Get host type.
        s :: ServerParams
        return :: str - 'storage', 'proxy', or 'archive'.
        '''
        verify_type(s, ServerParams)
        return self.run_ctl(s, ['get', 'host-type'])

    def get_state(self, s, vol):
        '''
        Get state of a volume.
        s :: ServerParams
        vol :: str    - volume name.
        return :: str - state.
        '''
        verify_type(s, ServerParams)
        verify_type(vol, str)
        return self.run_ctl(s, ['get', 'state', vol])

    def get_state_all(self, vol, sL=None):
        '''
        Get all state fo a volume.
        vol :: str - volume name
        sL :: [ServerParams] or None - None means all servers in the layout.
        '''
        verify_type(vol, str)
        if sL is None:
            sL = self.sLayout.get_all()
        verify_type(sL, list, ServerParams)
        for s in sL:
            msg = "%s %s:%d %s" % (s.name, s.address, s.port, server_kind_to_str(s.kind))
            try:
                st = self.get_state(s, vol)
            except:
                st = "err"
            print msg, st

    def verify_state(self, s, vol, state):
        '''
        s :: ServerParams
        vol :: str
        state :: str
        '''
        verify_type(state, str)
        st = self.get_state(s, vol)
        if st != state:
            raise Exception('verify_state: differ', s.name, st, state)

    def get_vol_list(self, s):
        '''
        Get volume list.
        s :: ServerParams
        return :: [str] - volume name list.
        '''
        verify_server_kind(s, serverKinds)
        return self.run_ctl(s, ['get', 'vol']).split()

    def _get_size_value(self, ax, vol, cmd):
        '''
        Get size value from an archive server.
        ax :: ServerParams  - archive server.
        vol :: str        - volume name.
        cmd :: str        - command string
        return :: u64     - size value.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_type(cmd, str)
        if cmd not in ['vol-size', 'progress']:
            raise Exception('_get_size_value: bad cmd', ax.name, vol, cmd)
        ret = self.run_ctl(ax, ['get', cmd, vol])
        sizeLb = int(ret)
        verify_u64(sizeLb)
        return sizeLb

    def get_vol_size_lb(self, ax, vol):
        '''
        Get volume size.
        ax :: ServerParams  - archive server.
        vol :: str        - volume name.
        return :: u64     - volume size [logical block].
        '''
        return self._get_size_value(ax, vol, 'vol-size')

    def _get_vol_size_mb(self, ax, vol):
        '''
        Get volume size [Mebi].
        '''
        return self.get_vol_size_lb(ax, vol) * Lbs / Mebi

    def get_progress_lb(self, ax, vol):
        '''
        Get progress of full/hash backup/replicate command.
        ax :: ServerParams  - archive server.
        vol :: str        - volume name.
        return :: u64     - progress [logical block].
        '''
        return self._get_size_value(ax, vol, 'progress')

    def get_base(self, ax, vol):
        '''
        Get base meta state of a volume in an archive server.
        ax :: ServerParams    - archive server.
        vol :: str          - volume name.
        return :: MetaState - meta state.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        metaStStr = self.run_ctl(ax, ['get', 'base', vol])
        return create_meta_state_from_str(metaStStr)

    def monitor_progress(self, ax, vol, timeoutS=TIMEOUT_SEC):
        '''
        Monitor progress.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        timeoutS :: int  - timeout [sec].
        '''
        verify_int(timeoutS)
        sizeLb = self.get_vol_size_lb(ax, vol)
        t0 = time.time()
        while time.time() < t0 + timeoutS:
            progressLb = self.get_progress_lb(ax, vol)
            if progressLb == 0:
                return
            print progressLb / float(sizeLb)
            time.sleep(1)
        raise Exception('monitor_progress:timeout', ax.name, vol, timeoutS)

    def get_diff_list(self, ax, vol, gid0=0, gid1=UINT64_MAX):
        '''
        Get wdiff list.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid0 :: u64      - range begin.
        gid1 :: u64      - range end.
        return :: [Diff] - wdiff information list managed by the archive server.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid0)
        verify_u64(gid1)
        ls = self.run_ctl(ax, ['get', 'diff', vol, str(gid0), str(gid1)])
        if not ls:
            return []
        return map(create_diff_from_str, ls.split('\n'))

    def print_diff_list(self, ax, vol, gid0=0, gid1=UINT64_MAX):
        '''
        Print wdiff list.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid0 :: int      - range begin.
        gid1 :: int      - range end.
        '''
        printL(self.get_diff_list(ax, vol, gid0, gid1))

    def get_applicable_diff_list(self, ax, vol, gid=UINT64_MAX):
        '''
        Get wdiff to list to apply.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: u64       - target gid.
        return :: [Diff] - wdiff information list managed by the archive server.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)
        ls = self.run_ctl(ax, ['get', 'applicable-diff', vol, str(gid)])
        if not ls:
            return []
        return map(create_diff_from_str, ls.split('\n'))

    def print_applicable_diff_list(self, ax, vol, gid=UINT64_MAX):
        '''
        Print applicable wdiff list.
        '''
        printL(self.get_applicable_diff_list(ax, vol, gid))

    def _get_with_gid_range(self, ax, vol, cmd, gid0=0, gid1=UINT64_MAX):
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_type(cmd, str)
        if cmd not in ['num-diff', 'total-diff-size']:
            raise Exception('invalid command', cmd)
        verify_u64(gid0)
        verify_u64(gid1)
        if gid0 > gid1:
            raise Exception('bad gid range', gid0, gid1)
        ret = self.run_ctl(ax, ['get', cmd, vol, str(gid0), str(gid1)])
        return int(ret)

    def get_num_diff(self, ax, vol, gid0=0, gid1=UINT64_MAX):
        '''
        Get number of wdiff files for a volume.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid0 :: int      - gid range begin.
        gid1 :: int      - gid range end.
        return :: int    - number of wdiffs in the gid range.
        '''
        return self._get_with_gid_range(ax, vol, 'num-diff', gid0, gid1)

    def get_total_diff_size(self, ax, vol, gid0=0, gid1=UINT64_MAX):
        '''
        Get total wdiff size.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid0 :: int      - gid range begin.
        gid1 :: int      - gid range end.
        return :: int    - total size in the gid range [byte].
        '''
        return self._get_with_gid_range(ax, vol, 'total-diff-size', gid0, gid1)

    def get_num_action(self, s, vol, action):
        '''
        Get number of an action running for a volume.
        s :: ServerParams - server.
        vol :: str      - volume name.
        action :: str   - action name.
        return :: int   - the number of running
                          the specified action on the volume.
        '''
        verify_type(s, ServerParams)
        verify_type(vol, str)
        verify_type(action, str)
        num = int(self.run_ctl(s, ['get', 'num-action', vol, action]))
        return num

    def get_all_actions(self, ax):
        '''
        Get all action state for all the volumes.
        ax :: ServerParams - an archive server.
        return :: {str: {str: (int, datetime or None)}}
            - volId, actionName, number of running actions, last beginning time or None.
        '''
        verify_type(ax, ServerParams)
        def parse(s):
            L = s.split()
            if len(L) == 0 or len(L) % 3 == 0:
                raise Exception('parse all-actions response failed:', s)
            vol = L.pop(0)
            tL = []
            while len(L) > 0:
                action = L.pop(0)
                num = L.pop(0)
                tsStr = L.pop(0)
                if tsStr == '---':
                    ts = None
                else:
                    ts = str_to_datetime(tsStr, DatetimeFormatPretty)
                tL.append((action, int(num), ts))
            return (vol, tL)
        d = defaultdict(lambda: defaultdict(lambda: (0, None)))
        res  = self.run_ctl(ax, ['get', 'all-actions'])
        if res == '':
            return d # empty.
        sL = res.split('\n')
        for vol, tL in map(parse, sL):
            for action, nr, ts in tL:
                d[vol][action] = (nr, ts)
        return d

    def get_vol_dict_without_running_actions(self, ax):
        '''
        Get volume dict whose actions are not running at all.
        ax :: ServerParams - an archive server.
        return :: {(str:{str: datetime or None})}
            - volume, action name, the last time to begin.
        '''
        volD = {}
        d = self.get_all_actions(ax)
        for vol, d2 in d.iteritems():
            total = 0
            d3 = {}
            for action, (nr, ts) in d2.iteritems():
                total += nr
                d3[action] = ts
            if total == 0:
                volD[vol] = d3
        return volD

    def reset(self, s, vol, timeoutS=SHORT_TIMEOUT_SEC):
        '''
        Reset a volume.
        s :: ServerParams - storage or archive.
        timeoutS :: int - timeout in sec. 0 means default.
        vol :: str  - volume name.
        '''
        verify_type(s, ServerParams)
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
        sx :: ServerParams - storage server.
        vol :: str       - volume name.
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
            self.stop(sx, vol, 'force')
            self.stop_synchronizing(self.sLayout.get_primary_archive(), vol)
            self.reset(sx, vol)
            self._go_standby(sx, vol)
            return
        raise Exception('go_standby:bad state', sx.name, vol, state)

    def _go_standby(self, sx, vol):
        '''
        change state in sx from SyncReady to Standby

        sx :: ServerParams - storage server.
        vol :: str       - volume name.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        state = self.get_state(sx, vol)
        if state != sSyncReady:
            raise Exception('_go_standby:bad state', sx.name, vol, state)
        self.run_ctl(sx, ['start', vol, 'standby'])
        self._wait_for_state_change(sx, vol, [stStartStandby], [sStandby])

    def kick_all(self, sL):
        '''
        Kick all servers.
        sL :: [ServerParams] - list of servers each of which
                              must be storage or proxy.
        '''
        verify_type(sL, list, ServerParams)
        for s in sL:
            verify_server_kind(s, [K_STORAGE, K_PROXY])
            self.run_ctl(s, ["kick"])

    def kick_storage_all(self):
        ''' Kick all storage servers. '''
        self.kick_all(self.sLayout.storageL)

    def set_full_scan_bps(self, sx, throughputU):
        '''
        sx :: ServerParams - storage server.
        throughputU :: str - throughput [bytes/sec]
            0 means unlimited.
            Unit suffix like '100M' is allowed.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_size_unit(throughputU)
        args = ['set-full-scan-bps', throughputU]
        self.run_ctl(sx, args)

    def is_overflow(self, sx, vol):
        '''
        Check a storage is overflow or not.
        sx :: ServerParams - storage server.
        vol :: str       - volume name.
        return :: bool   - True if the storage overflows.
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
        px :: ServerParams - proxy.
        vol :: str       - volume name.
        ax :: ServerParams - archive.
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
        s :: ServerParams - storage or archive.
        return :: str   - uuid string that regex pattern [0-9a-f]{32}.
        '''
        verify_server_kind(s, [K_STORAGE, K_ARCHIVE])
        verify_type(vol, str)
        return self.run_ctl(s, ['get', 'uuid', vol])

    def get_archive_uuid(self, ax, vol):
        '''
        Get archive uuid string.
        ax :: ServerParams - archive.
        return :: str    - uuid string that regex pattern [0-9a-f]{32}.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        return self.run_ctl(ax, ['get', 'archive-uuid', vol])

    def status(self, sL=None, vol=None, timeoutS=SHORT_TIMEOUT_SEC):
        '''
        print server status.
        sL :: ServerParams | [ServerParams] - server or server list.
        vol :: str or None - volume name. None means all.
        '''
        if not sL:
            sL = self.sLayout.get_all()
        if isinstance(sL, ServerConnectionParam) or isinstance(sL, ServerStartupParam):
            sL = [sL]
        verify_type(sL, list, ServerParams)
        for s in sL:
            args = ['status']
            if vol:
                args.append(vol)
            print '++++++++++++++++++++', s.name, '++++++++++++++++++++'
            print self.run_ctl(s, args, timeoutS=timeoutS)

    def shutdown(self, s, mode="graceful"):
        '''
        Shutdown a server.
        s :: ServerParams
        mode :: str - 'graceful' or 'force'.
        '''
        verify_type(s, ServerParams)
        verify_type(mode, str)
        verify_shutdown_mode(mode, 'shutdown')
        self.run_ctl(s, ["shutdown", mode])

    def shutdown_list(self, sL, mode='graceful'):
        '''
        Shutdown listed servers.
        '''
        verify_type(sL, list, ServerParams)
        verify_shutdown_mode(mode, 'shutdown_list')
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
        sx :: ServerParams
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
        s :: ServerParams         - storage or archive
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
        s :: ServerParams
        vol :: str
        '''
        verify_server_kind(s, serverKinds)
        verify_type(vol, str)
        st = self.get_state(s, vol)
        if s.kind == K_STORAGE:
            if st == sClear:
                return
            if st in [sTarget, sStandby]:
                self.stop(s, vol, 'force')
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
                self.stop(s, vol, 'force')
                st = self.get_state(s, vol)
            if st != pStopped:
                raise Exception('clear', s.name, vol, st)
        else:
            assert s.kind == K_ARCHIVE
            if st == aClear:
                return
            if st == aArchived:
                self.stop(s, vol, 'force')
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
        ax :: ServerParams - archive server.
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
        s :: ServerParams
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
            if prevSt in [sStandby, stHashSync, stFullSync]:
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
        s :: ServerParams
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
        s :: ServerParams
        vol :: str
        '''
        verify_server_kind(s, serverKinds)
        verify_type(vol, str)
        if s.kind == K_STORAGE:
            self.run_ctl(s, ['start', vol, 'target'])
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
        px :: ServerParams - proxy server.
        vol :: str       - voume name.
        ax :: ServerParams - archive server.
        '''
        verify_server_kind(px, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        st = self.get_state(px, vol)
        if st in pActive:
            self.stop(px, vol, 'force')
        aL = self.get_archive_info_list(px, vol)
        if ax.name in aL:
            self.run_ctl(px, ['archive-info', 'delete', vol, ax.name])
        st = self.get_state(px, vol)
        if st == pStopped:
            self.start(px, vol)

    def add_archive_to_proxy(self, px, vol, ax, doStart=True, syncOpt=None):
        '''
        Add an archive to a proxy.
        px :: ServerParams           - proxy server.
        vol :: str                 - volume name.
        ax :: serverInfo           - archive server.
        doStart :: bool            - False if not to start proxy after adding.
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
            self.stop(px, vol, 'force')
        aL = self.get_archive_info_list(px, vol)
        cmd = 'update' if ax.name in aL else 'add'
        args = ['archive-info', cmd, vol, ax.name, ax.get_host_port()]
        if syncOpt:
            args += syncOpt.getArchiveInfoArgs()
        self.run_ctl(px, args)
        st = self.get_state(px, vol)
        if st == pStopped and doStart:
            self.start(px, vol)

    def copy_archive_info(self, pSrc, vol, pDst):
        '''
        Copy archive info from a proxy to another proxy.
        pSrc :: ServerParams - srouce proxy.
        vol :: str
        pDst :: ServerParams - destination proxy. It must be stopped.
        '''
        verify_server_kind(pSrc, [K_PROXY])
        verify_type(vol, str)
        verify_server_kind(pDst, [K_PROXY])
        for axName in self.get_archive_info_list(pSrc, vol):
            ax = get_server(axName, self.sLayout.archiveL)
            _, syncOpt = self.get_archive_info(pSrc, vol, ax)
            self.add_archive_to_proxy(pDst, vol, ax, doStart=False, syncOpt=syncOpt)
        self.start(pDst, vol)

    def stop_synchronizing(self, ax, vol):
        '''
        Stop synchronization of a volume with an archive.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        for px in self.sLayout.proxyL:
            self.del_archive_from_proxy(px, vol, ax)
        self.kick_storage_all()

    def start_synchronizing(self, ax, vol, syncOpt=None):
        '''
        Start synchronization of a volume with an archive.
        ax :: ServerParams           - archive server.
        vol :: str                 - volume name.
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
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        opt :: str       - you can specify 'all'.
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
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        opt :: str       - you can specify 'all'.
        return :: [int]  - gid list.
        '''
        return [x.gid for x in self.get_restorable(ax, vol, opt)]

    def print_restorable(self, ax, vol, opt=''):
        '''
        Print restorable gid list.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        opt :: str       - you can specify 'all'.
        '''
        printL(self.get_restorable(ax, vol, opt))

    def get_restored_path(self, ax, vol, gid):
        '''
        Get restored volume path.
        This function does not confirm existance of the volume.
        ax :: ServerParams - archive server.
        vol :: str         - volume name.
        gid :: int         - generation id.
        '''
        vgName = self.run_ctl(ax, ['get', 'volume-group'])
        return '/dev/' + vgName + '/' + RESTORED_VOLUME_PREFIX + vol + '_' + str(gid)

    def _get_snapshot(self, ax, vol, isCold):
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_type(isCold, bool)
        target = 'cold' if isCold else 'restored'
        ret = self.run_ctl(ax, ['get', target, vol])
        if not ret:
            return []
        return map(int, ret.split('\n'))

    def get_restored(self, ax, vol):
        '''
        Get restored gid list.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        return :: [int]  - gid list.
        '''
        return self._get_snapshot(ax, vol, False)

    def get_cold(self, ax, vol):
        '''
        Get restored gid list.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        return :: [int]  - gid list.
        '''
        return self._get_snapshot(ax, vol, True)

    def get_meta_snap(self, ax, vol, gid=None):
        '''
        Get snapshot having gid.
        ax :: ServerParams - archive server.
        vol :: str         - volume name.
        gid :: None or int - generation id. None means the latest snapshot.
        return :: Snapshot - snapshot object which gidB is maximum one no more than the specified gid.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        if gid is not None:
            verify_u64(gid)
        args = ['get', 'meta-snap', vol]
        if gid is not None:
            args.append(str(gid))
        ret = self.run_ctl(ax, args)
        return create_snapshot_from_str(ret)

    def get_meta_state(self, ax, vol, isApplying, gid=None):
        '''
        Get snapshot having gid.
        ax :: ServerParams  - archive server.
        vol :: str          - volume name.
        isApplying :: bool  - specify True to get applying state.
        gid :: None or int  - generation id. None means the latest snapshot.
        return :: MetaState - metastate object which B.gidB is maximum one no more than the specified gid.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_type(isApplying, bool)
        if gid is not None:
            verify_u64(gid)
        args = ['get', 'meta-state', vol, '1' if isApplying else '0']
        if gid is not None:
            args.append(str(gid))
        ret = self.run_ctl(ax, args)
        return create_meta_state_from_str(ret)

    def wait_for_restorable(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot specified of a gid to be restorable.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: int       - generation id.
        timeoutS :: int  - timeout [sec].
        '''
        self._wait_for_gid(ax, vol, gid, 'restorable', timeoutS)

    def wait_for_restored(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for a snapshot specified of a gid to be restored.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: int       - generation id.
        timeoutS :: int  - timeout [sec].
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
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: int       - generation id.
        waitS :: int     - wait period [sec].
        msg :: str       - message for error.
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

    def _verify_having_same_archive_uuid(self, ax, ay, vol):
        verify_server_kind(ax, [K_ARCHIVE])
        verify_server_kind(ay, [K_ARCHIVE])
        verify_type(vol, str)
        uuid0 = self.get_archive_uuid(ax, vol)
        uuid1 = self.get_archive_uuid(ay, vol)
        if uuid0 != uuid1:
            raise Exception('Archive uuids differ. Try call this with doResync=True',
                            ax.name, uuid0, ay.name, uuid1)

    def _exists_base_image(self, ax, vol):
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        return int(self.run_ctl(ax, ['get', 'exists-base-image', vol])) != 0

    def _grow_if_necessary_for_replicate(self, aSrc, vol, aDst):
        if not self._exists_base_image(aDst, vol):
            return
        sizeSrc = self._get_vol_size_mb(aSrc, vol)
        sizeDst = self._get_vol_size_mb(aDst, vol)
        if sizeSrc < sizeDst:
            raise Exception('Volume size of aDst is larger than that of aSrc',
                            aSrc.name, sizeSrc, aDst.name, sizeDst)
        elif sizeSrc > sizeDst:
            self.resize_archive(aDst, vol, sizeSrc, False)

    def replicate_once_nbk(self, aSrc, vol, aDst, doResync=False, dontMerge=False, syncOpt=None, gid=None):
        '''
        Copy current (aSrc, vol) to aDst.
        This does not wait for the replication done.
        Use wait_for_replicated() or replicate_once() to wait for it.
        aSrc :: ServerParams - source archive (as a client).
        vol :: str         - volume name.
        aDst :: ServerParams - destination archive (as a server).
        doResync :: bool - resync if necessary.
        dontMerge :: bool - do not merge diffs (to avoid recompress).
        syncOpt :: SyncOpt or None - synchronization option.
        gid :: None or int - target gid. If None, gid of the latest clean snapshot will be used.
        return :: int  - latest gid to replicate
        '''
        verify_server_kind(aSrc, [K_ARCHIVE])
        verify_type(vol, str)
        verify_server_kind(aDst, [K_ARCHIVE])
        verify_type(doResync, bool)
        verify_type(dontMerge, bool)
        if syncOpt:
            verify_type(syncOpt, SyncOpt)

        st = self.get_state(aDst, vol)
        if st == aClear:
            self._init(aDst, vol)
        elif st == aArchived and not doResync:
            self._verify_having_same_archive_uuid(aSrc, aDst, vol)

        self._grow_if_necessary_for_replicate(aSrc, vol, aDst)

        if gid is None:
            gid = self.get_restorable_gid(aSrc, vol)[-1]
        args = ['replicate', vol, "gid", str(gid), aDst.get_host_port(),
                '1' if doResync else '0',
                '1' if dontMerge else '0']
        if syncOpt:
            args += syncOpt.getReplicateArgs()
        self.run_ctl(aSrc, args)
        return gid

    def wait_for_replicated(self, aSrc, vol, aDst, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for replication action done.
        aSrc :: ServerParams    - archive server as a replication client.
        vol :: str            - volume name.
        aDst :: ServerParams    - archive server as a replication server.
        gid :: int            - generation id.
        timeoutS :: int       - timeout [sec].
        '''
        verify_server_kind(aSrc, [K_ARCHIVE])
        verify_server_kind(aDst, [K_ARCHIVE])
        verify_u64(gid)
        self._wait_for_no_action(aSrc, vol, aaReplSync, timeoutS)
        self._wait_for_not_state(aDst, vol, aDuringReplicate, timeoutS)
        gidL = self.get_restorable_gid(aDst, vol, 'all')
        if gidL and gid <= gidL[-1]:
            return
        raise Exception("wait_for_replicated:replicate failed",
                        aSrc.name, aDst.name, vol, gid, gidL)

    def replicate_once(self, aSrc, vol, aDst, timeoutS=TIMEOUT_SEC, doResync=False, dontMerge=False, syncOpt=None, gid=None):
        '''
        Copy current (aSrc, vol) to aDst.
        This will wait for the replicated done.
        aSrc :: ServerParams         - source archive (as a client).
        vol :: str                 - volume name.
        aDst :: ServerParams         - destination archive (as a server).
        timeoutS :: int            - timeout [sec].
        doResync :: bool           - resync if necessary.
        dontMerge :: bool          - do not merge diffs (to avoid recompress).
        syncOpt :: SyncOpt or None - synchronization option.
        gid :: None or int - target gid. If None, gid of the latest clean snapshot will be used.
        return :: int              - replicated gid.
        '''
        gid = self.replicate_once_nbk(aSrc, vol, aDst, doResync, dontMerge, syncOpt, gid)
        self.wait_for_replicated(aSrc, vol, aDst, gid, timeoutS)
        return gid

    def synchronize(self, aSrc, vol, aDst, timeoutS=TIMEOUT_SEC, syncOpt=None):
        '''
        Synchronize aDst with (aSrc, vol).
        To reduce proxies stopped period, replicate nosync before calling this.
        aSrc :: ServerParams         - source archive (as a client).
        vol :: str                 - volume name.
        aDst :: ServerParams         - destination archive (as a server).
        timeoutS :: int            - timeout [sec].
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

        self.replicate_once(aSrc, vol, aDst, timeoutS, False, False, syncOpt)

        for px in self.sLayout.proxyL:
            self.start(px, vol)
        self.kick_storage_all()

    def full_backup(self, sx, vol, timeoutS=TIMEOUT_SEC, block=True, bulkSizeU=None, syncOpt=None):
        '''
        Run full backup a volume of a storage server.
        Log transfer to the primary archive server will start automatically.
        This function will return when a clean snapshot is
        sx :: ServerParams - storage server.
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

        sx :: ServerParams - storage server.
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
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: int       - generation id.
        timeoutS :: int  - timeout [sec]
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)
        if gid in self.get_restored(ax, vol):
            raise Exception('restore: alreay restored', ax.name, vol, gid)
        self.run_ctl(ax, ['restore', vol, str(gid)])
        self.wait_for_restored(ax, vol, gid, timeoutS)

    def _del_snapshot(self, ax, vol, gid, isCold):
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)
        verify_type(isCold, bool)
        cmd = 'del-cold' if isCold else 'del-restored'
        retryTimes = 3
        for i in xrange(retryTimes):
            try:
                if i != 0 and gid not in self._get_snapshot(ax, vol, isCold):
                    break
                self.run_ctl(ax, [cmd, vol, str(gid)])
                break
            except Exception, e:
                print cmd + ' retry', i, e
                time.sleep(1)
        else:
            raise Exception(cmd + ': exceeds max retry times', ax.name, vol, gid)
        self._wait_for_not_restored(ax, vol, gid)

    def del_restored(self, ax, vol, gid):
        '''
        Delete a restored volume.
        ax :: ServerParams  - archive server.
        vol :: str        - volume name.
        gid :: int        - generation id.
        '''
        self._del_snapshot(ax, vol, gid, False)

    def del_cold(self, ax, vol, gid):
        '''
        Delete a cold volume.
        ax :: ServerParams  - archive server.
        vol :: str        - volume name.
        gid :: int        - generation id.
        '''
        self._del_snapshot(ax, vol, gid, True)

    def _del_restored_all(self, ax, vol):
        '''
        Delete all restored volumes of a volume.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        return :: [int]  - gid list of deleted snapshots.
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
        sx :: ServerParams  - storage server.
        vol :: str        - volume name.
        return :: int     - gid of the taken snapshot.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        gid = self.run_ctl(sx, ['snapshot', vol])
        return int(gid)

    def snapshot(self, sx, vol, axL, timeoutS=TIMEOUT_SEC):
        '''
        Take a snapshot and wait for it to be restorable in archive servers.
        sx :: ServerParams    - storage server.
        vol :: str          - volume name.
        axL :: [ServerParams] - archive server list.
        return :: int       - gid of the taken snapshot.
        '''
        verify_server_kind(sx, [K_STORAGE])
        verify_type(vol, str)
        verify_type(axL, list, ServerParams)
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

    def disable_snapshot(self, ax, vol, gidL):
        '''
        Disable snapshots
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gidL :: [int]    - list of gid
        '''
        self._change_snapshot(ax, vol, gidL, isEnable=False)

    def enable_snapshot(self, ax, vol, gidL):
        '''
        Enable snapshots
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gidL :: [int]    - list of gid
        '''
        self._change_snapshot(ax, vol, gidL, isEnable=True)

    def _change_snapshot(self, ax, vol, gidL, isEnable):
        '''
        isEnable :: bool
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_type(isEnable, bool)
        cmd = 'enable-snapshot' if isEnable else 'disable-snapshot'
        args = [cmd, vol]
        if not isinstance(gidL, list):
            raise Exception(cmd, ax.name, vol, gidL)
        for gid in gidL:
            verify_u64(gid)
            args.append(str(gid))
        return self.run_ctl(ax, args)

    def apply(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Apply diffs older than a gid the base lv.
        ax :: ServerParams - archive server
        vol :: str       - volume name.
        gid :: int       - generation id.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(gid)
        gidL = [d.E.gidB for d in self.get_applicable_diff_list(ax, vol)]
        if gid not in gidL:
            raise Exception('apply: gid is not restorable', ax.name, vol, gid)
        self.run_ctl(ax, ["apply", vol, str(gid)])
        self._wait_for_applied(ax, vol, gid, timeoutS)

    def _apply_all(self, ax, vol, timeoutS=TIMEOUT_SEC):
        '''
        Apply diffs older than a gid the base lv.
        ax :: ServerParams - archive server
        vol :: str       - volume name.
        return :: int    - gid which all previous diffs were applied.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        diffL = self.get_applicable_diff_list(ax, vol)
        if len(diffL) == 0:
            # There are no diffs to apply.
            return
        gid = diffL[-1].E.gidB
        self.apply(ax, vol, gid, timeoutS=timeoutS)
        return gid

    def merge(self, ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
        '''
        Merge diffs in gid ranges.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gidB :: int      - begin gid.
        gidE :: int      - end gid.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_gid_range(gidB, gidE, 'merge')
        diffL = self.get_applicable_diff_list(ax, vol, gidE)
        if gidB not in [d.B.gidB for d in diffL] or gidE not in [d.E.gidB for d in diffL]:
            raise Exception("merge: specify exact ranges", ax.name, vol, gidB, gidE)
        self.run_ctl(ax, ["merge", vol, str(gidB), "gid", str(gidE)])
        self._wait_for_merged(ax, vol, gidB, gidE, timeoutS)

    def replicate(self, aSrc, vol, aDst, synchronizing, timeoutS=TIMEOUT_SEC, doResync=False, dontMerge=False, syncOpt=None):
        '''
        Replicate archive data by copying a volume from one archive to another.
        aSrc :: ServerParams    - source archive server (as client).
        vol :: str            - volume name.
        aDst :: ServerParams    - destination archive server (as server).
        synchronizing :: bool - True if you want to make aDst synchronizing.
        timeoutS :: int       - timeout [sec].
        doResync :: bool      - resync if necessary.
        dontMerge :: bool - do not merge diffs (to avoid recompress).
        syncOpt :: SyncOpt or None - synchronization option.
        '''
        verify_server_kind(aSrc, [K_ARCHIVE])
        verify_type(vol, str)
        verify_server_kind(aDst, [K_ARCHIVE])
        verify_type(synchronizing, bool)

        self.replicate_once(aSrc, vol, aDst, timeoutS, doResync, dontMerge, syncOpt)
        if synchronizing:
            self.synchronize(aSrc, vol, aDst, timeoutS, syncOpt)

    def get_latest_clean_snapshot(self, ax, vol):
        '''
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
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
        ax :: ServerParams    - archive server.
        vol :: str          - volume name.
        sizeMb :: int       - new size [MiB]. must not be 0.
        doZeroClear :: bool - True if zero-clear extended area.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(sizeMb)
        verify_type(doZeroClear, bool)
        if sizeMb == 0:
            raise Exception('resize_archive:sizeMb must not be zero', ax.name, vol, sizeMb)
        st = self.get_state(ax, vol)
        if st == aClear:
            return
        elif st not in aAcceptForResize:
            raise Exception('resize_archive:bad state', ax.name, vol, sizeMb, st)

        oldSizeMb = self._get_vol_size_mb(ax, vol)
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
        sx :: ServerParams  - storage server.
        vol :: str        - voume name.
        sizeMb :: int     - new size [MiB]. specify 0 to auto-detect its underlying ddev size.
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
        sizeMb :: int       - new size [MiB]. must not be 0.
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

    def gc_diff(self, ax, vol):
        '''
        Run garbage collect diffs for a volume.
        ax :: ServerParams  - archive server.
        vol :: str          - volume name.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        self.run_ctl(ax, ['gc-diff', vol])

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
            return lambda: self.get_restorable_gid(ax, vol)
        elif cmd == 'restored':
            return lambda: self.get_restored(ax, vol)
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
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: int       - generation id.
        timeoutS :: int  - timeout [sec].
        '''
        self._wait_for_not_gid(ax, vol, gid, 'restored', timeoutS)

    def _wait_for_applied(self, ax, vol, gid, timeoutS=TIMEOUT_SEC):
        '''
        Wait for diffs older than a gid to be applied.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: int       - generation id.
        timeoutS :: int  - timeout [sec].
        '''
        verify_u64(gid)
        self._wait_for_no_action(ax, vol, aaApply, timeoutS)
        metaSt = self.get_base(ax, vol)
        if metaSt.is_applying():
            raise Exception('wait_for_applied:failed', ax.name, vol, gid, str(metaSt))
        gidL = self.get_restorable_gid(ax, vol)
        if gidL and gid <= gidL[0]:
            return
        raise Exception('wait_for_applied:failed', ax.name, vol, gid, gidL)

    def _wait_for_merged(self, ax, vol, gidB, gidE, timeoutS=TIMEOUT_SEC):
        '''
        Wait for diffs in a gid range to be merged.
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gidB :: int      - begin generation id.
        gidE :: int      - end generation id.
        timeoutS :: int  - timeout [sec].
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_gid_range(gidB, gidE, 'wait_for_merged')
        verify_type(timeoutS, int)
        self._wait_for_no_action(ax, vol, aaMerge, timeoutS)
        for diff in self.get_applicable_diff_list(ax, vol, gidE):
            if diff.B.gidB == gidB and diff.E.gidB == gidE:
                return
        raise Exception("wait_for_merged:failed",
                        ax.name, vol, gidB, gidE)

    def _prepare_backup(self, sx, vol, syncOpt=None):
        '''
        Prepare backup.
        sx :: ServerParams           - storage server.
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

    def _wait_for_no_action(self, s, vol, action, timeoutS=TIMEOUT_SEC):
        '''
        s :: ServerParams - server.
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
        ax :: ServerParams  - archive server.
        vol :: str        - volume name.
        oldSizeMb :: int  - old size [MiB].
        sizeMb :: int     - new size [MiB].
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_type(vol, str)
        verify_u64(sizeMb)
        self._wait_for_no_action(ax, vol, aaResize, SHORT_TIMEOUT_SEC)
        curSizeMb = self._get_vol_size_mb(ax, vol)
        if curSizeMb == oldSizeMb:
            raise Exception('wait_for_resize:size is not changed(restored vol may exist)',
                            ax.name, vol, oldSizeMb, sizeMb)
        if curSizeMb != sizeMb:
            raise Exception('wait_for_resize:failed',
                            ax.name, vol, oldSizeMb, sizeMb, curSizeMb)

    def get_block_hash(self, ax, vol, gid, bulkSizeU='64K', scanSizeU=None):
        '''
        Get block hash for virtual
        ax :: ServerParams - archive server.
        vol :: str       - volume name.
        gid :: int       - generaion id.
        bulkSizeU :: str - bulk size hash calculation (with unit suffix)
        scanSizeU :: str - scanning size (with unit suffix).
                           It must not exceeds the device size.
        return :: str - hash value as a string.
        '''
        verify_server_kind(ax, [K_ARCHIVE])
        verify_u64(gid)
        verify_type(vol, str)
        verify_size_unit(bulkSizeU)
        args = ['bhash', vol, str(gid), bulkSizeU]
        if scanSizeU is not None:
            verify_size_unit(scanSizeU)
            args.append(scanSizeU)
        return self.run_ctl(ax, args)
