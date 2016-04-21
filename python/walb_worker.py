#!/usr/bin/env python
import sys, time, yaml, datetime, os, threading, signal
from walblib import *

walbcDebug = False
g_quit = False
g_verbose = False
g_retryNum = 3
g_step = 0

OLDEST_TIME = datetime.datetime(2000, 1, 1, 0, 0)

def getCurrentTime():
    return datetime.datetime.utcnow()

WARN = 'WARN'
INFO = 'INFO'
ERR = 'ERROR'
DEBUG = 'DEBUG'

def log(mode, *s):
    if mode == DEBUG and not g_verbose:
        return
    print getCurrentTime(), mode, ":".join(map(str,s))
    sys.stdout.flush()

def logd(*s):
    log(DEBUG, *s)

def loge(*s):
    log(ERR, *s)

def logi(*s):
    log(INFO, *s)

def logStart(argv, cfg):
    logi('start walb-worker')
    print ' '.join(argv)
    print '>>>\n' + str(cfg) + '\n<<<'

def logEnd():
    logi('end walb-worker')

def quitHandler(sig, frame):
    logi('signal walb-worker')
    global g_quit
    g_quit = True

def parseFLAG(s):
    verify_type(s, str)
    if s == '0':
        return False
    if s == '1':
        return True
    raise Exception('parseFLAG:bad s', s)

def parseSuffix(s, suf):
    if type(s) == int:
        n = s
    else:
        verify_type(s, str)
        verify_type(suf, dict)
        suffix = 1
        c = s[-1]
        if c in suf:
            suffix = suf[c]
            s = s[0:-1]
        n = int(s)
        n *= suffix
    if n < 0:
        raise Exception('parseSuffix:negative value', s)
    return n

def parsePERIOD(s):
    """
        digits suffix
        digits = [0-9]+
        suffix = (m|h|d)
        or
        hh:mm:ss
    """
    if type(s) == str:
        v = s.split(':')
        if len(v) == 3:
            return datetime.timedelta(hours=int(v[0]), minutes=int(v[1]), seconds=int(v[2]))
    v = parseSuffix(s, {'m':60, 'h':3600, 'd':86400})
    return datetime.timedelta(seconds=v)

def parseSIZE_UNIT(s):
    """
        digits suffix
        suffix = (K|M|G)
    """
    return parseSuffix(s, {'K':1024, 'M':1024 * 1024, 'G':1024 * 1024 * 1024})

def parsePositive(d):
    verify_type(d, int)
    if d < 0:
        raise Exception('parsePositive:negative', d)
    return d

def parsePort(d):
    verify_type(d, int)
    if d < 0 or d > 65535:
        raise Exception('parsePort', d)
    return d

def formatIndent(d, indent):
    s = ''
    sp = ' ' * indent
    n = len(d)
    i = 0
    for (k, v) in d:
        s += sp
        s += k + ': ' + str(v)
        if i < n - 1:
            s += '\n'
        i += 1
    return s

def setValIfExist(obj, d, k, pred):
    if d.has_key(k):
        # obj.k = pred(d[k])
        setattr(obj, k, pred(d[k]))

def identity(x):
    return x

class General:
    def __init__(self):
        self.addr = ''
        self.port = 0
        self.walbc_path = ''
        self.max_task = 1
        self.max_replication_task = 1
        self.kick_interval = 1

    def set(self, d):
        verify_type(d, dict)
        tbl = {
            'addr': identity,
            'port': parsePort,
            'walbc_path': identity,
            'max_task': parsePositive,
            'max_replication_task': parsePositive,
            'kick_interval': parsePositive
        }
        for (k, pred) in tbl.items():
            setValIfExist(self, d, k, pred)

    def verify(self):
        if self.addr == '':
            raise Exception('General addr is not set')
        if self.port == 0:
            raise Exception('General port is not set')
        if self.walbc_path == '':
            raise Exception('General walbc_path is not set')
        if not os.path.exists(self.walbc_path):
            raise Exception('walbc_path is not found', self.walbc_path)

    def __str__(self, indent=2):
        d = [
            ('addr', self.addr),
            ('port', self.port),
            ('walbc_path', self.walbc_path),
            ('max_task', self.max_task),
            ('max_replication_task', self.max_replication_task),
            ('kick_interval', self.kick_interval),
        ]
        return formatIndent(d, indent)

class Apply:
    def __init__(self):
        self.keep_period = datetime.timedelta()
        self.interval = datetime.timedelta(days=1)
        self.time_window = (0, 0)
    def set(self, d):
        verify_type(d, dict)
        setValIfExist(self, d, 'keep_period', parsePERIOD)
        setValIfExist(self, d, 'interval', parsePERIOD)

    def verify(self):
        if self.keep_period == datetime.timedelta():
            raise Exception('Apply keep_period is not set')

    def __str__(self, indent=2):
        d = [
            ('keep_period', self.keep_period),
            ('interval', self.interval),
        ]
        return formatIndent(d, indent)

class Merge:
    def __init__(self):
        self.interval = datetime.timedelta()
        self.max_nr = UINT64_MAX
        self.max_size = UINT64_MAX
        self.threshold_nr = UINT64_MAX
    def set(self, d):
        verify_type(d, dict)
        tbl = {
            'interval': parsePERIOD,
            'max_nr': parsePositive,
            'max_size': parseSIZE_UNIT,
            'threshold_nr': parsePositive,
        }
        for (k, pred) in tbl.items():
            setValIfExist(self, d, k, pred)

    def verify(self):
        if self.interval == datetime.timedelta():
            raise Exception('Merge interval is not set')

    def __str__(self, indent=2):
        d = [
            ('interval', self.interval),
            ('max_nr', self.max_nr),
            ('max_size', self.max_size),
            ('threashold_nr', self.threshold_nr),
        ]
        return formatIndent(d, indent)

class ReplServer:
    def __init__(self):
        self.name = ''
        self.addr = ''
        self.port = 0
        self.interval = 0
        self.compress = None
        self.max_merge_size = '1G'
        self.max_send_size = None
        self.bulk_size = '64K'
        self.log_name = ''
        self.enabled = True

    def set(self, name, d):
        verify_type(name, str)
        verify_type(d, dict)
        self.name = name
        tbl = {
            'addr':identity,
            'port': parsePort,
            'interval': parsePERIOD,
            'max_merge_size': str,
            'max_send_size': parseSIZE_UNIT,
            'bulk_size': str,
            'log_name': identity,
            'enabled': identity,
        }
        for (k, pred) in tbl.items():
            setValIfExist(self, d, k, pred)
        if d.has_key('compress'):
            self.compress = CompressOpt()
            self.compress.parse(d['compress'])
    def verify(self):
        if not self.enabled:
            return
        if self.addr == '':
            raise Exception('ReplServer addr is not set')
        if self.port == 0:
            raise Exception('ReplServer port is not set')
        if self.interval == 0:
            raise Exception('ReplServer interval is not set')
        verify_type(self.addr, str)
        if self.log_name is not None:
            verify_type(self.log_name, str)
        verify_type(self.enabled, bool)

    def __str__(self, indent=2):
        d = [
            ('addr', self.addr),
            ('port', self.port),
            ('interval', self.interval),
            ('compress', self.compress),
            ('max_merge_size', self.max_merge_size),
            ('max_send_size', self.max_send_size),
            ('bulk_size', self.bulk_size),
            ('log_name', self.log_name),
            ('enabled', self.enabled),
        ]
        return formatIndent(d, indent)
    def getServerConnectionParam(self):
        return ServerConnectionParam(self.name, self.addr, self.port, K_ARCHIVE)

class Repl:
    def __init__(self):
        self.servers = {}
        self.disabled_volumes = []
    def set(self, d):
        verify_type(d, dict)
        if d.has_key('servers'):
            ss = d['servers']
            for (name, v) in ss.items():
                if self.servers.has_key(name):
                    self.servers[name].set(name, v)
                else:
                    rs = ReplServer()
                    rs.set(name, v)
                    self.servers[name] = rs
        if d.has_key('disabled_volumes'):
            self.disabled_volumes = d['disabled_volumes']
    def verify(self):
        for rs in self.servers.values():
            rs.verify()
        verify_type(self.disabled_volumes, list, str)

    def __str__(self):
        indent = 2
        n = len(self.servers)
        i = 0
        s = ' ' * indent + 'servers:\n'
        for (name, rs) in self.servers.items():
            s += ' ' * indent * 2 + name + ':\n'
            s += rs.__str__(indent * 3)
            if i < n - 1:
                s += '\n'
            i += 1
        return s

    def getEnabledList(self):
        rsL = []
        for rs in self.servers.values():
            if rs.enabled:
                rsL.append(rs)
        return rsL


class Config:
    def __init__(self):
        self.general = General()
        self.apply_ = Apply()
        self.merge = Merge()
        self.repl = Repl()

    def set(self, d):
        verify_type(d, dict)
        if d.has_key('general'):
            self.general.set(d['general'])
        if d.has_key('apply'):
            self.apply_.set(d['apply'])
        if d.has_key('merge'):
            self.merge.set(d['merge'])
        if d.has_key('repl'):
            self.repl.set(d['repl'])

    def verify(self):
        self.general.verify()
        self.apply_.verify()
        self.merge.verify()
        self.repl.verify()

    def setStr(self, s):
        verify_type(s, str)
        d = yaml.load(s)
        self.set(d)

    def load(self, configName):
        verify_type(configName, str)
        s = ''
        if configName == '-':
                s = sys.stdin.read()
        else:
            with open(configName) as f:
                s = f.read()
        self.setStr(s)

    def __str__(self):
        s = "general:\n"
        s += str(self.general) + '\n'
        s += "apply:\n"
        s += str(self.apply_) + '\n'
        s += "merge:\n"
        s += str(self.merge) + '\n'
        s += 'repl:\n'
        s += str(self.repl)
        return s


class ExecedRepl:
    def __init__(self, vol, rs, ts):
        verify_type(vol, str)
        verify_type(rs, ReplServer)
        verify_type(ts, datetime.datetime)
        self.vol = vol
        self.rs = rs
        self.ts = ts
    def __str__(self):
        return "vol=%s rs=%s ts=%s" % (self.vol, self.rs, self.ts)

def getLatestGidInfoBefore(curTime, infoL):
    verify_type(curTime, datetime.datetime)
    verify_type(infoL, list, GidInfo)
    prev = None
    for info in infoL[1:]:
        if info.ts > curTime:
            break
        prev = info
    return prev

def getGidToRepl(diffL, max_send_size, a1latest):
    verify_type(diffL, list, Diff)
    if max_send_size is None:
        return None
    verify_int(max_send_size)
    verify_int(a1latest)

    n = len(diffL)
    if n <= 1:
        return None

    for i in xrange(n - 1):
        if diffL[i + 1].B.gidB > a1latest:
            break
    else:
        return None
    begin = i
    s = 0
    for i in xrange(begin, n):
        s += diffL[i].dataSize
        if s > max_send_size:
            break
    else:
        return None
    end = i
    if begin == end:
        end = begin + 1
    return end.B.gidB

def sumDiffSize(diffL):
    return sum([d.dataSize for d in diffL])


def getMergeGidRange(diffL, max_size, max_nr):
    """
        select range which satisfies the following conditions:
        [L0, L1, ...]
        L0 : not diff.isCompDiff
        L1, L2, ... : not diff.isCompDiff and diff.isMergeable
        2 <= len(diffL) <= max_nr
        sum diffL <= max_size
    """
    inApplying = False
    begin = 0
    totalSize = 0
    candidate = None

    def average((begin, end, size)):
        return size / (end - begin)
    for i in xrange(len(diffL)):
        diff = diffL[i]
        canBeMerged = not diff.isCompDiff and diff.dataSize <= max_size
        if inApplying:
            toBeAppended = diff.isMergeable and canBeMerged
            totalSize += diff.dataSize
            n = i - begin
            if toBeAppended and totalSize <= max_size and n <= max_nr:
                continue
            if n >= 2:
                t = (begin, i, totalSize)
                if not candidate or average(t) < average(candidate):
                    candidate = t
            inApplying = False
        if not inApplying and canBeMerged:
            begin = i
            totalSize = diff.dataSize
            inApplying = True
    else:
        # don't select the latest diff
        pass

    if candidate:
        begin, end, _ = candidate
        gidB = diffL[begin].B.gidB
        gidE = diffL[end - 1].E.gidB
        return (gidB, gidE)
    return None

class Task:
    def __init__(self, name, vol, ax):
        verify_type(name, str)
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        if name not in ['merge', 'apply', 'repl']:
            raise Exception('Task bad name', name, vol)
        self.name = name
        self.vol = vol
        self.ax = ax
    def run(self):
        pass
    def __str__(self):
        return "name={} vol={} ax={}".format(self.name, self.vol, self.ax)
    def __eq__(self, rhs):
        return self.name == rhs.name and self.vol == rhs.vol and self.ax == rhs.ax
    def __ne__(self, rhs):
        return not self.__eq__(rhs)

class ApplyTask(Task):
    def __init__(self, vol, ax, gid):
        Task.__init__(self, 'apply', vol, ax)
        verify_u64(gid)
        self.gid = gid
    def run(self, walbc):
        verify_type(walbc, Controller)
        walbc.apply(self.ax, self.vol, self.gid)
    def __str__(self):
        return Task.__str__(self) + " gid={}".format(self.gid)
    def __eq__(self, rhs):
        return Task.__eq__(self, rhs) and self.gid == rhs.gid

class MergeTask(Task):
    def __init__(self, vol, ax, gidB, gidE):
        Task.__init__(self, 'merge', vol, ax)
        verify_gid_range(gidB, gidE, 'merge')
        self.gidB = gidB
        self.gidE = gidE
    def run(self, walbc):
        verify_type(walbc, Controller)
        walbc.merge(self.ax, self.vol, self.gidB, self.gidE)
    def __str__(self):
        return Task.__str__(self) + " gid=({}, {})".format(self.gidB, self.gidE)
    def __eq__(self, rhs):
        return Task.__eq__(self, rhs) and self.gidB == rhs.gidB and self.gidE == rhs.gidE

class ReplTask(Task):
    def __init__(self, vol, ax, rs, maxGid=None):
        Task.__init__(self, 'repl', vol, ax)
        verify_type(rs, ReplServer)
        self.log_name = rs.name
        if rs.log_name:
            self.log_name += '_' + rs.log_name
        self.dst = rs.getServerConnectionParam()
        self.syncOpt = SyncOpt(cmprOpt=rs.compress, maxWdiffMergeSizeU=rs.max_merge_size, bulkSizeU=rs.bulk_size)
        self.maxGid = maxGid
    def run(self, walbc):
        verify_type(walbc, Controller)
        walbc.replicate_once(self.ax, self.vol, self.dst, syncOpt=self.syncOpt, gid=self.maxGid)
    def __str__(self):
        return Task.__str__(self) + " dst={} maxGid={}".format(self.log_name, self.maxGid)
    def __eq__(self, rhs):
        return Task.__eq__(self, rhs) and self.dst == rhs.dst

g_binDir = ''
g_dirName = ''
g_logName = ''

class Worker:
    def createSeverLayout(self, cfg):
        self.a0 = ServerConnectionParam('a0', cfg.general.addr, cfg.general.port, K_ARCHIVE)
        s0 = ServerConnectionParam('s0', '', 0, K_STORAGE)
        p0 = ServerConnectionParam('p0', '', 0, K_PROXY)
        return ServerLayout([s0], [p0], [self.a0])

    def __init__(self, cfg, Ctl=Controller):
        verify_type(cfg, Config)
        self.cfg = cfg
        self.serverLayout = self.createSeverLayout(self.cfg)
        self.walbc = Ctl(self.cfg.general.walbc_path, self.serverLayout, walbcDebug)
        self.doneReplServerList = {}

    def selectApplyTask1(self, volL):
        for vol in volL:
            ms = self.walbc.get_base(self.a0, vol)
            if ms.is_applying():
                return ApplyTask(vol, self.a0, ms.E.gidB)
        return None

    def selectApplyTask2(self, volActTimeL, curTime):
        '''
            get (vol, gid) having max diff
        '''
        ls = []
        for (vol, actTimeD) in volActTimeL:
            ts = actTimeD.get(aaApply)
            interval = self.cfg.apply_.interval
            if ts and interval > datetime.timedelta() and curTime < ts + interval:
                continue
            infoL = self.walbc.get_restorable(self.a0, vol, 'all')
            gidInfo = getLatestGidInfoBefore(curTime - self.cfg.apply_.keep_period, infoL)
            if not gidInfo:
                continue
            gid = gidInfo.gid
            size = self.walbc.get_total_diff_size(self.a0, vol, gid1=gid)
            logd('Task2 candidate', size, vol, gid)
            ls.append((size, vol, gid))
        if ls:
            ls.sort(key=lambda x : x[0])
            (size, vol, gid) = ls[-1]
            return ApplyTask(vol, self.a0, gid)
        else:
            return None

    def getNumDiffList(self, volL):
        numDiffL = []
        for vol in volL:
            n = self.walbc.get_num_diff(self.a0, vol)
            numDiffL.append(n)
        return numDiffL


    def selectMaxDiffNumMergeTask(self, ls):
        """
            ls = [(n, vol)]
        """
        verify_type(ls, list, tuple)
        if ls:
            verify_type(ls[0][0], int)
            verify_type(ls[0][1], str)
            ls.sort(key=lambda x : x[0], reverse=True)
            (_, vol) = ls[0]
            diffL = self.walbc.get_applicable_diff_list(self.a0, vol)
            r = getMergeGidRange(diffL, self.cfg.merge.max_size, self.cfg.merge.max_nr)
            if r:
                return MergeTask(vol, self.a0, r[0], r[1])
        return None

    def selectMergeTask1(self, volL, numDiffL):
        ls = []
        for (vol, n) in zip(volL, numDiffL):
            if n >= self.cfg.merge.threshold_nr:
                ls.append((n, vol))
        return self.selectMaxDiffNumMergeTask(ls)

    def selectMergeTask2(self, volActTimeL, numDiffL, curTime):
        ls = []
        for ((vol, actTimeD), n) in zip(volActTimeL, numDiffL):
            ts = actTimeD.get(aaMerge)
            if ts and curTime < ts + self.cfg.merge.interval:
                continue
            ls.append((n, vol))
        return self.selectMaxDiffNumMergeTask(ls)

    def selectReplTask(self, volL, curTime):
        tL = []
        rsL = self.cfg.repl.getEnabledList()
        for vol in volL:
            if vol in self.repl.disabled_volumes:
                continue
            a0State = self.walbc.get_state(self.a0, vol)
            a0latest = self.walbc.get_latest_clean_snapshot(self.a0, vol)
            if a0State not in aActive:
                continue
            for rs in rsL:
                a1 = rs.getServerConnectionParam()
                a1State = self.walbc.get_state(a1, vol)
                if a1State not in aActive:
                    continue
                ts = self.doneReplServerList.get((vol, rs))
                if ts and curTime < ts + rs.interval:
                    continue
                a1latest = self.walbc.get_latest_clean_snapshot(a1, vol)
                # skip if repl is not necessary
                if a0latest == a1latest:
                    continue
                if not ts:
                    ts = OLDEST_TIME
                tL.append((ts, vol, rs, a1latest))
        if tL:
            tL.sort(key=lambda x:x[0])
            (_, vol, rs, a1latest) = tL[0]
            diffL = self.walbc.get_applicable_diff_list(self.a0, vol)
            maxGid = getGidToRepl(diffL, rs.max_send_size, a1latest)
            self.doneReplServerList[(vol, rs)] = curTime
            return ReplTask(vol, self.a0, rs, maxGid)
        else:
            return None

    def selectTask(self, volActTimeL, curTime):
        volL = map(lambda x:x[0], volActTimeL)
        numDiffL = self.getNumDiffList(volL)
        t = None
        # step 1
        if g_step in [0, 1]:
            t = self.selectApplyTask1(volL)
            if t:
                logd('selectApplyTask1', t)
                return t
        # step 2
        if g_step in [0, 2]:
            t = self.selectApplyTask2(volActTimeL, curTime)
            if t:
                logd('selectApplyTask2', t)
                return t
        # step 3
        if g_step in [0, 3]:
            t = self.selectMergeTask1(volL, numDiffL)
            if t:
                logd('selectMergeTask1', t)
                return t
        # step 4
        if g_step in [0, 4]:
            t = self.selectReplTask(volL, curTime)
            if t:
                logd('selectReplTask', t)
                return t
        # step 5
        if g_step in [0, 5]:
            t = self.selectMergeTask2(volActTimeL, numDiffL, curTime)
            if t:
                logd('selectMergeTask2', t)
                return t
        return None

class TaskManager:
    def __init__(self, max_task, max_repl_task):
        """
            exec one task(name='merge', 'apply', 'repl')  for each vol
            total num <= max_task
            total repl num <= max_replication_task
        """
        verify_type(max_task, int)
        verify_type(max_repl_task, int)
        self.max_task = max_task
        self.max_repl_task = max_repl_task
        self.tasks = {} # map<vol, name>
        self.handles = {} # map<hdl, vol>
        self.mutex = threading.Lock()
        self.repl_task_num = 0 # current repl task num

    def getNonActiveList(self, volD):
        """
            remove active vol from volD and return volL
        """
        verify_type(volD, dict)
        with self.mutex:
            return [(k, volD[k]) for k in volD.viewkeys() - self.tasks.viewkeys()]

    def tryRun(self, task, args=()):
        """
            run worker(args)
        """
        verify_type(task, Task)
        verify_type(args, tuple)
        vol = task.vol
        name = task.name
        target = task.run

        with self.mutex:
            if self.tasks.has_key(vol):
                return False
            if len(self.tasks) >= self.max_task:
                return False
            if self.repl_task_num >= self.max_repl_task:
                return False
            if name == 'repl':
                self.repl_task_num += 1

            def wrapperTarget(*args, **kwargs):
                """
                    call target(args) and remove own handle in tasks
                """
                target = kwargs['target']
                try:
                    if args:
                        target(*args)
                    else:
                        target()
                    logi('endTask  ', task)
                except Exception, e:
                    loge('errTask  ', task, e)
                finally:
                    ref_hdl = kwargs['ref_hdl']
                    tasks = kwargs['tasks']
                    handles = kwargs['handles']

                    with self.mutex:
                        hdl = ref_hdl[0]
                        vol = handles[hdl]
                        name = tasks.pop(vol)
                        if name == 'repl':
                            self.repl_task_num -= 1
                        handles.pop(hdl)

            ref_hdl = [] # use [] to set the value later
            kwargs = {
                'ref_hdl':ref_hdl,
                'tasks':self.tasks,
                'handles':self.handles,
                'target':target
            }
            hdl = threading.Thread(target=wrapperTarget, args=args, kwargs=kwargs)
            ref_hdl.append(hdl)
            self.tasks[vol] = name
            self.handles[hdl] = vol

        # lock is unnecessary
        logi('startTask', task)
        hdl.start()
        return True

    def join(self):
        cur = threading.current_thread()
        for th in threading.enumerate():
            if th != cur:
                th.join()
        with self.mutex:
            if len(self.handles) > 0:
                loge("TaskManager.join err handle", len(self.handles))
            if self.repl_task_num != 0:
                loge("TaskManager.join err repl_task_num", self.repl_task_num)


def usage():
    print "walb-worker -f configName [opt]"
    print "    -f configName ; load config ; load from stdin if configName = '-'"
    print "    -d ; for debug"
    print "    (the following options are for only debug)"
    print "    -step num ; only select step of task"
    print "    -lifetime seconds : lifetime of worker"
    print "    -no : select task but no action"
    exit(1)

def workerMain(cfg, verbose=False, step=0, lifetime=0, noAction=False):
    verify_type(cfg, Config)
    verify_type(verbose, bool)
    verify_type(step, int)
    verify_type(lifetime, int)
    global g_verbose
    global g_step
    global g_quit
    logi('verbose', verbose, 'step', step, 'lifetime', lifetime)
    g_verbose = verbose
    g_step = step
    startTime = getCurrentTime()

    cfg.verify()
    w = Worker(cfg)
    manager = TaskManager(cfg.general.max_task, cfg.general.max_replication_task)
    logStart(sys.argv, cfg)
    while not g_quit:
        task = None
        for i in xrange(g_retryNum):
            if g_quit:
                break
            try:
                volActTimeD = w.walbc.get_vol_dict_without_running_actions(w.a0)
                volActTimeL = manager.getNonActiveList(volActTimeD)
                curTime = getCurrentTime()
                if lifetime > 0 and curTime - startTime > datetime.timedelta(seconds=lifetime):
                    logd('lifetime ends')
                    g_quit = True
                    break
                task = w.selectTask(volActTimeL, curTime)
                break
            except Exception, e:
                loge('err', e)
                time.sleep(10)
        else:
            loge('max retryNum')
            os._exit(1)
        if not noAction and task:
            b = manager.tryRun(task, (w.walbc,))
            if b:
                continue
            logd("task is canceled(max limit)", task)
        logd('no task')
        time.sleep(cfg.general.kick_interval)
    manager.join()
    logEnd()

def main():
    configNames = []
    verbose = False
    step = 0
    lifetime = 0
    noAction = False
    i = 1
    argv = sys.argv
    argc = len(argv)
    while i < argc:
        c = argv[i]
        if c == '-f' and i < argc - 1:
            configNames.append(argv[i + 1])
            i += 2
            continue
        if c == '-step' and i < argc - 1:
            step = int(argv[i + 1])
            i += 2
            continue
        if c == '-lifetime' and i < argc - 1:
            lifetime = int(argv[i + 1])
            i += 2
            continue
        if c == '-no':
            noAction = True
            i += 1
            continue
        if c == '-d':
            verbose = True
            i += 1
            continue
        else:
            print "option error", argv[i]
            usage()

    if not configName:
        print "set -f option"
        usage()

    cfg = Config()
    for name in configNames:
        cfg.load(name)
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, quitHandler)
    workerMain(cfg, verbose, step, lifetime, noAction)

if __name__ == "__main__":
    main()
