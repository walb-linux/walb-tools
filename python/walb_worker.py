#!/usr/bin/env python
import sys, time, yaml, datetime, collections, os, threading
from walblib import *

walbcDebug = False
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

def logd(*s):
    log(DEBUG, *s)

def loge(*s):
    log(ERR, *s)

def logi(*s):
    log(INFO, *s)

def logstart(argv, cfg):
    logi('start walb-worker')
    print ' '.join(argv)
    print '>>>\n' + str(cfg) + '<<<'

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
    """
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
    if d <= 0 or d > 65535:
        raise Exception('parsePort', d)
    return d

class General:
    def __init__(self):
        self.addr = ""
        self.port = 0
        self.walbc_path = ''
        self.max_task = 1
        self.max_replication_task = 1
        self.kick_interval = 1

    def set(self, d):
        verify_type(d, dict)
        self.addr = d['addr']
        verify_type(self.addr, str)
        self.port = parsePort(d['port'])
        self.walbc_path = d['walbc_path']
        verify_type(self.walbc_path, str)
        if not os.path.exists(self.walbc_path):
            raise Exception('walbc_path is not found', self.walbc_path)
        self.max_task = parsePositive(d['max_task'])
        if d.has_key('max_replication_task'):
            self.max_replication_task = parsePositive(d['max_replication_task'])
        if d.has_key('kick_interval'):
            self.kick_interval = parsePositive(d['kick_interval'])

    def __str__(self):
        return "addr={} port={} max_task={} max_replication_task={} kick_interval={}".format(self.addr, self.port, self.max_task, self.max_replication_task, self.kick_interval)

class Apply:
    def __init__(self):
        self.keep_period = datetime.timedelta()
        self.interval = datetime.timedelta(days=1)
        self.time_window = (0, 0)
    def set(self, d):
        verify_type(d, dict)
        self.keep_period = parsePERIOD(d['keep_period'])
        if d.has_key('interval'):
            self.interval = parsePERIOD(d['interval'])
    def __str__(self):
        return "keep_period={}".format(self.keep_period)

class Merge:
    def __init__(self):
        self.interval = datetime.timedelta()
        self.max_nr = UINT64_MAX
        self.max_size = UINT64_MAX
        self.threshold_nr = UINT64_MAX
    def set(self, d):
        verify_type(d, dict)
        self.interval = parsePERIOD(d['interval'])
        if d.has_key('max_nr'):
            self.max_nr = parsePositive(d['max_nr'])
        if d.has_key('max_size'):
            self.max_size = parseSIZE_UNIT(d['max_size'])
        if d.has_key('threshold_nr'):
            self.threshold_nr = parsePositive(d['threshold_nr'])
    def __str__(self):
        return "interval={} max_nr={} max_size={} threshold_nr={}".format(self.interval, self.max_nr, self.max_size, self.threshold_nr)

class ReplServer:
    def __init__(self):
        self.addr = ""
        self.port = 0
        self.interval = 0
        self.compress = None
        self.max_merge_size = '1G'
        self.bulk_size = '64K'
    def set(self, name, d):
        verify_type(name, str)
        verify_type(d, dict)
        self.name = name
        self.addr = d['addr']
        verify_type(self.addr, str)
        self.port = parsePort(d['port'])
        self.interval = parsePERIOD(d['interval'])
        if d.has_key('compress'):
            self.compress = CompressOpt()
            self.compress.parse(d['compress'])
        if d.has_key('max_merge_size'):
            self.max_merge_size = str(d['max_merge_size'])
        if d.has_key('bulk_size'):
            self.bulk_size = str(d['bulk_size'])
    def __str__(self):
        return "name={} addr={} port={} interval={} compress={} max_merge_size={} bulk_size={}".format(self.name, self.addr, self.port, self.interval, self.compress, self.max_merge_size, self.bulk_size)
    def getServerConnectionParam(self):
        return ServerConnectionParam(self.name, self.addr, self.port, K_ARCHIVE)

class Config:
    def __init__(self):
        self.general = General()
        self.apply_ = Apply()
        self.merge = Merge()
        self.repl_servers = {}

    def set(self, d):
        verify_type(d, dict)
        self.general.set(d['general'])
        self.apply_.set(d['apply'])
        self.merge.set(d['merge'])
        ss = d.get('repl_servers')
        if ss:
            for (name, v) in ss.items():
                rs = ReplServer()
                rs.set(name, v)
                self.repl_servers[name] = rs

    def __str__(self):
        s = "general\n"
        s += str(self.general) + '\n'
        s += "apply\n"
        s += str(self.apply_) + '\n'
        s += "merge\n"
        s += str(self.merge) + '\n'
        s += 'repl_servers\n'
        for (name, rs) in self.repl_servers.items():
            s += name + ':' + str(rs) + '\n'
        return s

def loadConfig(configName):
    verify_type(configName, str)
    s = ''
    with open(configName) as f:
        s = f.read().decode('utf8')
    d = yaml.load(s)
    cfg = Config()
    cfg.set(d)
    return cfg

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

def sumDiffSize(diffL):
    return sum([d.dataSize for d in diffL])

def getMergeGidRange(diffL, max_size):
    """
        diffL must satisfy the following conditions:
        [L0, L1, ...]
        L0 : canBeMerged
        L1, L2, ... : toBeAppended
        len >= 2
    """
    diffLL = []
    inAppending = False
    begin = None
    for i in xrange(len(diffL)):
        diff = diffL[i]
        canBeMerged = not diff.isCompDiff and diff.dataSize < max_size
        toBeAppended = diff.isMergeable and canBeMerged
        if inAppending and not toBeAppended:
            if i - begin >= 2:
                diffLL.append(diffL[begin:i])
            inAppending = False
        if not inAppending and canBeMerged:
            begin = i
            inAppending = True
    if inAppending and len(diffL) - begin >= 2:
        diffLL.append(diffL[begin:])

    if not diffLL:
        return None

    def f(diffL):
        return (sumDiffSize(diffL) / len(diffL), diffL[0].B.gidB, diffL[-1].E.gidB)
    tL = map(f, diffLL)
    tL.sort(key=lambda x:x[0])
    (_, gidB, gidE) = tL[0]
    return (gidB, gidE)

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
        return "Task name={} vol={} ax={}".format(self.name, self.vol, self.ax)
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
    def __init__(self, vol, ax, rs):
        Task.__init__(self, 'repl', vol, ax)
        verify_type(rs, ReplServer)
        self.dst = rs.getServerConnectionParam()
        self.syncOpt = SyncOpt(cmprOpt=rs.compress, maxWdiffMergeSizeU=rs.max_merge_size, bulkSizeU=rs.bulk_size)
    def run(self, walbc):
        verify_type(walbc, Controller)
        walbc.replicate_once(self.ax, self.vol, self.dst, syncOpt=self.syncOpt)
    def __str__(self):
        return Task.__str__(self) + " dst={} opt={}".format(self.dst, self.syncOpt)
    def __eq__(self, rhs):
        # QQQ : add self.syncOpt == rhs.syncOpt?
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
        self.doneReplServerList = collections.defaultdict()
        self.doneApplyList = collections.defaultdict()

    def selectApplyTask1(self, volL):
        for vol in volL:
            ms = self.walbc.get_base(self.a0, vol)
            if ms.is_applying():
                return ApplyTask(vol, self.a0, ms.E.gidB)
        return None

    def selectApplyTask2(self, volL, curTime):
        '''
            get (vol, gid) having max diff
        '''
        ls = []
        for vol in volL:
            ts = self.doneApplyList.get(vol)
            interval = self.cfg.apply_.interval
            if ts and interval > datetime.timedelta() and curTime < ts + interval:
                continue
            infoL = self.walbc.get_restorable(self.a0, vol, 'all')
            gidInfo = getLatestGidInfoBefore(curTime - self.cfg.apply_.keep_period, infoL)
            if not gidInfo:
                continue
            gid = gidInfo.gid
            size = self.walbc.get_total_diff_size(self.a0, vol, gid1=gid)
            ls.append((size, vol, gid))
        if ls:
            ls.sort(key=lambda x : x[0])
            (size, vol, gid) = ls[-1]
            self.doneApplyList[vol] = curTime
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
            r = getMergeGidRange(diffL, self.cfg.merge.max_size)
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
        rsL = self.cfg.repl_servers.values()
        for vol in volL:
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
                tL.append((ts, vol, rs))
        if tL:
            tL.sort(key=lambda x:x[0])
            (_, vol, rs) = tL[0]
            self.doneReplServerList[(vol, rs)] = curTime
            return ReplTask(vol, self.a0, rs)
        else:
            return None

    def selectTask(self, volActTimeL, curTime):
        volL = map(lambda x:x[0], volActTimeL)
        numDiffL = self.getNumDiffList(volL)
        # step 1
        if g_step in [0, 1]:
            t = self.selectApplyTask1(volL)
            if t:
                return t
        # step 2
        if g_step in [0, 2]:
            t = self.selectApplyTask2(volL, curTime)
            if t:
                return t
        # step 3
        if g_step in [0, 3]:
            t = self.selectMergeTask1(volL, numDiffL)
            if t:
                return t
        # step 4
        if g_step in [0, 4]:
            t = self.selectReplTask(volL, curTime)
            if t:
                return t
        # step 5
        if g_step in [0, 5]:
            t = self.selectMergeTask2(volActTimeL, numDiffL, curTime)
        return t

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
    print "walb-worker [-f configName]"
    exit(1)

def main():
    configName = ""
    i = 1
    argv = sys.argv
    argc = len(argv)
    while i < argc:
        c = argv[i]
        if c == '-f' and i < argc - 1:
            configName = argv[i + 1]
            i += 2
            continue
        if c == '-step' and i < argc - 1:
            global g_step
            g_step = int(argv[i + 1])
            i += 2
            continue
        if c == '-d':
            global g_verbose
            g_verbose = True
            i += 1
            continue
        else:
            print "option error", argv[i]
            usage()

    if not configName:
        print "set -f option [-d]"
        print "  -d ; debug"
        usage()

    cfg = loadConfig(configName)
    w = Worker(cfg)
    manager = TaskManager(cfg.general.max_task, cfg.general.max_replication_task)
    logstart(sys.argv, cfg)
    while True:
        for i in xrange(g_retryNum):
            try:
                volActTimeD = w.walbc.get_vol_dict_without_running_actions(w.a0)
                volActTimeL = manager.getNonActiveList(volActTimeD)
                curTime = getCurrentTime()
                task = w.selectTask(volActTimeL, curTime)
                break
            except Exception, e:
                loge('err', e)
                time.sleep(10)
        else:
            loge('max retryNum')
            os._exit(1)
        if task:
            logd("selected task", task)
            b = manager.tryRun(task, (w.walbc,))
            if b:
                continue
            logd("task is canceled(max limit)", task)
        logd('no task')
        time.sleep(cfg.general.kick_interval)

if __name__ == "__main__":
    main()
