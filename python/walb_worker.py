#!/usr/bin/env python
import sys, time, datetime, os, threading, signal, traceback
from walblib import *
import walblib.worker as worker
from collections import defaultdict
import json

walbcDebug = False
g_quit = False
g_verbose = 0  # -v: 1 -vv: 2
g_retryNum = 3
g_step = 0

OLDEST_TIME = datetime.datetime(2000, 1, 1, 0, 0)

def getCurrentTime():
    return datetime.datetime.utcnow()

WARN = 'WARN'
INFO = 'INFO'
ERR = 'ERROR'
DEBUG = 'DEBUG'

def verbose2():
    return g_verbose >= 2

def verbose1():
    return g_verbose >= 1

def log(mode, *s):
    print getCurrentTime(), mode, ":".join(map(str,s))
    sys.stdout.flush()

def logdd(*s):
    if not verbose2():
        return
    log(DEBUG, *s)

def logd(*s):
    if not verbose1():
        return
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


def volActTimeD2Str(volTimeActD):
    '''
    Args:
        volTimeActD: {str:{str:datetime or None}}
    Returns:
        str
    '''
    ret = {}
    for vol, d1 in volTimeActD.iteritems():
        d2 = {}
        for act, dt in d1.iteritems():
            if dt is None:
                d2[act] = None
            else:
                verify_type(dt, datetime.datetime)
                d2[act] = str(dt)
    ret[vol] = d2
    return json.dumps(ret)


class ExecedRepl:
    def __init__(self, vol, rs, ts):
        verify_type(vol, str)
        verify_type(rs, worker.ReplServerConfig)
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
    verify_int(max_send_size)
    verify_int(a1latest)
    if max_send_size == 0:
        return None

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
    end = i
    if begin == end:
        end = begin + 1
    return diffL[end].E.gidB

def sumDiffSize(diffL):
    return sum([d.dataSize for d in diffL])


def getMergeGidRange(diffL, max_size, max_nr, notMergeGidL):
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

    def IsAnyOfGidLInRange(gidL, begin, end):
        for gid in gidL:
            if begin < gid and gid < end:
                return True
        return False

    for i in xrange(len(diffL)):
        diff = diffL[i]
        canBeMerged = not diff.isCompDiff and diff.dataSize <= max_size
        if inApplying:
            inRange = IsAnyOfGidLInRange(notMergeGidL, diffL[begin].B.gidB, diff.E.gidB)
            toBeAppended = diff.isMergeable and canBeMerged and not inRange
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
    def __init__(self, name, vol, ax, callback=None):
        verify_type(name, str)
        verify_type(vol, str)
        verify_server_kind(ax, [K_ARCHIVE])
        verify_function(callback, allowNone=True)
        if name not in ['merge', 'apply', 'repl']:
            raise Exception('Task bad name', name, vol)
        self.name = name
        self.vol = vol
        self.ax = ax
        self._callback = callback
    def run(self):
        pass
    def callback(self):
        if self._callback is not None:
            self._callback()
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
    def __init__(self, vol, ax, rs, maxGid=None, updateTime=None):
        Task.__init__(self, 'repl', vol, ax, updateTime)
        verify_type(rs, worker.ReplServerConfig)
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
        verify_type(cfg, worker.Config)
        self.cfg = cfg
        self.serverLayout = self.createSeverLayout(self.cfg)
        self.walbc = Ctl(self.cfg.general.walbc_path, self.serverLayout, walbcDebug)
        self.doneReplServerList = {}

    def selectApplyTask1(self, volL):
        for vol in volL:
            ms = self.walbc.get_base(self.a0, vol)
            logdd('selectApplyTask1', vol, ms)
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
        logdd('selectApplyTask2', ls)
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

    def getNotMergeGidLL(self, volL):
        ts_deltaL = self.walbc.get_ts_delta(self.a0)
        vol_gid = defaultdict(list)
        for d in ts_deltaL:
            vol = d['name']
            dest_gid = int(d['dest_gid'])
            vol_gid[vol].append(dest_gid)
        ret = []
        for vol in volL:
            ret.append(vol_gid[vol])
        return ret

    def selectMaxDiffNumMergeTask(self, ls):
        """
            ls = [(n, vol, notMergeGidL)]
        """
        verify_type(ls, list, tuple)
        if ls:
            verify_type(ls[0][0], int)
            verify_type(ls[0][1], str)
            verify_type(ls[0][2], list)
            ls.sort(key=lambda x : x[0], reverse=True)
            (_, vol, notMergeGidL) = ls[0]
            diffL = self.walbc.get_applicable_diff_list(self.a0, vol)
            r = getMergeGidRange(diffL, self.cfg.merge.max_size, self.cfg.merge.max_nr, notMergeGidL)
            if r:
                return MergeTask(vol, self.a0, r[0], r[1])
        return None

    def selectMergeTask1(self, volL, numDiffL, notMergeGidLL):
        ls = []
        for (vol, n, notMergeGidL) in zip(volL, numDiffL, notMergeGidLL):
            if n >= self.cfg.merge.threshold_nr:
                ls.append((n, vol, notMergeGidL))
        logdd('selectMergeTask1', ls)
        return self.selectMaxDiffNumMergeTask(ls)

    def selectMergeTask2(self, volActTimeL, numDiffL, notMergeGidLL, curTime):
        ls = []
        for ((vol, actTimeD), n, notMergeGidL) in zip(volActTimeL, numDiffL, notMergeGidLL):
            ts = actTimeD.get(aaMerge)
            if ts and curTime < ts + self.cfg.merge.interval:
                continue
            ls.append((n, vol, notMergeGidL))
        logdd('selectMergeTask2', ls)
        return self.selectMaxDiffNumMergeTask(ls)

    def selectReplTask(self, volL, curTime):
        tL = []
        rsL = self.cfg.repl.getEnabledList()
        logdd('selectReplTask', 'rsL', [(rs.name, rs.log_name, rs.addr, rs.port) for rs in rsL])
        for vol in volL:
            if vol in self.cfg.repl.disabled_volumes:
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
        logdd('selectReplTask', 'tL', [(x[0], x[1], x[2].name, x[3]) for x in tL])
        if tL:
            tL.sort(key=lambda x:x[0])
            (_, vol, rs, a1latest) = tL[0]
            diffL = self.walbc.get_applicable_diff_list(self.a0, vol)
            maxGid = getGidToRepl(diffL, rs.max_send_size, a1latest)
            def updateTime():
                self.doneReplServerList[(vol, rs)] = curTime
            return ReplTask(vol, self.a0, rs, maxGid, updateTime)
        else:
            return None

    def selectTask(self, volActTimeL, curTime):
        volL = map(lambda x:x[0], volActTimeL)
        if verbose2():
            logd('selectTask', 'volL', volL)
        numDiffL = self.getNumDiffList(volL)
        if verbose2():
            logd('selectTask', 'numDiffL', numDiffL)
        notMergeGidLL = self.getNotMergeGidLL(volL)
        if verbose2():
            logd('selectTask', 'notMergeGidLL', notMergeGidLL)
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
            t = self.selectMergeTask1(volL, numDiffL, notMergeGidLL)
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
            t = self.selectMergeTask2(volActTimeL, numDiffL, notMergeGidLL, curTime)
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
    print "    -v ; for verbose message"
    print "    -vv ; for more verbose message"
    print "    (the following options are for only debug)"
    print "    -step num ; only select step of task"
    print "    -lifetime seconds : lifetime of worker"
    print "    -no : select task but no action"
    exit(1)

def workerMain(cfg, verbose=0, step=0, lifetime=0, noAction=False):
    verify_type(cfg, worker.Config)
    verify_type(verbose, int)
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
                if verbose2():
                    logd('volActTimeD', volActTimeD2Str(volActTimeD))
                volActTimeL = manager.getNonActiveList(volActTimeD)
                curTime = getCurrentTime()
                if lifetime > 0 and curTime - startTime > datetime.timedelta(seconds=lifetime):
                    logd('lifetime ends')
                    g_quit = True
                    break
                task = w.selectTask(volActTimeL, curTime)
                break
            except Exception:
                loge('err', traceback.format_exc())
                time.sleep(10)
        else:
            loge('max retryNum')
            os._exit(1)
        if not noAction and task:
            b = manager.tryRun(task, (w.walbc,))
            if b:
                task.callback()
                continue
            logd("task is canceled(max limit)", task)
        logd('no task')
        time.sleep(cfg.general.kick_interval)
    manager.join()
    logEnd()

def main():
    configNames = []
    verbose = 0
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
        if c == '-v':
            verbose = 1
            i += 1
            continue
        if c == '-vv':
            verbose = 2
            i += 1
            continue
        else:
            print "option error", argv[i]
            usage()

    if not configNames:
        print "set -f option"
        usage()

    cfg = worker.Config()
    for name in configNames:
        cfg.load(name)
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, quitHandler)
    workerMain(cfg, verbose, step, lifetime, noAction)

if __name__ == "__main__":
    main()
