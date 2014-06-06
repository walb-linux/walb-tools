#pragma once
#include "walb_util.hpp"
#include "protocol.hpp"
#include "state_machine.hpp"
#include "atomic_map.hpp"
#include "action_counter.hpp"
#include "proxy_vol_info.hpp"
#include "wdiff_data.hpp"
#include "host_info.hpp"
#include "task_queue.hpp"
#include "tmp_file.hpp"
#include "walb_util.hpp"
#include "walb_diff_merge.hpp"
#include "walb_diff_compressor.hpp"
#include "walb_diff_converter.hpp"
#include "walb_diff_mem.hpp"
#include "walb_log_net.hpp"
#include "wdiff_transfer.hpp"

namespace walb {

class ActionState
{
    using Map = std::map<std::string, bool>;
    mutable Map map_;
    std::recursive_mutex &mu_;

public:
    explicit ActionState(std::recursive_mutex &mu)
        : mu_(mu) {
    }
    void clearAll() {
        UniqueLock ul(mu_);
        map_.clear();
    }
    void clear(const std::string &name) {
        UniqueLock ul(mu_);
        map_[name] = false;
    }
    void set(const std::string &name) {
        UniqueLock ul(mu_);
        map_[name] = true;
    }
    bool get(const std::string &name) const {
        UniqueLock ul(mu_);
        return map_[name];
    }
};

struct ProxyVolState
{
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac; // archive name is action identifier here.
    ActionState actionState;

    MetaDiffManager diffMgr; // for the master.
    AtomicMap<MetaDiffManager> diffMgrMap; // for each archive.

    /**
     * This is protected by state machine.
     * In Started/WlogRecv state, this is read-only and accessed by multiple threads.
     * Otherwise, this is writable and accessed by only one thread.
     */
    std::set<std::string> archiveSet;

    /**
     * Timestamp of the latest wdiff received from a storage server.
     * Lock of mu is required to access this.
     * 0 means invalid.
     */
    uint64_t lastWlogReceivedTime;
    /**
     * Timestamp of the latest wdiff sent to each archive server.
     * Lock of mu is required to access this.
     * Key is archiveName, value is the corresponding timestamp.
     */
    std::map<std::string, uint64_t> lastWdiffSentTimeMap;

    explicit ProxyVolState(const std::string &volId)
        : stopState(NotStopping), sm(mu), ac(mu), actionState(mu)
        , diffMgr(), diffMgrMap(), archiveSet()
        , lastWlogReceivedTime(0), lastWdiffSentTimeMap() {
        sm.init(statePairTbl);
        initInner(volId);
    }
private:
    void initInner(const std::string &volId);
};

struct ProxyTask
{
    std::string volId;
    std::string archiveName;

    ProxyTask() = default;
    ProxyTask(const std::string &volId, const std::string &archiveName)
        : volId(volId), archiveName(archiveName) {}
    bool operator==(const ProxyTask &rhs) const {
        return volId == rhs.volId && archiveName == rhs.archiveName;
    }
    bool operator<(const ProxyTask &rhs) const {
        int c = volId.compare(rhs.volId);
        if (c < 0) return true;
        if (c > 0) return false;
        return archiveName < rhs.archiveName;
    }
    std::string str() const {
        std::ostringstream ss;
        ss << "(" << volId << "," << archiveName << ")";
        return ss.str();
    }
    friend std::ostream& operator<<(std::ostream& os, const ProxyTask& task) {
        os << task.str();
        return os;
    }
};

class ProxyWorker
{
private:
    const ProxyTask task_;

    void setupMerger(diff::Merger& merger, MetaDiffVec& diffV, MetaDiff& mergedDiff, const ProxyVolInfo& volInfo, const std::string& archiveName);

public:
    explicit ProxyWorker(const ProxyTask &task) : task_(task) {
    }
    void operator()();
private:
    struct PushOpt
    {
        bool isForce;
        size_t delaySec;
    };
    int transferWdiffIfNecessary(PushOpt &);
};

struct ProxySingleton
{
    static ProxySingleton& getInstance() {
        static ProxySingleton instance;
        return instance;
    }

    /**
     * Read-only except for daemon initialization.
     */
    std::string nodeId;
    std::string baseDirStr;
    size_t maxWdiffSendMb;
    size_t delaySecForRetry;
    size_t retryTimeout;
    size_t maxForegroundTasks;
    size_t maxConversionMb;
    size_t socketTimeout;

    /**
     * Writable and must be thread-safe.
     */
    std::atomic<bool> forceQuit;
    AtomicMap<ProxyVolState> stMap;
    TaskQueue<ProxyTask> taskQueue;
    std::unique_ptr<DispatchTask<ProxyTask, ProxyWorker> > dispatcher;
    std::atomic<uint64_t> conversionUsageMb;
};

inline ProxySingleton& getProxyGlobal()
{
    return ProxySingleton::getInstance();
}

const ProxySingleton& gp = getProxyGlobal();

/**
 * This is called just one time and by one thread.
 * You need not take care about thread-safety inside this function.
 */
inline void ProxyVolState::initInner(const std::string &volId)
{
    cybozu::FilePath volDir(gp.baseDirStr);
    volDir += volId;

    ProxyVolInfo volInfo(gp.baseDirStr, volId, diffMgr, diffMgrMap, archiveSet);
    if (!volInfo.existsVolDir()) {
        sm.set(pClear);
        return;
    }

    sm.set(pStopped);
    volInfo.loadAllArchiveInfo();

    // Retry to make hard links of wdiff files in the master directory.
    MetaDiffVec diffV = volInfo.getAllDiffsInMaster();
    LOGs.debug() << "found diffs" << volId << diffV.size(); // debug
    for (const MetaDiff &d : diffV) {
        LOGs.debug() << "try to make hard link" << d; // debug
        volInfo.tryToMakeHardlinkInSlave(d);
    }
    volInfo.deleteDiffs(diffV);
    // Here the master directory must contain no wdiff file.
    if (!diffMgr.getAll().empty()) {
        throw cybozu::Exception("ProxyVolState::initInner")
            << "there are wdiff files in the master directory"
            << volId;
    }
}

inline ProxyVolState &getProxyVolState(const std::string &volId)
{
    return getProxyGlobal().stMap.get(volId);
}

namespace proxy_local {

class ConversionMemoryTransaction
{
private:
    size_t sizeMb_;
public:
    explicit ConversionMemoryTransaction(size_t sizeMb)
        : sizeMb_(sizeMb) {
        getProxyGlobal().conversionUsageMb += sizeMb_;
    }
    ~ConversionMemoryTransaction() noexcept {
        getProxyGlobal().conversionUsageMb -= sizeMb_;
    }
};

inline void verifyMaxConversionMemory(const char *msg)
{
    if (gp.conversionUsageMb > gp.maxConversionMb) {
        throw cybozu::Exception(msg)
            << "exceeds max conversion memory size in MB" << gp.maxConversionMb;
    }
}

inline StrVec getAllStateStrVec()
{
    StrVec ret;
    const auto &fmt = cybozu::util::formatString;

    const std::vector<std::pair<ProxyTask, int64_t> > tqv = gp.taskQueue.getAll();
    ret.push_back(fmt("TaskQueue %zu", tqv.size()));
    for (const auto &pair : tqv) {
        const ProxyTask &task = pair.first;
        const int64_t &timeDiffMs = pair.second;
        std::stringstream ss;
        ss << "volume " << task.volId
           << " archiveName " << task.archiveName
           << " timeDiffMs " << timeDiffMs;
        ret.push_back(ss.str());
    }

    for (const std::string &volId : getProxyGlobal().stMap.getKeyList()) {
        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);
        const ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
        const std::string state = volSt.sm.get();
        const uint64_t totalSize = volInfo.getTotalDiffFileSize();
        const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
        const std::string tsStr = util::timeToPrintable(volSt.lastWlogReceivedTime);
        ret.push_back(
            fmt("%s %s %zu %s %s"
                , volId.c_str(), state.c_str(), volSt.diffMgr.size()
                , totalSizeStr.c_str(), tsStr.c_str()));

        const std::vector<int> actionNum = volSt.ac.getValues(volSt.archiveSet);
        size_t i = 0;
        for (const auto& archiveName : volSt.archiveSet) {
            const MetaDiffManager &mgr = volSt.diffMgrMap.get(archiveName);
            const uint64_t totalSize = volInfo.getTotalDiffFileSize(archiveName);
            const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
            uint64_t minGid, maxGid;
            std::tie(minGid, maxGid) = mgr.getMinMaxGid();
            const std::string tsStr = util::timeToPrintable(volSt.lastWdiffSentTimeMap[archiveName]);
            ret.push_back(
                fmt("  %s %s %zu %s %" PRIu64 " %" PRIu64 " %s"
                    , archiveName.c_str()
                    , actionNum[i] == 0 ? "None" : "WdiffSend"
                    , mgr.size(), totalSizeStr.c_str(), minGid, maxGid
                    , tsStr.c_str()));
            i++;
        }
    }
    return ret;
}

inline StrVec getVolStateStrVec(const std::string &volId)
{
    StrVec ret;
    const auto &fmt = cybozu::util::formatString;

    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    const ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);

    const std::string state = volSt.sm.get();
    const size_t num = volSt.diffMgr.size();
    const uint64_t totalSize = volInfo.getTotalDiffFileSize();
    const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
    const std::string tsStr = util::timeToPrintable(volSt.lastWlogReceivedTime);

    ret.push_back(fmt("volId %s", volId.c_str()));
    ret.push_back(fmt("state %s", state.c_str()));
    ret.push_back(fmt("num %zu", num));
    ret.push_back(fmt("totalSize %s", totalSizeStr.c_str()));
    ret.push_back(fmt("timestamp %s", tsStr.c_str()));

    const std::vector<int> actionNum = volSt.ac.getValues(volSt.archiveSet);
    size_t i = 0;
    for (const std::string& archiveName : volSt.archiveSet) {
        const MetaDiffManager &mgr = volSt.diffMgrMap.get(archiveName);
        const HostInfoForBkp hi = volInfo.getArchiveInfo(archiveName);
        const std::string tsStr = util::timeToPrintable(volSt.lastWdiffSentTimeMap[archiveName]);

        ret.push_back(fmt("archive %s %s", archiveName.c_str(), hi.str().c_str()));
        ret.push_back(fmt("  action %s", actionNum[i] == 0 ? "None" : "WdiffSend"));
        ret.push_back(fmt("  num %zu", mgr.size()));
        ret.push_back(fmt("  timestamp %s", tsStr.c_str()));

        const MetaDiffVec diffV = mgr.getAll();
        uint64_t totalSize = 0;
        StrVec wdiffStrV;
        uint64_t minTs = -1;
        for (const MetaDiff &diff : diffV) {
            const uint64_t fsize = volInfo.getDiffFileSize(diff, archiveName);
            const std::string fsizeStr = cybozu::util::toUnitIntString(fsize);
            wdiffStrV.push_back(
                fmt("  wdiff %s %d %s %s"
                    , diff.str().c_str()
                    , diff.isMergeable ? 1 : 0
                    , cybozu::unixTimeToStr(diff.timestamp).c_str()
                    , fsizeStr.c_str()));
            totalSize += fsize;
            if (minTs > diff.timestamp) minTs = diff.timestamp;
        }
        const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
        ret.push_back(fmt("  totalSize %s", totalSizeStr.c_str()));
        uint64_t sendDelay = 0;
        if (!diffV.empty()) sendDelay = ::time(0) - minTs;
        ret.push_back(fmt("  wdiffSendDelay %" PRIu64 "", sendDelay));
        for (const std::string &s : wdiffStrV) ret.push_back(s);

        i++;
    }
    return ret;
}

inline void pushAllTasksForVol(const std::string &volId)
{
    ProxyVolState &volSt = getProxyVolState(volId);
    volSt.actionState.clearAll();
    for (const std::string& archiveName : volSt.archiveSet) {
        getProxyGlobal().taskQueue.push(ProxyTask(volId, archiveName));
    }
}

} // namespace proxy_local

inline void c2pGetStateServer(protocol::ServerParams &p)
{
    protocol::c2xGetStateServer(p, getProxyVolState, gp.nodeId, __func__);
}

/**
 * params:
 *   [0]: volId or none.
 */
inline void c2pStatusServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        StrVec stStrV;
        if (v.empty()) {
            stStrV = proxy_local::getAllStateStrVec();
        } else {
            const std::string &volId = v[0];
            stStrV = proxy_local::getVolStateStrVec(volId);
        }
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(stStrV);
        logger.debug() << "status succeeded";
        packet::Ack(p.sock).sendFin();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2pListVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = util::getDirNameList(gp.baseDirStr);
    protocol::sendStrVec(p.sock, v, 0, FUNC);
    packet::Ack(p.sock).sendFin();
    ProtocolLogger logger(gp.nodeId, p.clientId);
    logger.debug() << "listVol succeeded";
}

inline void startProxyVol(const std::string &volId)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);
    const std::string &st = volSt.sm.get();
    if (st != pStopped) {
        throw cybozu::Exception("bad state") << st;
    }

    StateMachineTransaction tran(volSt.sm, pStopped, ptStart);
    proxy_local::pushAllTasksForVol(volId);
    tran.commit(pStarted);
}

namespace proxy_local {

inline bool hasDiffs(ProxyVolState &volSt)
{
    UniqueLock ul(volSt.mu);
    if (!volSt.diffMgr.empty()) return true;
    for (const std::string &archiveName : volSt.archiveSet) {
        if (!volSt.diffMgrMap.get(archiveName).empty()) return true;
    }
    return false;
}

/**
 * pStarted --> ptWaitForEmpty --> pStopped.
 */
inline void stopAndEmptyProxyVol(const std::string &volId)
{
    const char *const FUNC = __func__;
    ProxySingleton &g = getProxyGlobal();
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);

    Stopper stopper(volSt.stopState);
    if (!stopper.changeFromNotStopping(WaitingForEmpty)) {
        throw cybozu::Exception(FUNC) << "already under stopping wlog receiver" << volId;
    }

    waitUntil(ul, [&]() {
            return isStateIn(volSt.sm.get(), {pClear, pStopped, pStarted});
        }, FUNC);

    verifyStateIn(volSt.sm.get(), {pStarted}, FUNC);
    StateMachineTransaction tran(volSt.sm, pStarted, ptWaitForEmpty);

    waitUntil(ul, [&]() {
            const bool hasDiffs = proxy_local::hasDiffs(volSt);
            if (hasDiffs) {
                for (const std::string &archiveName : volSt.archiveSet) {
                    g.taskQueue.push(ProxyTask(volId, archiveName));
                }
            }
            return !hasDiffs;
        }, FUNC);

    if (!stopper.changeFromWaitingForEmpty(Stopping)) {
        throw cybozu::Exception(FUNC) << "BUG : not here, already under stopping wdiff sender" << volId;
    }

    verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);

    // Clear all related tasks from the task queue.
    g.taskQueue.remove([&](const ProxyTask &task) {
            return task.volId == volId;
        });
    tran.commit(pStopped);
}

/**
 *   pStarted --> pStopped
 */
inline void stopProxyVol(const std::string &volId, bool isForce)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);

    Stopper stopper(volSt.stopState);
    if (!stopper.changeFromNotStopping(isForce ? ForceStopping : Stopping)) {
        throw cybozu::Exception(FUNC) << "already under stopping wlog" << volId;
    }

    waitUntil(ul, [&]() {
            if (!volSt.ac.isAllZero(volSt.archiveSet)) return false;
            return isStateIn(volSt.sm.get(), {pClear, pStopped, pStarted});
        }, FUNC);

    const std::string &stFrom = volSt.sm.get();
    if (stFrom != pStarted) {
        throw cybozu::Exception(FUNC) << "bad state" << stFrom;
    }

    StateMachineTransaction tran(volSt.sm, stFrom, ptStop, FUNC);
    ul.unlock();

    // Clear all related tasks from the task queue.
    getProxyGlobal().taskQueue.remove([&](const ProxyTask &task) {
            return task.volId == volId;
        });

    tran.commit(pStopped);
}

} // proxy_local

/**
 * params:
 *   [0]: volId
 *
 * State transition: Stopped --> Start --> Started.
 */
inline void c2pStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
    const std::string &volId = v[0];
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        startProxyVol(volId);
        pkt.writeFin(msgOk);
        sendErr = false;
        logger.info() << "start succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * params:
 *   [0]: volId
 *   [1]: StopOpt as string (optional)
 *
 * State transition: Started --> Stop --> Stopped.
 * In addition, this will stop all background tasks before changing state.
 */
inline void c2pStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        std::string volId;
        StopOpt stopOpt;
        std::tie(volId, stopOpt) = parseStopParams(v, FUNC);
        pkt.writeFin(msgAccept);
        sendErr = false;

        if (stopOpt.isEmpty()) {
            proxy_local::stopAndEmptyProxyVol(volId);
        } else {
            proxy_local::stopProxyVol(volId, stopOpt.isForce());
        }
        logger.info() << "stop succeeded" << volId << stopOpt;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

namespace proxy_local {

inline void listArchiveInfo(const std::string &volId, StrVec &archiveNameV)
{
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    archiveNameV.assign(volSt.archiveSet.begin(), volSt.archiveSet.end());
}

inline void getArchiveInfo(const std::string& volId, const std::string &archiveName, HostInfoForBkp &hi)
{
    const char *const FUNC = __func__;
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    if (!volInfo.existsArchiveInfo(archiveName)) {
        throw cybozu::Exception(FUNC) << "archive info not exists" << archiveName;
    }
    hi = volInfo.getArchiveInfo(archiveName);
}

inline void addArchiveInfo(const std::string &volId, const std::string &archiveName, const HostInfoForBkp &hi, bool ensureNotExistance)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);
    const std::string &curr = volSt.sm.get(); // pStopped or pClear

    StateMachineTransaction tran(volSt.sm, curr, ptAddArchiveInfo);
    ul.unlock();
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    if (curr == pClear) volInfo.init();
    volInfo.addArchiveInfo(archiveName, hi, ensureNotExistance);
    tran.commit(pStopped);
}

inline void deleteArchiveInfo(const std::string &volId, const std::string &archiveName)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);

    StateMachineTransaction tran(volSt.sm, pStopped, ptDeleteArchiveInfo);
    ul.unlock();
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    volInfo.deleteArchiveInfo(archiveName);
    ul.lock();
    bool shouldClear = volInfo.notExistsArchiveInfo();
    if (shouldClear) volInfo.clear();
    tran.commit(shouldClear ? pClear : pStopped);
}

} // namespace proxy_local

/**
 * params:
 *   [0]: add/delete/update/get/info as string
 *   [1]: volId
 *   [2]: archive name
 *   [3]: serialized HostInfo data. (add/update only)
 *
 * State transition.
 *   (1) Clear --> AddArchiveInfo --> Stopped
 *   (2) Stopped --> X --> Stopped
 *       X is AddArchiveInfo/DeleteArchiveInfo/UpdateArchiveInfo.
 *   (3) Stopped --> DeleteArchiveInfo --> Clear
 */
inline void c2pArchiveInfoServer(protocol::ServerParams &p)
{
    const char * const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &cmd = v[0];
        const std::string &volId = v[1];
        logger.debug() << cmd << volId;

        std::string archiveName;
        HostInfoForBkp hi;
        if (cmd == "add" || cmd == "update") {
            pkt.read(archiveName);
            pkt.read(hi);
            logger.debug() << archiveName << hi;
            proxy_local::addArchiveInfo(volId, archiveName, hi, cmd == "add");
            logger.info() << "archive-info add/update succeeded" << volId << archiveName << hi;
            pkt.writeFin(msgOk);
            return;
        } else if (cmd == "get") {
            pkt.read(archiveName);
            proxy_local::getArchiveInfo(volId, archiveName, hi);
            logger.info() << "archive-info get succeeded" << volId << archiveName << hi;
            pkt.write(msgOk);
            sendErr = false;
            pkt.writeFin(hi);
            return;
        } else if (cmd == "delete") {
            pkt.read(archiveName);
            proxy_local::deleteArchiveInfo(volId, archiveName);
            logger.info() << "archive-info delete succeeded" << volId << archiveName;
            pkt.writeFin(msgOk);
            return;
        } else if (cmd == "list") {
            StrVec v;
            proxy_local::listArchiveInfo(volId, v);
            logger.info() << "archive-info list succeeded" << volId << v.size();
            pkt.write(msgOk);
            sendErr = false;
            pkt.writeFin(v);
            return;
        }
        throw cybozu::Exception(FUNC) << "invalid command name" << cmd;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * params:
 *   [0]: volId
 *
 * State transition: stopped --> ClearVol --> clear.
 */
inline void c2pClearVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
    const std::string &volId = v[0];
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);

        StateMachineTransaction tran(volSt.sm, pStopped, ptClearVol);
        volSt.archiveSet.clear();
        ul.unlock();
        ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
        volInfo.clear();
        tran.commit(pClear);
        pkt.writeFin(msgOk);
        sendErr = false;
        logger.info() << "clearVol succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

namespace proxy_local {

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool recvWlogAndWriteDiff(
    cybozu::Socket &sock, int fd, const cybozu::Uuid &uuid, uint32_t pbs, uint32_t salt,
    const std::atomic<int> &stopState, const std::atomic<bool> &forceQuit, Logger &logger)
{
    DiffMemory diffMem(DEFAULT_MAX_IO_LB);
    diffMem.header().setUuid(uuid);

    LogPackHeader packH(pbs, salt);

    log::Receiver receiver(sock, logger);
    receiver.setParams(pbs, salt);
    receiver.start();

    while (receiver.popHeader(packH)) {
        if (stopState == ForceStopping || forceQuit) {
            return false;
        }
        LogBlockShared blockS(pbs);
        for (size_t i = 0; i < packH.header().n_records; i++) {
            LogRecord &lrec = packH.record(i);
            receiver.popIo(lrec, blockS);
            DiffRecord drec;
            DiffIo diffIo;
            if (convertLogToDiff(pbs, lrec, blockS, drec, diffIo)) {
                diffMem.add(drec, std::move(diffIo));
            }
        }
    }
    diffMem.writeTo(fd);
    return true;
}

} // namespace proxy_local

/**
 * protocol
 *   recv parameters.
 *     volId
 *     uuid (cybozu::Uuid)
 *     pbs (uint32_t)
 *     salt (uint32_t)
 *     sizeLb (uint64_t)
 *   send "ok" or error message.
 *   recv wlog data
 *   recv diff (walb::MetaDiff)
 *   send ack.
 *
 * State transition: Started --> WlogRecv --> Started
 */
inline void s2pWlogTransferServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    std::string volId;
    cybozu::Uuid uuid;
    uint32_t pbs, salt;
    uint64_t volSizeLb, maxLogSizePb;

    packet::Packet pkt(p.sock);
    pkt.read(volId);
    pkt.read(uuid);
    pkt.read(pbs);
    pkt.read(salt);
    pkt.read(volSizeLb);
    pkt.read(maxLogSizePb);
    LOGs.debug() << "recv" << volId << uuid << pbs << salt << volSizeLb << maxLogSizePb;

    /* Decide to receive ok or not. */
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);

    ForegroundCounterTransaction foregroundTasksTran;
    proxy_local::ConversionMemoryTransaction convTran(maxLogSizePb * pbs / MEBI);
    try {
        verifyMaxForegroundTasks(gp.maxForegroundTasks, FUNC);
        proxy_local::verifyMaxConversionMemory(FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), {pStarted}, FUNC);
    } catch (std::exception &e) {
        logger.warn() << e.what();
        pkt.write(e.what());
        return;
    }
    pkt.write(msgAccept);

    StateMachineTransaction tran(volSt.sm, pStarted, ptWlogRecv);
    ul.unlock();

    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    cybozu::TmpFile tmpFile(volInfo.getMasterDir().str());
    if (!proxy_local::recvWlogAndWriteDiff(p.sock, tmpFile.fd(), uuid, pbs, salt,
                                           volSt.stopState, gp.forceQuit, logger)) {
        logger.warn() << FUNC << "force stopped wlog receiving" << volId;
        return;
    }
    MetaDiff diff;
    pkt.read(diff);
    if (!diff.isClean()) {
        throw cybozu::Exception(FUNC) << "diff is not clean" << diff;
    }
    tmpFile.save(volInfo.getDiffPath(diff).str());
    packet::Ack(p.sock).sendFin();

    ul.lock();
    volSt.actionState.clearAll();
    volInfo.addDiffToMaster(diff);
    volInfo.tryToMakeHardlinkInSlave(diff);
    volInfo.deleteDiffs({diff});
    for (const std::string &archiveName : volSt.archiveSet) {
        ProxyTask task(volId, archiveName);
        HostInfoForBkp hi = volInfo.getArchiveInfo(archiveName);
        getProxyGlobal().taskQueue.push(task, hi.wdiffSendDelaySec * 1000);
        logger.debug() << "task pushed" << task;
    }
    const uint64_t realSizeLb = volInfo.getSizeLb();
    if (realSizeLb < volSizeLb) {
        logger.info() << "detect volume grow" << realSizeLb << volSizeLb;
        volInfo.setSizeLb(volSizeLb);
    }
    volSt.lastWlogReceivedTime = ::time(0);
    tran.commit(pStarted);

    logger.info() << "wlog-transfer succeeded" << volId;
}

inline void ProxyWorker::setupMerger(diff::Merger& merger, MetaDiffVec& diffV, MetaDiff& mergedDiff, const ProxyVolInfo& volInfo, const std::string& archiveName)
{
    const char *const FUNC = __func__;
    const int maxRetryNum = 10;
    int retryNum = 0;
    cybozu::Uuid uuid;
    std::vector<cybozu::util::File> fileV;
retry:
    {
        diffV = volInfo.getDiffListToSend(archiveName, gp.maxWdiffSendMb * 1024 * 1024);
        if (diffV.empty()) return;
        // apply wdiff files indicated by diffV to lvSnap.
        for (const MetaDiff& diff : diffV) {
            cybozu::util::File file;
            if (!file.open(volInfo.getDiffPath(diff, archiveName).str(), O_RDONLY)) {
                retryNum++;
                if (retryNum == maxRetryNum) {
                    throw cybozu::Exception(FUNC) << "exceed max retry";
                }
                fileV.clear();
                goto retry;
            }
            diff::Reader reader(file.fd());
            DiffFileHeader header;
            reader.readHeaderWithoutReadingPackHeader(header);
            if (fileV.empty()) {
                uuid = header.getUuid();
                mergedDiff = diff;
            } else {
                if (uuid != header.getUuid()) {
                    diffV.resize(fileV.size());
                    break;
                }
                mergedDiff.merge(diff);
            }
            file.lseek(0, SEEK_SET);
            fileV.push_back(std::move(file));
        }
    }
    merger.addWdiffs(std::move(fileV));
    merger.prepare();
}

enum {
    DONT_SEND,
    CONTINUE_TO_SEND,
    SEND_ERROR,
};

/**
 * RETURN:
 *   DONT_SEND, CONTINUE_TO_SEND, or SEND_ERROR.
 */
inline int ProxyWorker::transferWdiffIfNecessary(PushOpt &pushOpt)
{
    const char *const FUNC = __func__;
    const std::string& volId = task_.volId;
    const std::string& archiveName = task_.archiveName;
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyStopState(volSt.stopState, NotStopping | WaitingForEmpty, volId, FUNC);
    verifyStateIn(volSt.sm.get(), {pStarted, ptWaitForEmpty}, FUNC);

    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);

    MetaDiffVec diffV;
    diff::Merger merger;
    MetaDiff mergedDiff;
    setupMerger(merger, diffV, mergedDiff, volInfo, archiveName);
    if (diffV.empty()) {
        LOGs.info() << FUNC << "no need to send wdiffs" << volId << archiveName;
        return DONT_SEND;
    }

    const HostInfoForBkp hi = volInfo.getArchiveInfo(archiveName);
    cybozu::Socket sock;
    ActionCounterTransaction trans(volSt.ac, archiveName);
    ul.unlock();
    util::connectWithTimeout(sock, hi.addrPort.getSocketAddr(), gp.socketTimeout);
    const std::string serverId = protocol::run1stNegotiateAsClient(sock, gp.nodeId, wdiffTransferPN);
    packet::Packet aPack(sock);

    ProtocolLogger logger(gp.nodeId, serverId);

    const DiffFileHeader& fileH = merger.header();

    /* wdiff-send negotiation */
    packet::Packet pkt(sock);
    pkt.write(volId);
    pkt.write(proxyHT);
    pkt.write(fileH.getUuid());
    pkt.write(fileH.getMaxIoBlocks());
    pkt.write(volInfo.getSizeLb());
    pkt.write(mergedDiff);
    logger.debug() << "send" << volId << proxyHT << fileH.getUuid()
                   << fileH.getMaxIoBlocks() << volInfo.getSizeLb() << mergedDiff;

    std::string res;
    pkt.read(res);
    if (res == msgAccept) {
        if (!wdiffTransferClient(pkt, merger, hi.cmpr, volSt.stopState, gp.forceQuit)) {
            logger.warn() << FUNC << "force stopped wdiff sending" << volId;
            return DONT_SEND;
        }
        packet::Ack(pkt.sock()).recv();
        ul.lock();
        volSt.lastWdiffSentTimeMap[archiveName] = ::time(0);
        ul.unlock();
        volInfo.deleteDiffs(diffV, archiveName);
        pushOpt.isForce = false;
        pushOpt.delaySec = 0;
        return CONTINUE_TO_SEND;
    }
    cybozu::Exception e("ProxyWorker");
    if (res == "stopped" || res == "wdiff-recv" || res == "too-new-diff") {
        const uint64_t curTs = ::time(0);
        ul.lock();
        if (volSt.lastWlogReceivedTime != 0 &&
            curTs - volSt.lastWlogReceivedTime > gp.retryTimeout) {
            e << "reached retryTimeout" << gp.retryTimeout;
            logger.error() << e.what();
            return SEND_ERROR;
        }
        e << res << "delay time" << gp.delaySecForRetry;
        logger.info() << e.what();
        pushOpt.isForce = true;
        pushOpt.delaySec = gp.delaySecForRetry;
        return CONTINUE_TO_SEND;
    }
    if (res == "different-uuid" || res == "too-old-diff") {
        e << res;
        logger.info() << e.what();
        volInfo.deleteDiffs(diffV, archiveName);
        pushOpt.isForce = true;
        pushOpt.delaySec = 0;
        return CONTINUE_TO_SEND;
    }
    /**
     * archive-not-found, not-applicable-diff, smaller-lv-size
     *
     * The background task will stop, and change to stop state.
     * You must start by hand.
     */
    e << res;
    logger.error() << e.what();
    return SEND_ERROR;
}

inline void ProxyWorker::operator()()
{
    const char *const FUNC = __func__;
    TaskQueue<ProxyTask> &q = getProxyGlobal().taskQueue;
    try {
        PushOpt opt;
        const int ret = transferWdiffIfNecessary(opt);
        switch (ret) {
        case CONTINUE_TO_SEND:
            {
                const size_t delayMs = opt.delaySec * 1000;
                if (opt.isForce) {
                    q.pushForce(task_, delayMs);
                } else {
                    q.push(task_, delayMs);
                }
            }
            break;
        case SEND_ERROR:
            LOGs.error() << "send error" << task_.volId << task_.archiveName;
            getProxyVolState(task_.volId).actionState.set(task_.archiveName);
            break;
        case DONT_SEND:
        default:
            break;
        }
    } catch (std::exception &e) {
        LOGs.error() << FUNC << e.what();
        q.pushForce(task_, 0);
    } catch (...) {
        LOGs.error() << FUNC << "unknown error";
        q.pushForce(task_, 0);
    }
}

/**
 * This is for test and debug.
 */
inline void c2pResizeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &volId = v[0];
        const uint64_t sizeLb = cybozu::util::fromUnitIntString(v[1]) / LOGICAL_BLOCK_SIZE;

        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), {pStopped}, FUNC);

        ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
        const uint64_t oldSizeLb = volInfo.getSizeLb();
        volInfo.setSizeLb(sizeLb);

        pkt.writeFin(msgOk);
        logger.info() << "resize succeeded" << volId << oldSizeLb << sizeLb;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

inline void c2pHostTypeServer(protocol::ServerParams &p)
{
    protocol::runHostTypeServer(p, proxyHT);
}

/**
 * params[0]: volId
 * params[1]: archiveName (optional)
 */
inline void c2pKickServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        std::string volId, archiveName;
        cybozu::util::parseStrVec(v, 0, 1, {&volId, &archiveName});

        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);
        ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
        if (archiveName.empty()) {
            proxy_local::pushAllTasksForVol(volId);
        } else {
            if (!volInfo.existsArchiveInfo(archiveName)) {
                throw cybozu::Exception(FUNC) << "archive does not exist" << archiveName;
            }
            volSt.actionState.clear(archiveName);
            getProxyGlobal().taskQueue.push(ProxyTask(volId, archiveName));
        }
        pkt.writeFin(msgOk);
        logger.info() << "kick succeeded" << volId << archiveName;
    } catch (std::exception &e) {
        LOGs.error() << e.what();
        pkt.write(e.what());
    }
}

} // walb
