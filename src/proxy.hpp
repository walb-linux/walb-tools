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
#include "walb_util.hpp"
#include "walb_diff_merge.hpp"
#include "walb_diff_compressor.hpp"

namespace walb {

struct ProxyVolState
{
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac; // archive name is action identifier here.

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
     */
    uint64_t lastWlogRecievedTime;
    /**
     * Timestamp of the latest wdiff sent to each archive server.
     * Lock of mu is required to access this.
     * Key is archiveName, value is the corresponding timestamp.
     */
    std::map<std::string, uint64_t> lastWdiffSentTimeMap;

    explicit ProxyVolState(const std::string &volId)
        : stopState(NotStopping), sm(mu), ac(mu)
        , diffMgr(), diffMgrMap(), archiveSet() {
        const struct StateMachine::Pair tbl[] = {
            { pClear, ptAddArchiveInfo },
            { ptAddArchiveInfo, pStopped },

            { pStopped, ptClearVol },
            { ptClearVol, pClear },

            { pStopped, ptAddArchiveInfo },
            { ptAddArchiveInfo, pStopped },

            { pStopped, ptDeleteArchiveInfo },
            { ptDeleteArchiveInfo, pStopped },

            { pStopped, ptDeleteArchiveInfo },
            { ptDeleteArchiveInfo, pClear },

            { pStopped, ptStart },
            { ptStart, pStarted },

            { pStarted, ptStop },
            { ptStop, pStopped },

            { pStarted, ptWlogRecv },
            { ptWlogRecv, pStarted },
        };
        sm.init(tbl);
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
};

class ProxyWorker : public cybozu::thread::Runnable
{
private:
    const ProxyTask task_;

    bool setupMerger(diff::Merger& merger, std::vector<MetaDiff>& diffV, MetaDiff& mergedDiff, const ProxyVolInfo& volInfo, const std::string& archiveName);


public:
    ProxyWorker(const ProxyTask &task) : task_(task) {
    }
    /**
     * This will do wdiff send to an archive server.
     * You can throw an exception.
     */
    void operator()();
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

    /**
     * Writable and must be thread-safe.
     */
    std::atomic<bool> forceQuit;
    AtomicMap<ProxyVolState> stMap;
    TaskQueue<ProxyTask> taskQueue;
    std::unique_ptr<DispatchTask<ProxyTask, ProxyWorker> > dispatcher;
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

    sm.set(volInfo.getState());
    volInfo.loadAllArchiveInfo();

    // Retry to make hard links of wdiff files in the master directory.
    std::vector<MetaDiff> diffV = volInfo.getAllDiffsInMaster();
    for (const MetaDiff &d : diffV) {
        for (const std::string &name : archiveSet) {
            volInfo.tryToMakeHardlinkInSlave(d, name);
        }
    }
    volInfo.deleteDiffs(diffV);
    // Here the master directory must contain no wdiff file.
    if (!diffMgr.getAll().empty()) {
        throw cybozu::Exception("ProxyVolState::initInner")
            << "there are wdiff files in the master directory";
    }
}

inline ProxyVolState &getProxyVolState(const std::string &volId)
{
    return getProxyGlobal().stMap.get(volId);
}

namespace proxy_local {

inline StrVec getAllStateStrVec()
{
    StrVec ret;
    const auto &fmt = cybozu::util::formatString;

    for (const std::string &volId : getProxyGlobal().stMap.getKeyList()) {
        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);
        const ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
        const std::string state = volSt.sm.get();
        const uint64_t totalSize = volInfo.getTotalDiffFileSize();
        const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
        const std::string tsStr = cybozu::unixTimeToStr(volSt.lastWlogRecievedTime);
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
            const std::string tsStr = cybozu::unixTimeToStr(volSt.lastWdiffSentTimeMap[archiveName]);
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
    const std::string tsStr = cybozu::unixTimeToStr(volSt.lastWlogRecievedTime);

    ret.push_back(fmt("volId %s", volId.c_str()));
    ret.push_back(fmt("state %s", state.c_str()));
    ret.push_back(fmt("num %zu", num));
    ret.push_back(fmt("totalSize %zu", totalSizeStr.c_str()));
    ret.push_back(fmt("timestamp %s", tsStr.c_str()));

    const std::vector<int> actionNum = volSt.ac.getValues(volSt.archiveSet);
    size_t i = 0;
    for (const std::string& archiveName : volSt.archiveSet) {
        const MetaDiffManager &mgr = volSt.diffMgrMap.get(archiveName);
        const HostInfo hi = volInfo.getArchiveInfo(archiveName);
        const std::string tsStr = walb::util::timeToPrintable(volSt.lastWdiffSentTimeMap[archiveName]);

        ret.push_back(fmt("archive %s %s", archiveName.c_str(), hi.str().c_str()));
        ret.push_back(fmt("  action %s", actionNum[i] == 0 ? "None" : "WdiffSend"));
        ret.push_back(fmt("  num %zu", mgr.size()));
        ret.push_back(fmt("  timestamp %s", tsStr.c_str()));

        const std::vector<MetaDiff> diffV = mgr.getAll();
        uint64_t totalSize = 0;
        StrVec wdiffStrV;
        uint64_t minTs = -1;
        for (const MetaDiff &diff : diffV) {
            const uint64_t fsize = volInfo.getDiffFileSize(diff, archiveName);
            const std::string fsizeStr = cybozu::util::toUnitIntString(fsize);
            wdiffStrV.push_back(
                fmt("  wdiff %s %d %s %s"
                    , diff.str().c_str()
                    , diff.canMerge ? 1 : 0
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

} // namespace proxy_local

/**
 * params:
 *   [0]: volId or none.
 */
inline void c2pStatusServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = protocol::recvStrVec(p.sock, 0, FUNC, false);
    packet::Packet pkt(p.sock);
    try {
        StrVec stStrV;
        if (v.empty()) {
            stStrV = proxy_local::getAllStateStrVec();
        } else {
            const std::string &volId = v[0];
            stStrV = proxy_local::getVolStateStrVec(volId);
        }
        pkt.write("ok");
        pkt.write(stStrV);
    } catch (std::exception &e) {
        pkt.write(std::string(FUNC) + e.what());
        throw;
    }
}

inline void startProxyVol(const std::string &volId, bool ignoreStateFile = false)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);
    if (!ignoreStateFile && volSt.sm.get() != pStopped) return;

    StateMachineTransaction tran(volSt.sm, pStopped, ptStart);
    ul.unlock();

    // Push all (volId, archiveName) pairs as tasks.
    for (const std::string& archiveName : volSt.archiveSet) {
        getProxyGlobal().taskQueue.push(ProxyTask(volId, archiveName));
    }

    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    if (!ignoreStateFile) {
        const std::string fst = volInfo.getState();
        if (fst != pStopped) {
            throw cybozu::Exception(FUNC) << "not Stopped state" << fst;
        }
    }
    volInfo.setState(pStarted);
    tran.commit(pStarted);
}

inline void stopProxyVol(const std::string &volId, bool isForce, bool ignoreStateFile = false)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    Stopper stopper(volSt.stopState, isForce);
    if (!stopper.isSuccess()) return;

    // Clear all related tasks from the task queue.
    getProxyGlobal().taskQueue.remove([&](const ProxyTask &task) {
            return task.volId == volId;
        });

    waitUntil(ul, [&]() {
            if (!volSt.ac.isAllZero(volSt.archiveSet)) return false;
            const std::string &st = volSt.sm.get();
            return st == pStopped || st == pStarted || st == pClear;
        }, FUNC);

    if (!ignoreStateFile && volSt.sm.get() != pStarted) return;

    StateMachineTransaction tran(volSt.sm, pStarted, ptStop, FUNC);
    ul.unlock();
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    if (!ignoreStateFile) {
        const std::string fst = volInfo.getState();
        if (fst != pStarted) {
            throw cybozu::Exception(FUNC) << "not Started state" << fst;
        }
    }
    volInfo.setState(pStopped);
    tran.commit(pStopped);
}

/**
 * params:
 *   [0]: volId
 *
 * State transition: Stopped --> Start --> Started.
 */
inline void c2pStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    packet::Packet pkt(p.sock);
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];

    packet::Ack(p.sock).send();
    startProxyVol(volId);
}

/**
 * params:
 *   [0]: volId
 *   [1]: isForce
 *
 * State transition: Started --> Stop --> Stopped.
 * In addition, this will stop all background tasks before changing state.
 */
inline void c2pStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string &volId = v[0];
    const bool isForce = static_cast<int>(cybozu::atoi(v[1])) != 0;

    packet::Ack(p.sock).send();
    stopProxyVol(volId, isForce);
}

namespace proxy_local {

inline void getArchiveInfo(const std::string& volId, const std::string &archiveName, HostInfo &hi)
{
    const char *const FUNC = __func__;
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    if (!volInfo.existsArchiveInfo(archiveName)) {
        throw cybozu::Exception(FUNC) << "archive info not exists" << archiveName;
    }
    hi = volInfo.getArchiveInfo(archiveName);
}

inline void addArchiveInfo(const std::string &volId, const std::string &archiveName, const HostInfo &hi, bool ensureNotExistance)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
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
    verifyNotStopping(volSt.stopState, volId, FUNC);
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
 *   [0]: volId
 *   [1]: add/delete/update as string
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
    const char * const FUNC = "c2pArchiveInfoServer";
    StrVec v = protocol::recvStrVec(p.sock, 3, FUNC, false);
    const std::string &volId = v[0];
    const std::string &cmd = v[1];
    const std::string &archiveName = v[2];

    packet::Packet pkt(p.sock);
    try {
        HostInfo hi;
        if (cmd == "add" || cmd == "update") {
            pkt.read(hi);
            proxy_local::addArchiveInfo(volId, archiveName, hi, cmd == "add");
            pkt.write("ok");
            return;
        } else if (cmd == "get") {
            proxy_local::getArchiveInfo(volId, archiveName, hi);
            pkt.write("ok");
            pkt.write(hi);
            return;
        } else if (cmd == "delete") {
            proxy_local::deleteArchiveInfo(volId, archiveName);
            pkt.write("ok");
            return;
        }
    } catch (std::exception &e) {
        pkt.write(std::string(FUNC) + e.what());
        throw;
    }
    throw cybozu::Exception(FUNC) << "invalid command name" << cmd;
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
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];
    packet::Ack(p.sock).send();

    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);
    {
        StateMachineTransaction tran(volSt.sm, pStopped, ptClearVol);
        volSt.archiveSet.clear();
        ul.unlock();
        ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
        volInfo.clear();
        tran.commit(pClear);
    }
}

/**
 * params:
 *   [0]: volId
 *   [1]: uuid (cybozu::Uuid)
 *   [2]: diff (walb::MetaDiff)
 *   [3]: pbs (uint32_t)
 *   [4]: salt (uint32_t)
 *   [5]: sizeLb (uint64_t)
 *   [6]: lsidB (uint64_t)
 *   [7]: lsidE (uint64_t)
 *
 * State transition: Started --> WlogRecv --> Started
 */
inline void s2pWlogTransferServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

inline bool ProxyWorker::setupMerger(diff::Merger& merger, std::vector<MetaDiff>& diffV, MetaDiff& mergedDiff, const ProxyVolInfo& volInfo, const std::string& archiveName)
{
    const int maxRetryNum = 10;
    int retryNum = 0;
    cybozu::Uuid uuid;
    std::vector<cybozu::util::FileOpener> ops;
retry:
    {
        diffV = volInfo.getDiffListToSend(archiveName, gp.maxWdiffSendMb * 1024 * 1024);
        if (diffV.empty()) {
            return false;
        }
        // apply wdiff files indicated by diffV to lvSnap.
        for (const MetaDiff& diff : diffV) {
            cybozu::util::FileOpener op;
            if (!op.open(volInfo.getDiffPath(diff, archiveName).str(), O_RDONLY)) {
                retryNum++;
                if (retryNum == maxRetryNum) throw cybozu::Exception("ArchiveVolInfo::restore:exceed max retry");
                ops.clear();
                goto retry;
            }
            diff::Reader reader(op.fd());
            diff::FileHeaderRaw header;
            reader.readHeaderWithoutReadingPackHeader(header);
            if (ops.empty()) {
                uuid = header.getUuid2();
                mergedDiff = diff;
            } else if (uuid != header.getUuid2()) {
                diffV.resize(ops.size());
                break;
            }
            mergedDiff.merge(diff);
            if (lseek(op.fd(), 0, SEEK_SET) < 0) throw cybozu::Exception("ProxyWorker:setupMerger") << cybozu::ErrorNo();
            ops.push_back(std::move(op));
        }
    }
    merger.addWdiffs(std::move(ops));
    return true;
}

namespace proxy_local {

inline void sendWdiffs(
    cybozu::Socket &sock, diff::Merger &merger, const HostInfo &hi)
{
    packet::StreamControl ctrl(sock);
    diff::RecIo recIo;
    const size_t nCPU = hi.compressionNumCPU;
    const size_t maxPushedNum = nCPU * 2 - 1;
    ConverterQueue conv(maxPushedNum, nCPU, true,
                        hi.compressionType, hi.compressionLevel);
    diff::Packer packer;
    size_t pushedNum = 0;
    while (merger.pop(recIo)) {
        const walb_diff_record& rec = recIo.record();
        const diff::IoData& io = recIo.io();
        if (packer.add(rec, io.rawData())) {
            continue;
        }
        conv.push(packer.getPackAsUniquePtr());
        pushedNum++;
        packer.reset();
        packer.add(rec, io.rawData());
        if (pushedNum < maxPushedNum) {
            continue;
        }
        std::unique_ptr<char[]> p = conv.pop();
        ctrl.next();
        sock.write(p.get(), diff::PackHeader(p.get()).wholePackSize());
        pushedNum--;
    }
    if (!packer.empty()) {
        conv.push(packer.getPackAsUniquePtr());
    }
    conv.quit();
    while (std::unique_ptr<char[]> p = conv.pop()) {
        ctrl.next();
        sock.write(p.get(), diff::PackHeader(p.get()).wholePackSize());
    }
    ctrl.end();
    packet::Ack(sock).recv();
}

} // namespace proxy_local

inline void ProxyWorker::operator()() {
    const char *const FUNC = "ProxyWorker:operator()";
    const std::string& volId = task_.volId;
    const std::string& archiveName = task_.archiveName;
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    {
        const std::string st = volSt.sm.get();
        if (st != pStarted) throw cybozu::Exception(FUNC) << "bad state" << st;
    }

    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);

    std::vector<MetaDiff> diffV;
    diff::Merger merger;
    MetaDiff mergedDiff;
    if (!setupMerger(merger, diffV, mergedDiff, volInfo, archiveName)) {
        LOGi("no need to send wdiffs %s:%s", volId.c_str(), archiveName.c_str());
        return;
    }

    const HostInfo hi = volInfo.getArchiveInfo(archiveName);
    cybozu::Socket sock;
    ActionCounterTransaction trans(volSt.ac, archiveName);
    ul.unlock();
    sock.connect(hi.addr, hi.port);
    const std::string serverId = walb::protocol::run1stNegotiateAsClient(sock, gp.nodeId, "wdiff-transfer");
    walb::packet::Packet aPack(sock);

    walb::ProtocolLogger logger(gp.nodeId, serverId);

    const walb::diff::FileHeaderWrap& fileH = merger.header();

    /* wdiff-send negotiation */
    walb::packet::Packet pkt(sock);
    pkt.write(volId);
    pkt.write("proxy");
    pkt.write(fileH.getUuid2());
    pkt.write(fileH.getMaxIoBlocks());
    pkt.write(volInfo.getSizeLb());
    pkt.write(mergedDiff);

    std::string res;
    pkt.read(res);
    if (res == "ok") {
        proxy_local::sendWdiffs(sock, merger, hi);
        ul.lock();
        volSt.lastWdiffSentTimeMap[archiveName] = ::time(0);
        ul.unlock();
        volInfo.deleteDiffs(diffV, archiveName);
        getProxyGlobal().taskQueue.push(task_);
        return;
    }
    cybozu::Exception e("ProxyWorker");
    if (res == "stopped" || res == "too-new-diff") {
        const uint64_t curTs = ::time(0);
        ul.lock();
        if (curTs - volSt.lastWlogRecievedTime > gp.retryTimeout) {
            e << "reached retryTimeout" << gp.retryTimeout;
            logger.throwError(e);
        }
        e << res << "delay time" << gp.delaySecForRetry;
        logger.info() << e.what();
        getProxyGlobal().taskQueue.pushForce(task_, gp.delaySecForRetry * 1000);
        return;
    }
    if (res == "different-uuid" || res == "too-old-diff") {
        e << res;
        logger.info() << e.what();
        volInfo.deleteDiffs(diffV, archiveName);
        getProxyGlobal().taskQueue.pushForce(task_, 0);
        return;
    }
    // archive-not-found, not-applicable-diff
    e << res;
    logger.throwError(e);
}

} // walb
