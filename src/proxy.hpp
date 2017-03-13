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
#include "command_param_parser.hpp"
#include "bdev_util.hpp"

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

    MetaDiffManager diffMgr; // for the target.
    AtomicMap<MetaDiffManager> diffMgrMap; // for each archive.

    /**
     * This is protected by state machine.
     * In Started/WlogRecv state, this is read-only and accessed by multiple threads.
     * Otherwise, this is writable and accessed by only one thread.
     */
    std::set<std::string> archiveSet;

    /**
     * Timestamp of the latest wlog received from a storage server.
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

    void setupMerger(DiffMerger& merger, MetaDiffVec& diffV, MetaDiff& mergedDiff, const ProxyVolInfo& volInfo, const std::string& archiveName);

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
    size_t maxWdiffSendNr;
    size_t delaySecForRetry;
    size_t retryTimeout;
    size_t maxForegroundTasks;
    size_t maxConversionMb;
    size_t socketTimeout;
    KeepAliveParams keepAliveParams;
    bool allowExec;

    /**
     * Writable and must be thread-safe.
     */
    ProcessStatus ps;
    AtomicMap<ProxyVolState> stMap;
    TaskQueue<ProxyTask> taskQueue;
    std::unique_ptr<DispatchTask<ProxyTask, ProxyWorker> > dispatcher;
    std::atomic<uint64_t> conversionUsageMb;
    protocol::HandlerStatMgr handlerStatMgr;

    void setSocketParams(cybozu::Socket& sock) const {
        util::setSocketParams(sock, keepAliveParams, socketTimeout);
    }
};

inline ProxySingleton& getProxyGlobal()
{
    return ProxySingleton::getInstance();
}

static const ProxySingleton& gp = getProxyGlobal();


inline ProxyVolState &getProxyVolState(const std::string &volId)
{
    return getProxyGlobal().stMap.get(volId);
}

inline ProxyVolInfo getProxyVolInfo(const std::string &volId)
{
    ProxyVolState &volSt = getProxyVolState(volId);
    return ProxyVolInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
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

inline void verifyDiskSpaceAvailable(uint64_t maxLogSizeMb, const char *msg)
{
    const uint64_t availMb = cybozu::util::getAvailableDiskSpace(gp.baseDirStr) / MEBI;
    if (availMb < maxLogSizeMb) {
        throw cybozu::Exception(msg)
            << "Not enough available disk space" << availMb << maxLogSizeMb;
    }
}

StrVec getAllStatusAsStrVec();
StrVec getVolStatusAsStrVec(const std::string &volId);
void pushAllTasksForVol(const std::string &volId, Logger *loggerP = nullptr);

inline void removeAllTasksForVol(const std::string &volId)
{
    getProxyGlobal().taskQueue.remove([&](const ProxyTask &task) {
            return task.volId == volId;
        });
}

void gcProxyVol(const std::string &volId);

} // namespace proxy_local

void c2pStatusServer(protocol::ServerParams &p);
void startProxyVol(const std::string &volId);
void c2pStartServer(protocol::ServerParams &p);
void c2pStopServer(protocol::ServerParams &p);
void c2pArchiveInfoServer(protocol::ServerParams &p);
void c2pClearVolServer(protocol::ServerParams &p);
void s2pWlogTransferServer(protocol::ServerParams &p);
void c2pResizeServer(protocol::ServerParams &p);
void c2pKickServer(protocol::ServerParams &p);


namespace proxy_local {

bool hasDiffs(ProxyVolState &volSt);
void stopAndEmptyProxyVol(const std::string &volId);
void stopProxyVol(const std::string &volId, bool isForce);

void listArchiveInfo(const std::string &volId, StrVec &archiveNameV);
void getArchiveInfo(const std::string& volId, const std::string &archiveName, HostInfoForBkp &hi);
void addArchiveInfo(const std::string &volId, const std::string &archiveName, const HostInfoForBkp &hi,
                    bool ensureNotExistance);
void deleteArchiveInfo(const std::string &volId, const std::string &archiveName);

bool recvWlogAndWriteDiff(
    cybozu::Socket &sock, int fd, const cybozu::Uuid &uuid, uint32_t pbs, uint32_t salt,
    const std::atomic<int> &stopState, const ProcessStatus &ps, int wlogFd);


inline void getState(protocol::GetCommandParams &p)
{
    protocol::runGetStateServer(p, getProxyVolState);
}

inline void getHostType(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, proxyHT);
}

inline void getVolList(protocol::GetCommandParams &p)
{
    StrVec v = util::getDirNameList(gp.baseDirStr);
    protocol::sendValueAndFin(p, v);
}

inline void getPid(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, static_cast<size_t>(::getpid()));
}

void isWdiffSendError(protocol::GetCommandParams &p);
StrVec getLatestSnapForVolume(const std::string& volId);
void getLatestSnap(protocol::GetCommandParams &p);
void getHandlerStat(protocol::GetCommandParams &p);

} // namespace proxy_local

const protocol::GetCommandHandlerMap proxyGetHandlerMap = {
    { stateTN, proxy_local::getState },
    { hostTypeTN, proxy_local::getHostType },
    { volTN, proxy_local::getVolList },
    { pidTN, proxy_local::getPid },
    { isWdiffSendErrorTN, proxy_local::isWdiffSendError },
    { getLatestSnapTN, proxy_local::getLatestSnap },
    { getHandlerStatTN, proxy_local::getHandlerStat },
};

inline void c2pGetServer(protocol::ServerParams &p)
{
    protocol::runGetCommandServer(p, gp.nodeId, proxyGetHandlerMap);
}

inline void c2pExecServer(protocol::ServerParams &p)
{
    protocol::runExecServer(p, gp.nodeId, gp.allowExec);
}

const protocol::Str2ServerHandler proxyHandlerMap = {
    { statusCN, c2pStatusServer },
    { startCN, c2pStartServer },
    { stopCN, c2pStopServer },
    { archiveInfoCN, c2pArchiveInfoServer },
    { clearVolCN, c2pClearVolServer },
    { resizeCN, c2pResizeServer },
    { kickCN, c2pKickServer },
    { getCN, c2pGetServer },
    { execCN, c2pExecServer },
    // protocols.
    { wlogTransferPN, s2pWlogTransferServer },
};

} // namespace walb
