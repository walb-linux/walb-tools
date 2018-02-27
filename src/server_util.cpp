#include "server_util.hpp"

namespace walb {
namespace server {

ProcessStatus *MultiThreadedServer::pps_;

void MultiThreadedServer::run(
    ProcessStatus &ps, uint16_t port, const std::string& nodeId,
    const protocol::Str2ServerHandler& handlers, protocol::HandlerStatMgr& handlerStatMgr,
    size_t maxNumThreads, const KeepAliveParams& keepAliveParams, size_t timeoutS)
{
    const char *const FUNC = __func__;
    const size_t acceptTimeoutMs = 100;
    pps_ = &ps;
    setQuitHandler();
    cybozu::Socket ssock;
    ssock.bind(port);
    cybozu::thread::ThreadRunnerFixedPool pool;
    pool.start(maxNumThreads);
    LOGs.info() << FUNC << "Ready to accept connections";
    try {
        for (;;) {
            for (;;) {
                if (!ps.isRunning()) goto quit;
                int ret = ssock.queryAcceptNoThrow(acceptTimeoutMs);
                if (ret > 0) break; // accepted
                if (ret == 0) continue; // timeout
                if (ret == -EINTR) {
                    LOGs.info() << FUNC << "queryAccept:interrupted";
                    continue;
                }
                throw cybozu::Exception(FUNC) << "queryAccept" << cybozu::NetErrorNo(-ret);
            }
            logErrors(pool.gc());
            if (pool.nrRunning() >= maxNumThreads) {
                putLogExceedsMaxConcurrency(maxNumThreads);
                std::this_thread::sleep_for(std::chrono::milliseconds(acceptTimeoutMs));
                continue;
            }
            putLogExceedsMaxConcurrencySuppressed();
            cybozu::Socket sock;
            ssock.accept(sock);
            util::setSocketParams(sock, keepAliveParams, timeoutS);
            const bool success = pool.add(
                protocol::RequestWorker(std::move(sock), nodeId, ps, handlers, handlerStatMgr));
            assert(success); unusedVar(success);
        }
      quit:
        LOGs.info() << FUNC << "Waiting for remaining tasks";
        pool.stop();
        logErrors(pool.gc());
    } catch (...) {
        // We must call setForceShutdown() before pool destructor is called
        // because running tasks must stop to progress worker destructors.
        ps.setForceShutdown();
        throw;
    }
}

} // namespace server

const char* stopStateToStr(StopState st)
{
    switch (st) {
    case NotStopping:
        return "NotStopping";
    case WaitingForEmpty:
        return "WAitingForEmpty";
    case Stopping:
        return "Stopping";
    case ForceStopping:
        return "ForceStopping";
    default:
        throw cybozu::Exception(__func__) << "bad state" << st;
    }
}

std::string formatActions(const char *prefix, ActionCounters &ac, const StrVec &actionV, bool useTime)
{
    const std::vector<ActionCounterItem> itemV = ac.getItems(actionV);
    std::string ret(prefix);
    for (size_t i = 0; i < actionV.size(); i++) {
        ret += cybozu::util::formatString(" %s %d", actionV[i].c_str(), itemV[i].count);
        if (useTime) {
            ret += " ";
            ret += util::timeToPrintable(itemV[i].bgn_time);
        }
    }
    return ret;
}

std::string formatMetaDiff(const char *prefix, const MetaDiff &diff)
{
    return cybozu::util::formatString(
        "%s%s %c%c %s %" PRIu64 ""
        , prefix, diff.str().c_str()
        , diff.isMergeable ? 'M' : '-'
        , diff.isCompDiff ? 'C' : '-'
        , util::timeToPrintable(diff.timestamp).c_str()
        , diff.dataSize);
}

} // namespace walb
