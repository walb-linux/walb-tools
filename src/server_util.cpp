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
    pps_ = &ps;
    setQuitHandler();
    cybozu::Socket ssock;
    ssock.bind(port);
    cybozu::thread::ThreadRunnerFixedPool pool;
    pool.start(maxNumThreads);
    LOGs.info() << FUNC << "Ready to accept connections";
    for (;;) {
        for (;;) {
            if (!ps.isRunning()) goto quit;
            int ret = ssock.queryAcceptNoThrow();
            if (ret > 0) break; // accepted
            if (ret == 0) continue; // timeout
            if (ret == -EINTR) {
                LOGs.info() << FUNC << "queryAccept:interrupted";
                continue;
            }
            throw cybozu::Exception(FUNC) << "queryAccept" << cybozu::NetErrorNo(-ret);
        }
        cybozu::Socket sock;
        ssock.accept(sock);
        util::setSocketParams(sock, keepAliveParams, timeoutS);
        logErrors(pool.gc());
        if (!pool.add(protocol::RequestWorker(std::move(sock), nodeId, ps, handlers, handlerStatMgr))) {
            putLogExceedsMaxConcurrency(maxNumThreads);
            // The socket will be closed.
        }
    }
  quit:
    LOGs.info() << FUNC << "Waiting for remaining tasks";
    pool.stop();
    logErrors(pool.gc());
}

void MultiThreadedServer::putLogExceedsMaxConcurrency(size_t maxNumThreads)
{
    static size_t count = 0;
    static cybozu::Timespec ts(0);
    const cybozu::TimespecDiff oneSec(1);
    cybozu::Timespec now = cybozu::getNowAsTimespec();
    if (now - ts < oneSec) {
        count++;
        return; // supress.
    }
    LOGs.warn() << "MultiThreadedServer:exceeds max concurrency"
                << maxNumThreads << "supressed" << count;
    count = 0;
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
