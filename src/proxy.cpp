#include "proxy.hpp"

namespace walb {

/**
 * This is called just one time and by one thread.
 * You need not take care about thread-safety inside this function.
 */
void ProxyVolState::initInner(const std::string &volId)
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
    LOGs.info() << "volume archive info" << volId << archiveSet.size()
                << cybozu::util::concat(archiveSet, ",");
}


/**
 * params:
 *   [0]: volId or none.
 */
void c2pStatusServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const VolIdOrAllParam param = parseVolIdOrAllParam(protocol::recvStrVec(p.sock, 0, FUNC), 0);
        StrVec stStrV;
        if (param.isAll) {
            stStrV = proxy_local::getAllStatusAsStrVec();
        } else {
            stStrV = proxy_local::getVolStatusAsStrVec(param.volId);
        }
        protocol::sendValueAndFin(pkt, sendErr, stStrV);
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


void startProxyVol(const std::string &volId)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyActionNotRunning(volSt.ac, volSt.archiveSet, FUNC);
    const std::string &st = volSt.sm.get();
    if (st != pStopped) {
        throw cybozu::Exception("bad state") << st;
    }
    proxy_local::gcProxyVol(volId);
    StateMachineTransaction tran(volSt.sm, pStopped, ptStart);
    proxy_local::pushAllTasksForVol(volId);
    tran.commit(pStarted);
}


/**
 * State transition: Stopped --> Start --> Started.
 */
void c2pStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
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
void c2pStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StopParam param = parseStopParam(protocol::recvStrVec(p.sock, 0, FUNC), true);
        const std::string &volId = param.volId;
        const StopOpt &stopOpt = param.stopOpt;

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
void c2pArchiveInfoServer(protocol::ServerParams &p)
{
    const char * const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const ArchiveInfoParam param = parseArchiveInfoParam(protocol::recvStrVec(p.sock, 2, FUNC));
        const std::string &cmd = param.cmd;
        const std::string &volId = param.volId;
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
void c2pClearVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyActionNotRunning(volSt.ac, volSt.archiveSet, FUNC);

        StateMachineTransaction tran(volSt.sm, pStopped, ptClearVol);
        ul.unlock();
        ProxyVolInfo volInfo = getProxyVolInfo(volId);
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
void s2pWlogTransferServer(protocol::ServerParams &p)
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
    const uint64_t maxLogSizeMb = maxLogSizePb * pbs / MEBI + 1;
    proxy_local::ConversionMemoryTransaction convTran(maxLogSizeMb);
    try {
        verifyMaxForegroundTasks(gp.maxForegroundTasks, FUNC);
        proxy_local::verifyMaxConversionMemory(FUNC);
        proxy_local::verifyDiskSpaceAvailable(maxLogSizeMb, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), {pStarted}, FUNC);
    } catch (std::exception &e) {
        logger.warn() << e.what();
        pkt.write(e.what());
        return;
    }
    pkt.write(msgAccept);
    pkt.flush();

    StateMachineTransaction tran(volSt.sm, pStarted, ptWlogRecv);
    ul.unlock();

    cybozu::Stopwatch stopwatch;
    ProxyVolInfo volInfo = getProxyVolInfo(volId);
    cybozu::TmpFile tmpFile(volInfo.getReceivedDir().str());
    cybozu::TmpFile wlogTmpFile;
    const bool savesWlog = false; // for DEBUG.
    if (savesWlog) wlogTmpFile.prepare(volInfo.getReceivedDir().str());
#if 0 /* deprecated */
    const bool ret = proxy_local::recvWlogAndWriteDiff(
        p.sock, tmpFile.fd(), uuid, pbs, salt, volSt.stopState, gp.ps, wlogTmpFile.fd());
#else /* use indexed diff. */
    const bool ret = proxy_local::recvWlogAndWriteDiff2(
        p.sock, tmpFile.fd(), uuid, pbs, salt, volSt.stopState, gp.ps, wlogTmpFile.fd());
#endif
    if (!ret) {
        logger.warn() << FUNC << "force stopped wlog receiving" << volId;
        return;
    }
    MetaDiff diff;
    pkt.read(diff);
    if (!diff.isClean()) {
        throw cybozu::Exception(FUNC) << "diff is not clean" << diff;
    }
    diff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
    tmpFile.save(volInfo.getDiffPath(diff).str());
    if (savesWlog) {
        const std::string fname =
            cybozu::util::removeSuffix(createDiffFileName(diff), ".wdiff") + ".wlog";
        wlogTmpFile.save((volInfo.volDir + fname).str());
    }
    // You must register the diff before trying to send ack.
    // When ack failed, next wlog-transfer will do the remaining procedures.
    ul.lock();
    volInfo.addDiffToReceivedDir(diff);

    volSt.actionState.clearAll();
    const MetaDiffVec diffV = volInfo.tryToMakeHardlinkInSendtoDir();
    volInfo.deleteDiffs(diffV);
    for (const std::string &archiveName : volSt.archiveSet) {
        HostInfoForBkp hi = volInfo.getArchiveInfo(archiveName);
        ProxyTask task(volId, archiveName);
        pushTask(task, hi.wdiffSendDelaySec * 1000);
        logger.debug() << "task pushed" << task;
    }
    const uint64_t realSizeLb = volInfo.getSizeLb();
    if (realSizeLb < volSizeLb) {
        logger.info() << "detect volume grow" << volId << realSizeLb << volSizeLb;
        volInfo.setSizeLb(volSizeLb);
    }
    volSt.lastWlogReceivedTime = ::time(0);
    tran.commit(pStarted);
    ul.unlock();

    // The order transaction commit --> send ack is important to avoid
    // phantom duplication of wlog sending process.
    packet::Ack(p.sock).sendFin();

    const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
    logger.debug() << "wlog-transfer succeeded" << volId << elapsed;
}


void ProxyWorker::setupMerger(DiffMerger& merger, MetaDiffVec& diffV, MetaDiff& mergedDiff,
                              const ProxyVolInfo& volInfo, const std::string& archiveName)
{
    const char *const FUNC = __func__;
    const int maxRetryNum = 10;
    int retryNum = 0;
    cybozu::Uuid uuid;
    std::vector<cybozu::util::File> fileV;
retry:
    {
        diffV = volInfo.getDiffListToSend(archiveName, gp.maxWdiffSendMb * MEBI, gp.maxWdiffSendNr);
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

            DiffFileHeader header;
            header.readFrom(file);
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
    merger.setMaxCacheSize(INDEXED_DIFF_CACHE_SIZE);
    merger.addWdiffs(std::move(fileV));
    merger.prepare();
}


/**
 * RETURN:
 *   DONT_SEND, CONTINUE_TO_SEND, or SEND_ERROR.
 */
ProxyWorker::TransferState ProxyWorker::transferWdiffIfNecessary(PushOpt &pushOpt)
{
    const char *const FUNC = __func__;
    const std::string& volId = task_.volId;
    const std::string& archiveName = task_.archiveName;
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyStopState(volSt.stopState, NotStopping | WaitingForEmpty, volId, FUNC);
    const std::string st = volSt.sm.get();
    if (st == ptStart) {
        // This is rare case, but possible.
        pushOpt.isForce = false;
        pushOpt.delayMs = 1000;
        return TransferState::DO_NEXT;
    }
    verifyStateIn(st, pAcceptForWdiffSend, FUNC);

    ProxyVolInfo volInfo = getProxyVolInfo(volId);

    MetaDiffVec diffV;
    DiffMerger merger;
    MetaDiff mergedDiff;
    setupMerger(merger, diffV, mergedDiff, volInfo, archiveName);
    if (diffV.empty()) {
        LOGs.debug() << FUNC << "no need to send wdiffs" << volId << archiveName;
        return TransferState::DONT_SEND;
    }
    const HostInfoForBkp hi = volInfo.getArchiveInfo(archiveName);
    ActionCounterTransaction trans(volSt.ac, archiveName);
    if (trans.count() > 0) {
        LOGs.debug() << FUNC << "another task is running" << volId << archiveName;
        return TransferState::DONT_SEND;
    }

    ul.unlock();
    cybozu::Socket sock;
    util::connectWithTimeout(sock, hi.addrPort.getSocketAddr(), gp.socketTimeout);
    gp.setSocketParams(sock);
    const std::string serverId = protocol::run1stNegotiateAsClient(sock, gp.nodeId, wdiffTransferPN);
    ProtocolLogger logger(gp.nodeId, serverId);

    const DiffFileHeader& fileH = merger.header();

    /* wdiff-send negotiation */
    packet::Packet pkt(sock);
    pkt.write(volId);
    pkt.write(proxyHT);
    pkt.write(fileH.getUuid());
    uint32_t maxIoBlocks = 0; // unused
    pkt.write(maxIoBlocks);
    pkt.write(volInfo.getSizeLb());
    pkt.write(mergedDiff);
    pkt.flush();
    logger.debug() << "send" << volId << proxyHT << fileH.getUuid()
                   << volInfo.getSizeLb() << mergedDiff;

    std::string res;
    pkt.read(res);
    if (res == msgAccept) {
        DiffStatistics statOut;
        if (!wdiffTransferClient(pkt, merger, hi.cmpr, volSt.stopState, gp.ps, statOut)) {
            logger.warn() << FUNC << "force stopped wdiff sending" << volId;
            return TransferState::DONT_SEND;
        }
        packet::Ack(pkt.sock()).recv();
        logger.debug() << "mergeIn " << volId << merger.statIn();
        logger.debug() << "mergeOut" << volId << statOut;
        logger.debug() << "mergeMemUsage" << volId << merger.memUsageStr();
        ul.lock();
        volSt.lastWdiffSentTimeMap[archiveName] = ::time(0);
        ul.unlock();
        volInfo.deleteDiffs(diffV, archiveName);
        pushOpt.isForce = false;
        pushOpt.delayMs = 0;
        return TransferState::DO_NEXT;
    }
    if (res == msgSmallerLvSize || res == msgArchiveNotFound) {
        /**
         * The background task will stop, and change to stop state.
         * You must restart by hand.
         */
        logger.error() << FUNC << res << volId;
        return TransferState::SEND_ERROR;
    }
    if (res == msgDifferentUuid || res == msgTooOldDiff) {
        logger.info() << FUNC << res << volId << mergedDiff;
        volInfo.deleteDiffs(diffV, archiveName);
        pushOpt.isForce = true;
        pushOpt.delayMs = 0; // retry soon.
        return TransferState::DO_NEXT;
    }
    /**
     * msgStopped, msgWdiffRecv, msgTooNewDiff, msgSyncing, and others.
     */
    const uint64_t curTs = ::time(0);
    ul.lock();
    if (volSt.lastWlogReceivedTime != 0 &&
        curTs - volSt.lastWlogReceivedTime > gp.retryTimeout) {
        logger.error() << FUNC << "reached retryTimeout" << gp.retryTimeout
                       << volId << mergedDiff;
        return TransferState::SEND_ERROR;
    }
    pushOpt.isForce = true;
    if (task_.delayMs == 0) {
        pushOpt.delayMs = gp.minDelaySecForRetry * 1000;
    } else {
        pushOpt.delayMs = std::min(task_.delayMs * 2, gp.maxDelaySecForRetry * 1000);
    }
    logger.info() << FUNC << res << "delay time ms" << pushOpt.delayMs
                  << volId << mergedDiff;
    return TransferState::RETRY;
}


void ProxyWorker::operator()()
{
    const char *const FUNC = __func__;
    try {
        PushOpt opt;
        const TransferState ret = transferWdiffIfNecessary(opt);
        switch (ret) {
        case DO_NEXT:
        case RETRY:
            if (opt.isForce) {
                pushTaskForce(task_, opt.delayMs, ret == RETRY);
            } else {
                pushTask(task_, opt.delayMs, ret == RETRY);
            }
            break;
        case SEND_ERROR:
            LOGs.error() << "SEND_ERROR_NO_MORE_RETRY" << task_.volId << task_.archiveName;
            getProxyVolState(task_.volId).actionState.set(task_.archiveName);
            break;
        case DONT_SEND:
        default:
            break;
        }
    } catch (std::exception &e) {
        LOGs.error() << FUNC << e.what();
        pushTaskForce(task_, 1000, true);
    } catch (...) {
        LOGs.error() << FUNC << "unknown error";
        pushTaskForce(task_, 1000, true);
    }
}


/**
 * This is for test and debug.
 */
void c2pResizeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const ResizeParam param = parseResizeParam(protocol::recvStrVec(p.sock, 2, FUNC), false, false);
        const std::string &volId = param.volId;
        const uint64_t newSizeLb = param.newSizeLb;

        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), {pStopped}, FUNC);

        ProxyVolInfo volInfo = getProxyVolInfo(volId);
        const uint64_t oldSizeLb = volInfo.getSizeLb();
        volInfo.setSizeLb(newSizeLb);

        pkt.writeFin(msgOk);
        logger.info() << "resize succeeded" << volId << oldSizeLb << newSizeLb;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}


/**
 * params[0]: volId (optional)
 * params[1]: archiveName (optional)
 */
void c2pKickServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const KickParam param = parseKickParam(protocol::recvStrVec(p.sock, 0, FUNC));
        const std::string &volId = param.volId;
        const std::string &archiveName = param.archiveName;

        if (volId.empty()) {
            for (const std::string &volId : getProxyGlobal().stMap.getKeyList()) {
                try {
                    ProxyVolState &volSt = getProxyVolState(volId);
                    UniqueLock ul(volSt.mu);
                    if (isStateIn(volSt.sm.get(), {pStarted})) {
                        proxy_local::pushAllTasksForVol(volId, &logger);
                    }
                } catch (std::exception &e) {
                    logger.error() << e.what();
                }
            }
        } else {
            ProxyVolState &volSt = getProxyVolState(volId);
            UniqueLock ul(volSt.mu);
            verifyStateIn(volSt.sm.get(), {pStarted}, FUNC);
            if (archiveName.empty()) {
                proxy_local::pushAllTasksForVol(volId, &logger);
            } else {
                ProxyVolInfo volInfo = getProxyVolInfo(volId);
                if (!volInfo.existsArchiveInfo(archiveName)) {
                    throw cybozu::Exception(FUNC) << "archive does not exist" << archiveName;
                }
                volSt.actionState.clear(archiveName);
                logger.info() << FUNC << "kick" << volId << archiveName;
                pushTaskForce(ProxyTask(volId, archiveName), 0);
            }
        }
        pkt.writeFin(msgOk);
        logger.info() << "kick succeeded"
                      << (volId.empty() ? "ALL" : volId)
                      << (archiveName.empty() ? "ALL" : archiveName);
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}


#ifndef NDEBUG
void c2pDebugServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    unusedVar(FUNC);
    ProtocolLogger logger(gp.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        pkt.writeFin(msgOk);
        /* debug code from here. */



    } catch (std::exception &e) {
        logger.error() << e.what();
    }
}
#endif


namespace proxy_local {

StrVec getAllStatusAsStrVec()
{
    StrVec ret;
    const auto &fmt = cybozu::util::formatString;

    ret.push_back("-----ProxyGlobal-----");
    ret.push_back(fmt("nodeId %s", gp.nodeId.c_str()));
    ret.push_back(fmt("baseDir %s", gp.baseDirStr.c_str()));
    ret.push_back(fmt("maxWdiffSendMb %zu", gp.maxWdiffSendMb));
    ret.push_back(fmt("minDelaySecForRetry %zu", gp.minDelaySecForRetry));
    ret.push_back(fmt("maxDelaySecForRetry %zu", gp.maxDelaySecForRetry));
    ret.push_back(fmt("retryTimeout %zu", gp.retryTimeout));
    ret.push_back(fmt("maxConnections %zu", gp.maxConnections));
    ret.push_back(fmt("maxForegroundTasks %zu", gp.maxForegroundTasks));
    ret.push_back(fmt("maxBackgroundTasks %zu", gp.maxBackgroundTasks));
    ret.push_back(fmt("maxConversionMb %zu", gp.maxConversionMb));
    ret.push_back(fmt("socketTimeout %zu", gp.socketTimeout));
    ret.push_back(fmt("keepAlive %s", gp.keepAliveParams.toStr().c_str()));

    const std::vector<std::pair<ProxyTask, int64_t> > tqv = gp.taskQueue.getAll();
    ret.push_back(fmt("-----TaskQueue %zu-----", tqv.size()));
    for (const auto &pair : tqv) {
        const ProxyTask &task = pair.first;
        const int64_t &timeDiffMs = pair.second;
        std::stringstream ss;
        ss << "volume " << task.volId
           << " archive " << task.archiveName
           << " timeDiffMs " << timeDiffMs;
        ret.push_back(ss.str());
    }

    ret.push_back("-----Volume-----");
    for (const std::string &volId : gp.stMap.getKeyList()) {
        ProxyVolState &volSt = getProxyVolState(volId);
        UniqueLock ul(volSt.mu);
        const std::string state = volSt.sm.get();
        if (state == pClear) continue;
        const ProxyVolInfo volInfo = getProxyVolInfo(volId);
        const uint64_t totalSize = volInfo.getTotalDiffFileSize();
        const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
        const std::string tsStr = util::timeToPrintable(volSt.lastWlogReceivedTime);
        ret.push_back(
            fmt("volume %s state %s numDiff %zu totalSize %s timestamp %s"
                , volId.c_str(), state.c_str(), volSt.diffMgr.size()
                , totalSizeStr.c_str(), tsStr.c_str()));

        const std::vector<int> actionNum = volSt.ac.getValues(volSt.archiveSet);
        size_t i = 0;
        for (const std::string& archiveName : volSt.archiveSet) {
            const MetaDiffManager &mgr = volSt.diffMgrMap.get(archiveName);
            const uint64_t totalSize = volInfo.getTotalDiffFileSize(archiveName);
            const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
            uint64_t minGid, maxGid;
            std::tie(minGid, maxGid) = mgr.getMinMaxGid();
            const std::string tsStr = util::timeToPrintable(volSt.lastWdiffSentTimeMap[archiveName]);
            ret.push_back(
                fmt("  archive %s action %s numDiff %zu"
                    " totalSize %s minGid %" PRIu64 " maxGid %" PRIu64 " %s"
                    , archiveName.c_str()
                    , actionNum[i] == 0 ? "None" : "WdiffSend"
                    , mgr.size(), totalSizeStr.c_str(), minGid, maxGid
                    , tsStr.c_str()));
            i++;
        }
    }
    return ret;
}


StrVec getVolStatusAsStrVec(const std::string &volId)
{
    StrVec ret;
    const auto &fmt = cybozu::util::formatString;

    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);

    const std::string state = volSt.sm.get();
    ret.push_back(fmt("hostType proxy"));
    ret.push_back(fmt("volume %s", volId.c_str()));
    ret.push_back(fmt("state %s", state.c_str()));
    if (state == pClear) return ret;

    const ProxyVolInfo volInfo = getProxyVolInfo(volId);
    const size_t numDiff = volSt.diffMgr.size();
    const uint64_t sizeLb = volInfo.getSizeLb();
    const std::string sizeS = cybozu::util::toUnitIntString(sizeLb * LOGICAL_BLOCK_SIZE);
    const uint64_t totalSize = volInfo.getTotalDiffFileSize();
    const std::string totalSizeStr = cybozu::util::toUnitIntString(totalSize);
    const std::string tsStr = util::timeToPrintable(volSt.lastWlogReceivedTime);
    const int totalNumAction = getTotalNumActions(volSt.ac, volSt.archiveSet);

    ret.push_back(fmt("sizeLb %" PRIu64, sizeLb));
    ret.push_back(fmt("size %s", sizeS.c_str()));
    ret.push_back(fmt("numDiff %zu", numDiff));
    ret.push_back(fmt("totalSize %s", totalSizeStr.c_str()));
    ret.push_back(fmt("lastWlogReceivedTime %s", tsStr.c_str()));
    ret.push_back(fmt("totalNumAction %d", totalNumAction));

    ret.push_back("-----Archive-----");
    size_t i = 0;
    for (const std::string& archiveName : volSt.archiveSet) {
        const MetaDiffManager &mgr = volSt.diffMgrMap.get(archiveName);
        const HostInfoForBkp hi = volInfo.getArchiveInfo(archiveName);
        const std::string tsStr = util::timeToPrintable(volSt.lastWdiffSentTimeMap[archiveName]);
        const char *action = volSt.ac.getValue(archiveName) == 0 ? "None" : "WdiffSend";
        const bool isWdiffSendError = volSt.actionState.get(archiveName);

        ret.push_back(fmt("  archive %s", archiveName.c_str()));
        ret.push_back(fmt("  host %s", hi.addrPort.str().c_str()));
        ret.push_back(fmt("  compression %s", hi.cmpr.str().c_str()));
        ret.push_back(fmt("  wdiffSendDelay %u", hi.wdiffSendDelaySec));
        ret.push_back(fmt("  action %s", action));
        ret.push_back(fmt("  numDiff %zu", mgr.size()));
        ret.push_back(fmt("  lastWdiffSentTime %s", tsStr.c_str()));
        ret.push_back(fmt("  isWdiffSendError %d", isWdiffSendError));

        const MetaDiffVec diffV = mgr.getAll();
        uint64_t totalSize = 0;
        uint64_t minTs = -1;
        for (const MetaDiff &diff : diffV) {
            totalSize += diff.dataSize;
            if (minTs > diff.timestamp) minTs = diff.timestamp;
        }
        ret.push_back(fmt("  wdiffTotalSize %" PRIu64 "", totalSize));
        uint64_t sendDelay = 0;
        if (!diffV.empty()) sendDelay = ::time(0) - minTs;
        ret.push_back(fmt("  wdiffSendDelayMeasured %" PRIu64, sendDelay));

        i++;
    }
    return ret;
}


void pushAllTasksForVol(const std::string &volId, Logger *loggerP)
{
    ProxyVolState &volSt = getProxyVolState(volId);
    volSt.actionState.clearAll();
    if (loggerP) loggerP->info() << "pushAllTasksForVol:volId" << volId;
    for (const std::string& archiveName : volSt.archiveSet) {
        if (loggerP) loggerP->info() << "pushAllTasksForVol:archiveName" << archiveName;
        pushTaskForce(ProxyTask(volId, archiveName), 0);
    }
}


void gcProxyVol(const std::string &volId)
{
    const char * const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    ProxyVolInfo volInfo = getProxyVolInfo(volId);

    // Retry to make hard links of wdiff files in the target directory.
    const MetaDiffVec diffV = volInfo.tryToMakeHardlinkInSendtoDir();
    volInfo.deleteDiffs(diffV);

    // Here the target directory must contain no wdiff file.
    if (!volSt.diffMgr.getAll().empty()) {
        throw cybozu::Exception(FUNC)
            << "there are wdiff files in the target directory"
            << volId;
    }

    // Collect temporary files.
    const size_t nr = volInfo.gcTmpFiles();
    if (nr > 0) {
        LOGs.info() << volId << "garbage collected tmp files" << nr;
    }
}


bool hasDiffs(ProxyVolState &volSt)
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
void stopAndEmptyProxyVol(const std::string &volId)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);

    Stopper stopper(volSt.stopState);
    if (!stopper.changeFromNotStopping(WaitingForEmpty)) {
        throw cybozu::Exception(FUNC) << "already under stopping wlog receiver" << volId;
    }

    waitUntil(ul, [&]() {
            return isStateIn(volSt.sm.get(), pSteadyStates);
        }, FUNC);

    verifyStateIn(volSt.sm.get(), {pStarted}, FUNC);
    StateMachineTransaction tran(volSt.sm, pStarted, ptWaitForEmpty);

    waitUntil(ul, [&]() {
            const bool hasDiffs = proxy_local::hasDiffs(volSt);
            if (hasDiffs) {
                for (const std::string &archiveName : volSt.archiveSet) {
                    pushTask(ProxyTask(volId, archiveName));
                }
                return false;
            } else {
                return volSt.ac.isAllZero(volSt.archiveSet);
            }
        }, FUNC);

    if (!stopper.changeFromWaitingForEmpty(Stopping)) {
        throw cybozu::Exception(FUNC) << "BUG : not here, already under stopping wdiff sender" << volId;
    }

    removeAllTasksForVol(volId);
    tran.commit(pStopped);
}


/**
 *   pStarted --> pStopped
 */
void stopProxyVol(const std::string &volId, bool isForce)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);

    Stopper stopper(volSt.stopState);
    if (!stopper.changeFromNotStopping(isForce ? ForceStopping : Stopping)) {
        throw cybozu::Exception(FUNC) << "already under stopping wlog" << volId;
    }

    waitUntil(ul, [&]() {
            return isStateIn(volSt.sm.get(), pSteadyStates)
                && volSt.ac.isAllZero(volSt.archiveSet);
        }, FUNC);

    const std::string &stFrom = volSt.sm.get();
    if (stFrom != pStarted) {
        throw cybozu::Exception(FUNC) << "bad state" << stFrom;
    }

    StateMachineTransaction tran(volSt.sm, stFrom, ptStop, FUNC);
    ul.unlock();

    removeAllTasksForVol(volId);
    tran.commit(pStopped);
}


void listArchiveInfo(const std::string &volId, StrVec &archiveNameV)
{
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    archiveNameV.assign(volSt.archiveSet.begin(), volSt.archiveSet.end());
}

void getArchiveInfo(const std::string& volId, const std::string &archiveName, HostInfoForBkp &hi)
{
    const char *const FUNC = __func__;
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    ProxyVolInfo volInfo = getProxyVolInfo(volId);
    if (!volInfo.existsArchiveInfo(archiveName)) {
        throw cybozu::Exception(FUNC) << "archive info not exists" << archiveName;
    }
    hi = volInfo.getArchiveInfo(archiveName);
}


void addArchiveInfo(const std::string &volId, const std::string &archiveName, const HostInfoForBkp &hi, bool ensureNotExistance)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyActionNotRunning(volSt.ac, volSt.archiveSet, FUNC);
    const std::string &curr = volSt.sm.get(); // pStopped or pClear

    StateMachineTransaction tran(volSt.sm, curr, ptAddArchiveInfo);
    ul.unlock();
    ProxyVolInfo volInfo = getProxyVolInfo(volId);
    if (curr == pClear) volInfo.init();
    volInfo.addArchiveInfo(archiveName, hi, ensureNotExistance);
    tran.commit(pStopped);
}


void deleteArchiveInfo(const std::string &volId, const std::string &archiveName)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyActionNotRunning(volSt.ac, volSt.archiveSet, FUNC);

    StateMachineTransaction tran(volSt.sm, pStopped, ptDeleteArchiveInfo);
    ul.unlock();
    ProxyVolInfo volInfo = getProxyVolInfo(volId);
    volInfo.deleteArchiveInfo(archiveName);
    ul.lock();
    bool shouldClear = volInfo.notExistsArchiveInfo();
    if (shouldClear) volInfo.clear();
    tran.commit(shouldClear ? pClear : pStopped);
}


/**
 * Use DiffMemory (SortedDiffWriter).
 *
 * RETURN:
 *   false if force stopped.
 */
bool recvWlogAndWriteDiff(
    cybozu::Socket &sock, int fd, const cybozu::Uuid &uuid, uint32_t pbs, uint32_t salt,
    const std::atomic<int> &stopState, const ProcessStatus &ps, int wlogFd)
{
    DiffMemory diffMem;
    diffMem.header().setUuid(uuid);

    LogPackHeader packH(pbs, salt);
    WlogReceiver receiver(sock, pbs, salt);

    bool isWlogHeaderWritten = false;
    std::unique_ptr<WlogWriter> wlogW;
    if (wlogFd >= 0) wlogW.reset(new WlogWriter(wlogFd));

    while (receiver.popHeader(packH)) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        if (wlogW) {
            if (!isWlogHeaderWritten) {
                WlogFileHeader wh;
                wh.init(pbs, salt, uuid, packH.logpackLsid(), MAX_LSID);
                wlogW->writeHeader(wh);
                isWlogHeaderWritten = true;
            }
            wlogW->writePackHeader(packH.header());
        }
        AlignedArray buf;
        for (size_t i = 0; i < packH.header().n_records; i++) {
            WlogRecord &lrec = packH.record(i);
            receiver.popIo(lrec, buf);
            if (wlogW) wlogW->writePackIo(buf);
            DiffRecord drec;
            if (convertLogToDiff(lrec, buf.data(), drec)) {
                if (!drec.isNormal()) buf.clear();
                diffMem.add(drec, std::move(buf));
            }
            buf.clear();
        }
    }
    if (wlogW) wlogW->close();
    diffMem.writeTo(fd);
    return true;
}

/**
 * Use IndexedDiffWriter
 */
bool recvWlogAndWriteDiff2(
    cybozu::Socket &sock, int fd, const cybozu::Uuid &uuid, uint32_t pbs, uint32_t salt,
    const std::atomic<int> &stopState, const ProcessStatus &ps, int wlogFd)
{
    unusedVar(wlogFd);

    IndexedDiffWriter writer;
    writer.setFd(fd);

    DiffFileHeader header;
    header.setUuid(uuid);
    header.type = WALB_DIFF_TYPE_INDEXED;
    writer.writeHeader(header);

    LogPackHeader packH(pbs, salt);
    WlogReceiver receiver(sock, pbs, salt);

    while (receiver.popHeader(packH)) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        AlignedArray data;
        for (size_t i = 0; i < packH.header().n_records; i++) {
            WlogRecord &lrec = packH.record(i);
            receiver.popIo(lrec, data);
            IndexedDiffRecord drec;
            if (convertLogToDiff(lrec, data.data(), drec)) {
                writer.compressAndWriteDiff(drec, data.data());
            }
        }
    }
    writer.finalize();
    return true;
}


void isWdiffSendError(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    const IsWdiffSendErrorParam param = parseIsWdiffSendErrorParamForGet(p.params);
    const std::string &volId = param.volId;
    const std::string &archiveName = param.archiveName;

    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    if (volSt.sm.get() == pClear) {
        throw cybozu::Exception(FUNC) << "bad state" << volId << pClear;
    }
    if (volSt.archiveSet.find(archiveName) == volSt.archiveSet.end()) {
        throw cybozu::Exception(FUNC) << "bad archive name" << volId << archiveName;
    }
    const bool isWdiffSendError = volSt.actionState.get(archiveName);
    ul.unlock();
    protocol::sendValueAndFin(p, size_t(isWdiffSendError));
}


StrVec getLatestSnapForVolume(const std::string& volId)
{
    auto &fmt = cybozu::util::formatString;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string state = volSt.sm.get();
    StrVec ret;
    if (state == pClear) return ret;

    const ProxyVolInfo volInfo = getProxyVolInfo(volId);
    for (const std::string& archiveName : volSt.archiveSet) {
        std::string line = fmt("name:%s\t", volId.c_str());
        line += "kind:proxy\t";
        line += fmt("archive:%s\t", archiveName.c_str());
        const MetaDiffManager &mgr = volSt.diffMgrMap.get(archiveName);
        MetaDiff diff;
        if (mgr.getDiffWithMaxGid(diff)) {
            line += fmt("gid:%" PRIu64 "\t", diff.snapE.gidB);
            line += fmt("timestamp:%s", cybozu::unixTimeToPrettyStr(diff.timestamp).c_str());
        } else {
            line += fmt("gid:\t");
            line += fmt("timestamp:");
        }
        ret.push_back(std::move(line));
    }
    return ret;
}


void getLatestSnap(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    VolIdOrAllParam param = parseVolIdOrAllParam(p.params, 1);
    StrVec ret;
    if (param.isAll) {
        for (const std::string &volId : gp.stMap.getKeyList()) {
            cybozu::util::moveToTail(ret, getLatestSnapForVolume(volId));
        }
    } else {
        cybozu::util::moveToTail(ret, getLatestSnapForVolume(param.volId));
        if (ret.empty()) {
            throw cybozu::Exception(FUNC)
                << "could not get latest snapshot for volume" << param.volId;
        }
    }
    protocol::sendValueAndFin(p, ret);
}

void getHandlerStat(protocol::GetCommandParams &p)
{
    const protocol::HandlerStat stat = getProxyGlobal().handlerStatMgr.getStatByMove();
    const StrVec ret = prettyPrintHandlerStat(stat);
    protocol::sendValueAndFin(p, ret);
    p.logger.debug() << "get handler-stat succeeded";
}

static MetaDiffVec getAllWdiffDetail(protocol::GetCommandParams &p)
{
    const KickParam param = parseVolIdAndArchiveNameParamForGet(p.params);

    ProxyVolState &volSt = getProxyVolState(param.volId);
    UniqueLock ul(volSt.mu);

    const MetaDiffManager &mgr = param.archiveName.empty() ?
        volSt.diffMgr : volSt.diffMgrMap.get(param.archiveName);
    return mgr.getAll();
}

void getProxyDiffList(protocol::GetCommandParams &p)
{
    const MetaDiffVec diffV = getAllWdiffDetail(p);
    StrVec v;
    for (const MetaDiff &diff : diffV) {
        v.push_back(formatMetaDiff("", diff));
    }
    protocol::sendValueAndFin(p, v);
}

} // namespace proxy_local

} // namespace walb
