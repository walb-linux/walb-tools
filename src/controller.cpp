#include "controller.hpp"

namespace walb {

void c2pArchiveInfoClient(protocol::ClientParams &p)
{
    const char * const FUNC = __func__;
    std::string cmd, volId;
    cybozu::util::parseStrVec(p.params, 0, 2, {&cmd, &volId});
    const char *acceptCmdTbl[] = {
        "list", "add", "update", "get", "delete"
    };
    bool found = false;
    for (const char *p : acceptCmdTbl) {
        if (cmd == p) {
            found = true;
            break;
        }
    }
    if (!found) throw cybozu::Exception(FUNC) << "bad command" << cmd;

    protocol::sendStrVec(p.sock, {cmd, volId}, 2, FUNC);
    packet::Packet pkt(p.sock);
    if (cmd != "list") {
        std::string archiveId;
        cybozu::util::parseStrVec(p.params, 2, 1, {&archiveId});
        pkt.write(archiveId);
    }
    if (cmd == "add" || cmd == "update") {
        const HostInfoForBkp hi = parseHostInfoForBkp(p.params, 3);
        LOGs.debug() << hi;
        pkt.write(hi);
    }
    pkt.flush();

    std::string res;
    pkt.read(res);
    if (res != msgOk) {
        throw cybozu::Exception(FUNC) << "not ok" << res;
    }

    if (cmd == "list") {
        StrVec v;
        pkt.read(v);
        for (const std::string& s : v) {
            std::cout << s << std::endl;
        }
        return;
    }
    if (cmd == "get") {
        HostInfoForBkp hi;
        pkt.read(hi);
        std::cout << hi << std::endl;
        return;
    }
}

void c2sSnapshotClient(protocol::ClientParams &p)
{
    const char *const FUNC = __func__;
    protocol::sendStrVec(p.sock, p.params, 1, FUNC);
    packet::Packet pkt(p.sock);

    std::string res;
    pkt.read(res);
    if (res != msgOk) {
        throw cybozu::Exception(FUNC) << "not ok" << res;
    }

    uint64_t gid;
    pkt.read(gid);
    std::cout << gid << std::endl;
}

void c2aBlockHashClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgAccept);

    packet::StreamControl ctrl(p.sock);
    while (ctrl.isDummy()) ctrl.reset();

    packet::Packet pkt(p.sock);
    std::string msg;
    pkt.read(msg);
    if (msg != msgOk) {
        throw cybozu::Exception(__func__) << "failed" << msg;
    }
    cybozu::murmurhash3::Hash hash;
    pkt.read(hash);
    std::cout << hash << std::endl;
}

void virtualFullScanClient(
    const std::string &devPath, packet::Packet& pkt, size_t bulkLb, uint64_t fsyncIntervalSize)
{
    const char *const FUNC = __func__;

    uint64_t sizeLb;
    pkt.read(sizeLb);

    cybozu::util::File file;
    if (devPath == "stdout") {
        file.setFd(1);
    } else {
        const cybozu::FileStat stat = cybozu::FilePath(devPath).stat();
        if (stat.exists() && stat.isBlock()) {
            file.open(devPath, O_RDWR);
            const uint64_t devSizeLb = cybozu::util::getBlockDeviceSize(file.fd()) / LOGICAL_BLOCK_SIZE;
            if (devSizeLb < sizeLb) {
                throw cybozu::Exception(FUNC) << "too small device size" << sizeLb << devSizeLb;
            }
        } else {
            file.open(devPath, O_WRONLY | O_TRUNC | O_CREAT, 0644);
        }
    }

    const size_t bulkSize = bulkLb * LOGICAL_BLOCK_SIZE;
    AlignedArray buf(bulkSize);
    AlignedArray encBuf(bulkSize);
    const AlignedArray zeroBuf(bulkSize, true);
    packet::StreamControl2 ctrl(pkt.sock());
    size_t writtenSize = 0;
    uint64_t remaining = sizeLb;
    for (;;) {
        ctrl.recv();
        if (ctrl.isEnd()) break;
        if (!ctrl.isNext()) throw cybozu::Exception(FUNC) << ctrl.toStr();
        const uint64_t lb = std::min<uint64_t>(remaining, bulkLb);
        const size_t bytes = lb * LOGICAL_BLOCK_SIZE;
        size_t encSize;
        pkt.read(encSize);
        if (encSize == 0) {
            file.write(zeroBuf.data(), bytes);
        } else {
            encBuf.resize(encSize);
            buf.resize(bytes);
            pkt.read(encBuf.data(), encSize);
            uncompressSnappy(encBuf, buf, FUNC);
            file.write(buf.data(), bytes);
        }
        writtenSize += bytes;
        if (writtenSize >= fsyncIntervalSize) {
            file.fdatasync();
            writtenSize = 0;
        }
        remaining -= lb;
    }
    if (remaining != 0) throw cybozu::Exception(FUNC) << "remaining must be 0" << remaining;
    file.fsync();
    file.close();

    packet::Ack(pkt.sock()).send();
    pkt.flush();
}

void c2aVirtualFullScanClient(protocol::ClientParams &p)
{
    const char *const FUNC = __func__;
    const VirtualFullScanCmdParam cmdParam = parseVirtualFullScanCmdParam(p.params);
    const StrVec args(++p.params.begin(), p.params.end());
    protocol::sendStrVec(p.sock, args, 0, __func__, msgAccept);
    packet::Packet pkt(p.sock);

    virtualFullScanClient(cmdParam.devPath, pkt, cmdParam.param.bulkLb, DEFAULT_FSYNC_INTERVAL_SIZE);

    std::string msg;
    pkt.read(msg);
    if (msg != msgOk) throw cybozu::Exception(FUNC) << "not ok";
}

const protocol::GetCommandInfoMap &getGetCommandInfoMap()
{
    static const protocol::GetCommandInfoMap m = {
        {isOverflowTN, {protocol::SizeType, verifyVolIdParamForGet, "[volId] get is-overflow boolean value."}},
        {logUsageTN, {protocol::StringVecType, verifyVolIdOrAllParamForGet, "[(volId)] get log usage and capacity in physical blocks for volume(s)."}},
        {isWdiffSendErrorTN, {protocol::SizeType, verifyIsWdiffSendErrorParamForGet, "[volId archiveName] get wdiff-send-error boolean value."}},
        {numActionTN, {protocol::SizeType, verifyNumActionParamForGet, "[volId actionName] get number of running actions."}},
        {stateTN, {protocol::StringType, verifyVolIdParamForGet, "[volId]"}},
        {hostTypeTN, {protocol::StringType, verifyNoneParam, "get host type as a string."}},
        {volTN, {protocol::StringVecType, verifyNoneParam, "get volume name list."}},
        {pidTN, {protocol::SizeType, verifyNoneParam, "get pid of the server process."}},
        {diffTN, {protocol::StringVecType, verifyVolIdAndGidRangeParamForGet, "[volId (gidB (gidE))] get diff list."}},
        {applicableDiffTN, {protocol::StringVecType, verifyApplicableDiffParamForGet, "[volId (maxGid)]"}},
        {totalDiffSizeTN, {protocol::SizeType, verifyVolIdAndGidRangeParamForGet, "[volId (gidB (gidE))] get total diff size in a range."}},
        {numDiffTN, {protocol::SizeType, verifyVolIdAndGidRangeParamForGet, "[volId (gidB (gidE))] get number of diff files in a range."}},
        {existsDiffTN, {protocol::SizeType, verifyExistsDiffParamForGet, "[volId gid0 gid1 gid2 gid3]"}},
        {existsBaseImageTN, {protocol::SizeType, verifyVolIdParamForGet, "[volId] 1 if base image exists, else 0."}},
        {restoredTN, {protocol::StringVecType, verifyVolIdParamForGet, "[volId] get restored clean snapshot list."}},
        {coldTN, {protocol::StringVecType, verifyVolIdParamForGet, "[volId] get cold snapshot list."}},
        {restorableTN, {protocol::StringVecType, verifyRestorableParamForGet, "[volId (all)] get restorable clean snapshot and timestamp list."}},
        {uuidTN, {protocol::StringType, verifyVolIdParamForGet, "[volId] get uuid of a volume."}},
        {archiveUuidTN, {protocol::StringType, verifyVolIdParamForGet, "[volId] get archive uuid of a volume."}},
        {baseTN, {protocol::StringType, verifyVolIdParamForGet, "[volId] get base(meta state) of a volume."}},
        {volSizeTN, {protocol::SizeType, verifyVolIdParamForGet, "[volId] get volume size [logical block]."}},
        {progressTN, {protocol::SizeType, verifyVolIdParamForGet, "[volId] get progress of full/hash backup/replication [logical block]."}},
        {volumeGroupTN, {protocol::StringType, verifyNoneParam, "get volume group name"}},
        {thinpoolTN, {protocol::StringType, verifyNoneParam, "get thinpool name (may be emtpy string)"}},
        {allActionsTN, {protocol::StringVecType, verifyNoneParam, "get running action information for all the volumes."}},
        {getMetaSnapTN, {protocol::StringType, verifyGetMetaSnapParam, "[volId (gid)] get MetaSnap having gid."}},
        {getMetaStateTN, {protocol::StringType, verifyGetMetaStateParam, "[volId isApplying (gid)] get MetaState having gid."}},
        {getLatestSnapTN, {protocol::StringVecType, verifyVolIdOrAllParamForGet, "[(volId)] get latest snapshot information for volume(s)."}},
        {getTsDeltaTN, {protocol::StringVecType, verifyNoneParam, "get timestamp delta information."}},
    };
    return m;
}

void c2xGetClient(protocol::ClientParams &p)
{
    const char *const FUNC = __func__;
    if (p.params.empty()) throw cybozu::Exception(FUNC) << "target not specified";
    const std::string &targetName = p.params[0];

    const protocol::GetCommandInfo &info = protocol::getGetCommandInfo(targetName, getGetCommandInfoMap(), FUNC);
    info.verify(p.params);
    protocol::sendStrVec(p.sock, p.params, 0, FUNC, msgOk);
    protocol::recvValueAndPut(p.sock, info.valueType, FUNC);
}

} // namespace walb
