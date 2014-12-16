#pragma once
#include "protocol.hpp"
#include "constant.hpp"
#include "host_info.hpp"
#include "murmurhash3.hpp"

namespace walb {

/**
 * Send parameters.
 * Receive string vector.
 */
inline void c2xGetStrVecClient(protocol::ClientParams &p)
{
    const char *const FUNC = __func__;
    protocol::sendStrVec(p.sock, p.params, 0, FUNC, msgOk);
    protocol::recvValueAndPut(p.sock, protocol::StringVecType, FUNC);
}

/**
 * For storage:
 *   params[0]: volId
 *   params[2]: wdevPath
 * For archive:
 *   params[0]: volId
 */
inline void c2xInitVolClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, "c2xInitVolClient", msgOk);
}

/**
 * Server is storage or archive.
 * params[0]: volId
 */
inline void c2xClearVolClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 1, "c2xClearVolClient", msgOk);
}

/**
 * For storage:
 *   params[0]: volId
 *   params[1]: target
 * For archive:
 *   params[0]: volId
 */
inline void c2xStartClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, "c2xStartClient", msgOk);
}

/**
 * params[0]: volId
 * params[1]: type (optional)
 *   "graceful" (default) or "force" or "empty".
 */
inline void c2xStopClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, "c2xStopClient", msgAccept);
}

namespace ctrl_local {

inline StrVec makeBkpParams(const StrVec &v)
{
    std::string volId;
    std::string bulkSize = cybozu::util::toUnitIntString(DEFAULT_BULK_LB * LOGICAL_BLOCK_SIZE);
    cybozu::util::parseStrVec(v, 0, 1, {&volId, &bulkSize});
    return {volId, bulkSize};
}

} // ctrl_local

inline void c2sFullBkpClient(protocol::ClientParams &p)
{
    const StrVec v = ctrl_local::makeBkpParams(p.params);
    protocol::sendStrVec(p.sock, v, 2, __func__, msgAccept);
}

inline void c2sHashBkpClient(protocol::ClientParams &p)
{
    const StrVec v = ctrl_local::makeBkpParams(p.params);
    protocol::sendStrVec(p.sock, v, 2, __func__, msgAccept);
}

/**
 * Restore command.
 * parameters: volId, gid
 */
inline void c2aRestoreClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, __func__, msgAccept);
}

/**
 * Delete-restored command.
 * parameters: volId, gid
 */
inline void c2aDelRestoredClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, __func__, msgOk);
}

/**
 * pattern (1)
 *   list <volId>
 * pattern (2)
 *   get/delete <volId> <archiveId>
 * pattern (3)
 *   add/update <volId> <archiveId> <addr>:<port> <cmprType>:<cmprLevel>:<cmprNumCPU> <wdiffSendDelaySec>
 *
 * <cmprType>: compression type. none, snappy, gzip, or lzma.
 * <cmprLevel>: compression level. integer from 0 to 9.
 * <cmprType>:<cmprLevel>:<cmprNumCPU> and <wdiffSendDelay> can be omitted.
 */
inline void c2pArchiveInfoClient(protocol::ClientParams &p)
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

/**
 * Take a snapshot which will be restorable at the archive site.
 *
 * params[0]: volId.
 *
 * Print the gid of the snapshot.
 */
inline void c2sSnapshotClient(protocol::ClientParams &p)
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

/**
 * params[0]: volId.
 *
 * !!!CAUSION!!!
 * This is for test and debug.
 */
inline void c2aReloadMetadataClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 1, __func__, msgOk);
}

/**
 * params[0]: volId.
 * params[1]: uuid string.
 *
 * !!!CAUSION!!!
 * This is for test and debug.
 */
inline void  c2aSetUuidClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, __func__, msgOk);
}

/**
 * params[0]: volId.
 * params[1]: state string.
 *
 * !!!CAUSION!!!
 * This is for test and debug.
 */
inline void  c2aSetStateClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, __func__, msgOk);
}

/**
 * params[0]: volId.
 * params[1]: MetaState string.
 *
 * !!!CAUSION!!!
 * This is for test and debug.
 */
inline void  c2aSetBaseClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, __func__, msgOk);
}

/**
 * params[0] volId
 * params[1] sizeMbStr. allowed size of remaining wdiffs [MiB].
 * params[2] archiveAddrPortStr like "192.168.1.1:10000".
 * params[3] archiveCompressionOptStr like "snappy:0:1" (optional)
 * params[4] archiveMaxWdiffMergeSizeStr like "100M" (optional)
 * params[5] archiveBulkSizeStr like "1M" (optional)
 */
inline void c2aReplicateClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgAccept);
}

/**
 * params[0]: volId
 * params[1]: gid: All snapshots where snap.gidB < gid will be deleted.
 */
inline void c2aApplyClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, __func__, msgAccept);
}

/**
 * params[0]: volId
 * params[1]: gidB: begin of the range.
 * params[2]: "size" or "gid" : type of params[3]
 * params[3]: case size: maxSizeMb; max size of total input wdiff files [MiB].
 *            case gid: gidE ; end of the range
 */
inline void c2aMergeClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 4, __func__, msgAccept);
}

/**
 * params[0]: volId
 * params[1]: size [byte] suffix k/m/g can be used.
 * params[2]: doZeroClear. "zeroclear". (optional)
 */
inline void c2xResizeClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
}

/**
 * params[0]: volId
 * params[1]: gid as string (optional)
 */
inline void c2xResetVolClient(protocol::ClientParams &p)
{
    const char *const FUNC = __func__;
    protocol::sendStrVec(p.sock, p.params, 0, FUNC, msgOk);
}

/**
 * For storage:
 *   No parameter is required.
 * For proxy:
 *   params[0]: volId
 *   params[1]: archiveName (optional)
 */
inline void c2xKickClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
}

/**
 * params[0]: volId
 * params[1]: gidStr
 * params[2]: blkSizeU (optional)
 */
inline void c2aBlockHashClient(protocol::ClientParams &p)
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

inline void c2xGetClient(protocol::ClientParams &p)
{
    const char *const FUNC = __func__;
    if (p.params.empty()) throw cybozu::Exception(FUNC) << "target not specified";
    const std::string &targetName = p.params[0];

    const protocol::ValueTypeMap typeM = {
        { isOverflowTN, protocol::SizeType },
        { isWdiffSendErrorTN, protocol::SizeType },
        { numActionTN, protocol::SizeType },
        { stateTN, protocol::StringType },
        { hostTypeTN, protocol::StringType },
        { volTN, protocol::StringVecType },
        { pidTN, protocol::SizeType },
        { diffTN, protocol::StringVecType },
        { totalDiffSizeTN, protocol::SizeType },
        { existsDiffTN, protocol::SizeType },
        { restoredTN, protocol::StringVecType },
        { restorableTN, protocol::StringVecType },
        { uuidTN, protocol::StringType },
        { baseTN, protocol::StringType },
    };
    const protocol::ValueType valType = protocol::getValueType(targetName, typeM, FUNC);
    protocol::sendStrVec(p.sock, p.params, 0, FUNC, msgOk);
    protocol::recvValueAndPut(p.sock, valType, FUNC);
}

} // namespace walb
