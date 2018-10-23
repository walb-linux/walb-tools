#pragma once
#include "protocol.hpp"
#include "constant.hpp"
#include "host_info.hpp"
#include "murmurhash3.hpp"
#include "bdev_util.hpp"
#include "snappy_util.hpp"

namespace walb {

/**
 * Send parameters.
 * Receive string vector.
 */
inline void c2xGetStrVecClient(protocol::ClientParams &p)
{
    const char *const FUNC = __func__;
    protocol::sendStrVec(p.sock, p.params, 0, FUNC, msgOk);
    protocol::recvValueAndCloseAndPut(p.sock, protocol::StringVecType, FUNC);
}


/**
 * For storage:
 *   params[0]: volId
 *   params[1]: wdevPath
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

inline void c2sBackupClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgAccept);
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

inline void c2aDelColdClient(protocol::ClientParams &p)
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
void c2pArchiveInfoClient(protocol::ClientParams &p);

/**
 * Take a snapshot which will be restorable at the archive site.
 *
 * params[0]: volId.
 *
 * Print the gid of the snapshot.
 */
void c2sSnapshotClient(protocol::ClientParams &p);

/**
 * Disable snapshots
 *
 * params[0]: volId.
 * params[1]: gid0
 * ...
 */
inline void c2aDisableSnapshot(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
}

/**
 * Enable snapshots
 *
 * params[0]: volId.
 * params[1]: gid0
 * ...
 */
inline void c2aEnableSnapshot(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
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
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
}

/**
 * For storage:
 *   No parameter is required.
 * For proxy:
 *   params[0]: volId (optional)
 *   params[1]: archiveName (optional)
 */
inline void c2xKickClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
}

inline void c2sSetFullScanBpsClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
}

/**
 * params[0]: volId
 * params[1]: gidStr
 * params[2]: blkSizeU (optional)
 * params[3]: scanSizeU (optional)
 */
void c2aBlockHashClient(protocol::ClientParams &p);

void virtualFullScanClient(
    const std::string &devPath, packet::Packet& pkt, size_t bulkLb, uint64_t fsyncIntervalSize);

/**
 * params[0]: device path or '-' for stdout.
 * params[1]: volId
 * params[2]: gidStr
 * params[3] blkSizeU (optional)
 * params[4]: scanSizeU (optional)
 */
void c2aVirtualFullScanClient(protocol::ClientParams &p);

inline void c2sDumpLogpackHeaderClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, __func__, msgOk);
}

inline void c2aGarbageCollectDiffClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 1, __func__, msgOk);
}

#ifndef NDEBUG
inline void c2xDebugClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, __func__, msgOk);
}
#endif

const protocol::GetCommandInfoMap &getGetCommandInfoMap();
void c2xGetClient(protocol::ClientParams &p);


} // namespace walb
