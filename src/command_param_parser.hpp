#pragma once
/**
 * Command parameters parser (and verifier)
 */
#include "walb_types.hpp"
#include "host_info.hpp"
#include "uuid.hpp"
#include "meta.hpp"
#include "stop_opt.hpp"
#include "cybozu/exception.hpp"

namespace walb {

namespace command_param_parser_local {

bool isOrVerifyVolIdFormat(const std::string& s, bool doThrowError);

} // namespace command_param_parser_local


inline void verifyVolIdFormat(const std::string& s)
{
    command_param_parser_local::isOrVerifyVolIdFormat(s, true);
}


inline bool isVolIdFormat(const std::string& s)
{
    return command_param_parser_local::isOrVerifyVolIdFormat(s, false);
}


std::string parseVolIdParam(const StrVec &args, size_t pos);


struct VolIdOrAllParam
{
    bool isAll;
    std::string volId;
};


VolIdOrAllParam parseVolIdOrAllParam(const StrVec &args, size_t pos);


struct InitVolParam
{
    std::string volId;
    std::string wdevPath;
};


InitVolParam parseInitVolParam(const StrVec &args, bool needWdevPath);


struct VolIdAndGidParam
{
    std::string volId;
    uint64_t gid;
};


/**
 * defaltGid will be used only if needGid is false.
 */
VolIdAndGidParam parseVolIdAndGidParam(const StrVec &args, size_t pos, bool needGid, uint64_t defaultGid);


struct VolIdAndLsidParam
{
    std::string volId;
    uint64_t lsid;
};


VolIdAndLsidParam parseVolIdAndLsidParam(const StrVec &args);


struct StartParam
{
    std::string volId;
    bool isTarget;
};


StartParam parseStartParam(const StrVec &args, bool needRole);


struct StopParam
{
    std::string volId;
    StopOpt stopOpt;
};


StopParam parseStopParam(const StrVec &args, bool allowEmpty);


struct ReplicateParam
{
    std::string volId;
    bool isSize;
    uint64_t param2;
    HostInfoForRepl hostInfo;
};


ReplicateParam parseReplicateParam(const StrVec &args);


struct MergeParam
{
    std::string volId;
    uint64_t gidB;
    bool isSize;
    uint64_t param3;
};


MergeParam parseMergeParam(const StrVec &args);


struct ResizeParam
{
    std::string volId;
    uint64_t newSizeLb; // logical block.
    bool doZeroClear;
};


ResizeParam parseResizeParam(const StrVec &args, bool allowZeroClear, bool allowZeroSize);


struct VirtualFullScanParam
{
    std::string volId;
    uint64_t gid;
    uint64_t bulkLb;
    uint64_t sizeLb; // 0 means whole device size.
};


VirtualFullScanParam parseVirtualFullScanParam(const StrVec &args);


struct VirtualFullScanCmdParam
{
    std::string devPath;
    VirtualFullScanParam param;
};


VirtualFullScanCmdParam parseVirtualFullScanCmdParam(const StrVec &args);


struct SetUuidParam
{
    std::string volId;
    cybozu::Uuid uuid;
};


SetUuidParam parseSetUuidParam(const StrVec &args);


struct SetStateParam
{
    std::string volId;
    std::string state;
};


SetStateParam parseSetStateParam(const StrVec &args);


struct SetBaseParam
{
    std::string volId;
    MetaState metaSt;
};


SetBaseParam parseSetBaseParam(const StrVec &args);


struct ChangeSnapshotParam
{
    std::string volId;
    std::vector<uint64_t> gidL;
};


ChangeSnapshotParam parseChangeSnapshotParam(const StrVec &args);


struct ArchiveInfoParam
{
    std::string cmd;
    std::string volId;
};


ArchiveInfoParam parseArchiveInfoParam(const StrVec &args);


struct KickParam
{
    std::string volId;
    std::string archiveName;
};


KickParam parseKickParam(const StrVec &args);
uint64_t parseSetFullScanBps(const StrVec &args);


struct BackupParam
{
    std::string volId;
    uint64_t bulkLb;
};


BackupParam parseBackupParam(const StrVec &args);
bool parseShutdownParam(const StrVec &args);


inline void verifyNoneParam(const StrVec &) {}

inline void verifyVolIdParam(const StrVec &args) { parseVolIdParam(args, 0); }
inline void verifyVolIdOrAllParam(const StrVec &args) { parseVolIdOrAllParam(args, 0); }
inline void verifyInitVolParam(const StrVec &args) { parseInitVolParam(args, false); }
inline void verifyResetVolParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, false, 0); }
inline void verifyStartParam(const StrVec &args) { parseStartParam(args, false); }
inline void verifyStopParam(const StrVec &args) { parseStopParam(args, true); }
inline void verifyRestoreParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, true, 0); }
inline void verifyDelRestoredParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, true, 0); }
inline void verifyDelColdParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, true, 0); }
inline void verifyReplicateParam(const StrVec &args) { parseReplicateParam(args); }
inline void verifyApplyParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, true, 0); }
inline void verifyMergeParam(const StrVec &args) { parseMergeParam(args); }
inline void verifyResizeParam(const StrVec &args) { parseResizeParam(args, true, true); }
inline void verifyVirtualFullScanParam(const StrVec &args) { parseVirtualFullScanParam(args); }
inline void verifyVirtualFullScanCmdParam(const StrVec &args) { parseVirtualFullScanCmdParam(args); }
inline void verifySetUuidParam(const StrVec &args) { parseSetUuidParam(args); }
inline void verifySetStateParam(const StrVec &args) { parseSetStateParam(args); }
inline void verifySetBaseParam(const StrVec &args) { parseSetBaseParam(args); }
inline void verifyChangeSnapshotParam(const StrVec &args) { parseChangeSnapshotParam(args); }
inline void verifyArchiveInfoParam(const StrVec &args) { parseArchiveInfoParam(args); }
inline void verifyKickParam(const StrVec &args) { parseKickParam(args); }
inline void verifySetFullScanBps(const StrVec &args) { parseSetFullScanBps(args); }
inline void verifyBackupParam(const StrVec &args) { parseBackupParam(args); }
inline void verifyShutdownParam(const StrVec &args) { parseShutdownParam(args); }
inline void verifyDumpLogpackHeader(const StrVec &args) { parseVolIdAndLsidParam(args); }


struct VolIdAndGidRangeParam
{
    std::string volId;
    uint64_t gid[2];
};

inline VolIdAndGidRangeParam parseVolIdAndGidRangeParamForGet(const StrVec &args)
{
    VolIdAndGidRangeParam param;
    std::string gidS[2];
    cybozu::util::parseStrVec(args, 1, 1, {&param.volId, &gidS[0], &gidS[1]});
    param.gid[0] = 0;
    param.gid[1] = UINT64_MAX;
    for (size_t i = 0; i < 2; i++) {
        if (!gidS[i].empty()) param.gid[i] = cybozu::atoi(gidS[i]);
    }
    if (param.gid[0] >= param.gid[1]) {
        throw cybozu::Exception(__func__) << "bad gid range" << param.gid[0] << param.gid[1];
    }
    return param;
};

struct ExistsDiffParam
{
    std::string volId;
    MetaDiff diff;
};

inline ExistsDiffParam parseExistsDiffParamForGet(const StrVec &args)
{
    ExistsDiffParam param;
    std::string volId, gidS[4];
    uint64_t gid[4];
    cybozu::util::parseStrVec(args, 1, 5, {&volId, &gidS[0], &gidS[1], &gidS[2], &gidS[3]});
    for (size_t i = 0; i < 4; i++) {
        gid[i] = cybozu::atoi(gidS[i]);
    }
    param.diff.snapB.set(gid[0], gid[1]);
    param.diff.snapE.set(gid[2], gid[3]);
    param.diff.verify();
    return param;
}

struct RestorableParam
{
    std::string volId;
    bool isAll;
};

inline RestorableParam parseRestorableParamForGet(const StrVec &args)
{
    RestorableParam param;
    std::string opt;
    cybozu::util::parseStrVec(args, 1, 1, {&param.volId, &opt});
    if (opt.empty()) {
        param.isAll = false;
    } else if (opt == "all") {
        param.isAll = true;
    } else {
        throw cybozu::Exception(__func__) << "bad opt" << opt;
    }
    return param;
}

struct NumActionParam
{
    std::string volId;
    std::string action;
};

inline NumActionParam parseNumActionParamForGet(const StrVec &args)
{
    NumActionParam param;
    cybozu::util::parseStrVec(args, 1, 2, {&param.volId, &param.action});
    return param;
}

struct IsWdiffSendErrorParam
{
    std::string volId;
    std::string archiveName;
};

inline IsWdiffSendErrorParam parseIsWdiffSendErrorParamForGet(const StrVec &args)
{
    IsWdiffSendErrorParam param;
    cybozu::util::parseStrVec(args, 1, 2, {&param.volId, &param.archiveName});
    return param;
}

struct GetMetaStateParam
{
    std::string volId;
    uint64_t gid; // -1 means the latest one.
    bool isApplying;
};

inline GetMetaStateParam parseGetMetaStateParam(const StrVec &args)
{
    GetMetaStateParam param;
    std::string isApplyingStr;
    std::string gidStr;
    cybozu::util::parseStrVec(args, 1, 2, {&param.volId, &isApplyingStr, &gidStr});
    if (isApplyingStr == "0") {
        param.isApplying = false;
    } else if (isApplyingStr == "1") {
        param.isApplying = true;
    } else {
        throw cybozu::Exception("bad isApplying parameter. specify 0 or 1.") << isApplyingStr;
    }
    if (gidStr.empty()) {
        param.gid = UINT64_MAX;
    } else {
        param.gid = cybozu::atoi(gidStr);
    }
    return param;
}

inline void verifyVolIdParamForGet(const StrVec &args) { parseVolIdParam(args, 1); }
inline void verifyVolIdOrAllParamForGet(const StrVec &args) { parseVolIdOrAllParam(args, 1); }
inline void verifyApplicableDiffParamForGet(const StrVec &args) { parseVolIdAndGidParam(args, 1, false, UINT64_MAX); }
inline void verifyVolIdAndGidRangeParamForGet(const StrVec &args) { parseVolIdAndGidRangeParamForGet(args); }
inline void verifyExistsDiffParamForGet(const StrVec &args) { parseExistsDiffParamForGet(args); }
inline void verifyRestorableParamForGet(const StrVec &args) { parseRestorableParamForGet(args); }
inline void verifyNumActionParamForGet(const StrVec &args) { parseNumActionParamForGet(args); }
inline void verifyIsWdiffSendErrorParamForGet(const StrVec &args) { parseIsWdiffSendErrorParamForGet(args); }
inline void verifyGetMetaSnapParam(const StrVec &args) { parseVolIdAndGidParam(args, 1, false, UINT64_MAX); }
inline void verifyGetMetaStateParam(const StrVec &args) { parseGetMetaStateParam(args); }

} // namespace walb
