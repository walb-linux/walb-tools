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

inline std::string parseVolIdParam(const StrVec &args, size_t pos)
{
    std::string volId;
    cybozu::util::parseStrVec(args, pos, 1, {&volId});
    return volId;
}

struct StatusParam
{
    bool isAll;
    std::string volId;
};

inline StatusParam parseStatusParam(const StrVec &args)
{
    StatusParam param;
    cybozu::util::parseStrVec(args, 0, 0, {&param.volId});
    param.isAll = param.volId.empty();
    return param;
}

struct InitVolParam
{
    std::string volId;
    std::string wdevPath;
};

inline InitVolParam parseInitVolParam(const StrVec &args, bool needWdevPath)
{
    InitVolParam param;
    const size_t n = needWdevPath ? 2 : 1;
    cybozu::util::parseStrVec(args, 0, n, {&param.volId, &param.wdevPath});
    return param;
}

struct VolIdAndGidParam
{
    std::string volId;
    uint64_t gid;
};

/**
 * defaltGid will be used only if needGid is false.
 */
inline VolIdAndGidParam parseVolIdAndGidParam(const StrVec &args, size_t pos, bool needGid, uint64_t defaultGid)
{
    VolIdAndGidParam param;
    const size_t n = needGid ? 2 : 1;
    std::string gidStr;
    cybozu::util::parseStrVec(args, pos, n, {&param.volId, &gidStr});
    if (gidStr.empty()) {
        param.gid = defaultGid;
    } else {
        param.gid = cybozu::atoi(gidStr);
    }
    return param;
}

struct VolIdAndLsidParam
{
    std::string volId;
    uint64_t lsid;
};

inline VolIdAndLsidParam parseVolIdAndLsidParam(const StrVec &args)
{
    VolIdAndLsidParam param;
    std::string lsidStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &lsidStr});
    param.lsid = cybozu::atoi(lsidStr);
    return param;
}

struct StartParam
{
    std::string volId;
    bool isTarget;
};

inline StartParam parseStartParam(const StrVec &args, bool needRole)
{
    StartParam param;
    const size_t n = needRole ? 2 : 1;
    std::string role;
    cybozu::util::parseStrVec(args, 0, n, {&param.volId, &role});
    if (role == "target") {
        param.isTarget = true;
    } else if (role == "standby") {
        param.isTarget = false;
    } else if (needRole) {
        throw cybozu::Exception(__func__) << "specify role: target or standby" << role;
    }
    return param;
}

struct StopParam
{
    std::string volId;
    StopOpt stopOpt;
};

inline StopParam parseStopParam(const StrVec &args, bool allowEmpty)
{
    StopParam param;
    std::string stopStr;
    cybozu::util::parseStrVec(args, 0, 1, {&param.volId, &stopStr});
    if (!stopStr.empty()) {
        param.stopOpt.parse(stopStr);
    }
    if (!allowEmpty && param.stopOpt.isEmpty()) {
        throw cybozu::Exception(__func__) << "stop empty is not allowed.";
    }
    return param;
}

struct ReplicateParam
{
    std::string volId;
    bool isSize;
    uint64_t param2;
    HostInfoForRepl hostInfo;
};

inline ReplicateParam parseReplicateParam(const StrVec &args)
{
    ReplicateParam param;
    std::string type, param2Str;
    cybozu::util::parseStrVec(args, 0, 3, {&param.volId, &type, &param2Str});
    if (type == "size") {
        param.isSize = true;
    } else if (type == "gid") {
        param.isSize = false;
    } else {
        throw cybozu::Exception(__func__) << "specify size or gid" << type;
    }
    param.param2 = cybozu::atoi(param2Str);
    param.hostInfo = parseHostInfoForRepl(args, 3);
    return param;
}

struct MergeParam
{
    std::string volId;
    uint64_t gidB;
    bool isSize;
    uint64_t param3;
};

inline MergeParam parseMergeParam(const StrVec &args)
{
    const char *const FUNC = __func__;
    MergeParam param;
    std::string gidBStr, type, param3Str;
    cybozu::util::parseStrVec(args, 0, 4, {&param.volId, &gidBStr, &type, &param3Str});
    param.gidB = cybozu::atoi(gidBStr);
    if (type == "size") {
        param.isSize = true;
    } else if (type == "gid") {
        param.isSize = false;
    } else {
        throw cybozu::Exception(FUNC) << "specify size or gid" << type;
    }
    param.param3 = cybozu::atoi(param3Str);
    if (!param.isSize) {
        if (param.param3 <= param.gidB) {
            throw cybozu::Exception(FUNC) << "bad gid range" << param.gidB << param.param3;
        }
    }
    return param;
}

struct ResizeParam
{
    std::string volId;
    uint64_t newSizeLb; // logical block.
    bool doZeroClear;
};

inline ResizeParam parseResizeParam(const StrVec &args, bool allowZeroClear, bool allowZeroSize)
{
    const char *const FUNC = __func__;
    ResizeParam param;
    std::string newSizeLbStr, doZeroClearStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &newSizeLbStr, &doZeroClearStr});
    const uint64_t newSizeLb = util::parseSizeLb(newSizeLbStr, FUNC);
    bool doZeroClear;
    if (doZeroClearStr.empty()) {
        doZeroClear = false;
    } else if (doZeroClearStr == "zeroclear") {
        if (!allowZeroClear) throw cybozu::Exception(FUNC) << "zeroclear option not allowed.";
        doZeroClear = true;
    } else {
        throw cybozu::Exception(FUNC) << "bad param" << doZeroClearStr;
    }
    if (!allowZeroSize && newSizeLb == 0) {
        throw cybozu::Exception(FUNC) << "size param must not be 0";
    }
    param.newSizeLb = newSizeLb;
    param.doZeroClear = doZeroClear;
    return param;
}

struct BlockHashParam
{
    std::string volId;
    uint64_t gid;
    uint64_t bulkLb;
};

inline BlockHashParam parseBlockHashParam(const StrVec &args)
{
    BlockHashParam param;
    std::string gidStr, bulkSizeU;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &gidStr, &bulkSizeU});
    param.gid = cybozu::atoi(gidStr);
    if (bulkSizeU.empty()) {
        param.bulkLb = DEFAULT_BULK_LB;
    } else {
        param.bulkLb = util::parseBulkLb(bulkSizeU, __func__);
    }
    return param;
};

struct SetUuidParam
{
    std::string volId;
    cybozu::Uuid uuid;
};

inline SetUuidParam parseSetUuidParam(const StrVec &args)
{
    SetUuidParam param;
    std::string uuidStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &uuidStr});
    param.uuid.set(uuidStr);
    return param;
}

struct SetStateParam
{
    std::string volId;
    std::string state;
};

inline SetStateParam parseSetStateParam(const StrVec &args)
{
    SetStateParam param;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &param.state});
    return param;
}

struct SetBaseParam
{
    std::string volId;
    MetaState metaSt;
};

inline SetBaseParam parseSetBaseParam(const StrVec &args)
{
    SetBaseParam param;
    std::string metaStStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &metaStStr});
    param.metaSt = strToMetaState(metaStStr);
    return param;
}

struct ChangeSnapshotParam
{
    std::string volId;
    std::vector<uint64_t> gidL;
};

inline ChangeSnapshotParam parseChangeSnapshotParam(const StrVec &args)
{
    ChangeSnapshotParam param;
    cybozu::util::parseStrVec(args, 0, 1, {&param.volId});
    for (size_t i = 1; i < args.size(); i++) {
        param.gidL.push_back(cybozu::atoi(args[i]));
    }
    if (param.gidL.empty()) {
        throw cybozu::Exception(__func__) << "specify gid list.";
    }
    return param;
}

struct ArchiveInfoParam
{
    std::string cmd;
    std::string volId;
};

inline ArchiveInfoParam parseArchiveInfoParam(const StrVec &args)
{
    ArchiveInfoParam param;
    cybozu::util::parseStrVec(args, 0, 2, {&param.cmd, &param.volId});
    for (const char *cmd : {"add", "update", "get", "delete", "list"}) {
        if (param.cmd == cmd) return param;
    }
    throw cybozu::Exception(__func__) << "bad command name" << param.cmd;
}

struct KickParam
{
    std::string volId;
    std::string archiveName;
};

inline KickParam parseKickParam(const StrVec &args)
{
    KickParam param;
    cybozu::util::parseStrVec(args, 0, 0, {&param.volId, &param.archiveName});
    return param;
}

struct BackupParam
{
    std::string volId;
    uint64_t bulkLb;
};

inline BackupParam parseBackupParam(const StrVec &args)
{
    BackupParam param;
    std::string bulkLbStr;
    cybozu::util::parseStrVec(args, 0, 1, {&param.volId, &bulkLbStr});
    if (bulkLbStr.empty()) {
        param.bulkLb = DEFAULT_BULK_LB;
    } else {
        param.bulkLb = util::parseBulkLb(bulkLbStr, __func__);
    }
    return param;
}

inline bool parseShutdownParam(const StrVec &args)
{
    std::string forceStr;
    cybozu::util::parseStrVec(args, 0, 0, {&forceStr});
    bool isForce;
    if (forceStr.empty() || forceStr == "graceful") {
        isForce = false;
    } else if (forceStr == "force") {
        isForce = true;
    } else {
        throw cybozu::Exception(__func__) << "bad option" << forceStr;
    }
    return isForce;
}


inline void verifyNoneParam(const StrVec &) {}

inline void verifyVolIdParam(const StrVec &args) { parseVolIdParam(args, 0); }
inline void verifyStatusParam(const StrVec &args) { parseStatusParam(args); }
inline void verifyInitVolParam(const StrVec &args) { parseInitVolParam(args, false); }
inline void verifyResetVolParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, false, 0); }
inline void verifyStartParam(const StrVec &args) { parseStartParam(args, false); }
inline void verifyStopParam(const StrVec &args) { parseStopParam(args, true); }
inline void verifyRestoreParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, true, 0); }
inline void verifyDelRestoredParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, true, 0); }
inline void verifyReplicateParam(const StrVec &args) { parseReplicateParam(args); }
inline void verifyApplyParam(const StrVec &args) { parseVolIdAndGidParam(args, 0, true, 0); }
inline void verifyMergeParam(const StrVec &args) { parseMergeParam(args); }
inline void verifyResizeParam(const StrVec &args) { parseResizeParam(args, true, true); }
inline void verifyBlockHashParam(const StrVec &args) { parseBlockHashParam(args); }
inline void verifySetUuidParam(const StrVec &args) { parseSetUuidParam(args); }
inline void verifySetStateParam(const StrVec &args) { parseSetStateParam(args); }
inline void verifySetBaseParam(const StrVec &args) { parseSetBaseParam(args); }
inline void verifyChangeSnapshotParam(const StrVec &args) { parseChangeSnapshotParam(args); }
inline void verifyArchiveInfoParam(const StrVec &args) { parseArchiveInfoParam(args); }
inline void verifyKickParam(const StrVec &args) { parseKickParam(args); }
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

inline void verifyVolIdParamForGet(const StrVec &args) { parseVolIdParam(args, 1); }
inline void verifyApplicableDiffParamForGet(const StrVec &args) { parseVolIdAndGidParam(args, 1, false, UINT64_MAX); }
inline void verifyVolIdAndGidRangeParamForGet(const StrVec &args) { parseVolIdAndGidRangeParamForGet(args); }
inline void verifyExistsDiffParamForGet(const StrVec &args) { parseExistsDiffParamForGet(args); }
inline void verifyRestorableParamForGet(const StrVec &args) { parseRestorableParamForGet(args); }
inline void verifyNumActionParamForGet(const StrVec &args) { parseNumActionParamForGet(args); }
inline void verifyIsWdiffSendErrorParamForGet(const StrVec &args) { parseIsWdiffSendErrorParamForGet(args); }

} // namespace walb
