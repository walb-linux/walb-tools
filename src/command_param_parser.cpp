#include "command_param_parser.hpp"

namespace walb {

namespace command_param_parser_local {

bool isOrVerifyVolIdFormat(const std::string& s, bool doThrowError)
{
    if (s.empty() || s.size() > 64) {
        if (doThrowError) {
            throw cybozu::Exception("bad s format: size") << s << s.size();
        } else {
            return false;
        }
    }
    for (const char &c : s) {
        if (!('0' <= c && c <= '9') &&
            !('a' <= c && c <= 'z') &&
            !('A' <= c && c <= 'Z') &&
            c != '-') {
            if (doThrowError) {
                throw cybozu::Exception("bad volId format: bad character") << s << c;
            } else {
                return false;
            }
        }
    }
    return true;
}

} // namespace command_param_parser_local


std::string parseVolIdParam(const StrVec &args, size_t pos)
{
    std::string volId;
    cybozu::util::parseStrVec(args, pos, 1, {&volId});
    verifyVolIdFormat(volId);
    return volId;
}


VolIdOrAllParam parseVolIdOrAllParam(const StrVec &args, size_t pos)
{
    VolIdOrAllParam param;
    cybozu::util::parseStrVec(args, pos, 0, {&param.volId});
    param.isAll = param.volId.empty();
    if (!param.isAll) verifyVolIdFormat(param.volId);
    return param;
}


InitVolParam parseInitVolParam(const StrVec &args, bool needWdevPath)
{
    InitVolParam param;
    const size_t n = needWdevPath ? 2 : 1;
    cybozu::util::parseStrVec(args, 0, n, {&param.volId, &param.wdevPath});
    verifyVolIdFormat(param.volId);
    return param;
}


VolIdAndGidParam parseVolIdAndGidParam(const StrVec &args, size_t pos, bool needGid, uint64_t defaultGid)
{
    VolIdAndGidParam param;
    const size_t n = needGid ? 2 : 1;
    std::string gidStr;
    cybozu::util::parseStrVec(args, pos, n, {&param.volId, &gidStr});
    verifyVolIdFormat(param.volId);
    if (gidStr.empty()) {
        param.gid = defaultGid;
    } else {
        param.gid = cybozu::atoi(gidStr);
    }
    return param;
}


VolIdAndLsidParam parseVolIdAndLsidParam(const StrVec &args)
{
    VolIdAndLsidParam param;
    std::string lsidStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &lsidStr});
    verifyVolIdFormat(param.volId);
    param.lsid = cybozu::atoi(lsidStr);
    return param;
}


StartParam parseStartParam(const StrVec &args, bool needRole)
{
    StartParam param;
    const size_t n = needRole ? 2 : 1;
    std::string role;
    cybozu::util::parseStrVec(args, 0, n, {&param.volId, &role});
    verifyVolIdFormat(param.volId);
    if (role == "target") {
        param.isTarget = true;
    } else if (role == "standby") {
        param.isTarget = false;
    } else if (needRole) {
        throw cybozu::Exception(__func__) << "specify role: target or standby" << role;
    }
    return param;
}


StopParam parseStopParam(const StrVec &args, bool allowEmpty)
{
    StopParam param;
    std::string stopStr;
    cybozu::util::parseStrVec(args, 0, 1, {&param.volId, &stopStr});
    verifyVolIdFormat(param.volId);
    if (!stopStr.empty()) {
        param.stopOpt.parse(stopStr);
    }
    if (!allowEmpty && param.stopOpt.isEmpty()) {
        throw cybozu::Exception(__func__) << "stop empty is not allowed.";
    }
    return param;
}


ReplicateParam parseReplicateParam(const StrVec &args)
{
    ReplicateParam param;
    std::string type, param2Str;
    cybozu::util::parseStrVec(args, 0, 3, {&param.volId, &type, &param2Str});
    verifyVolIdFormat(param.volId);
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


MergeParam parseMergeParam(const StrVec &args)
{
    const char *const FUNC = __func__;
    MergeParam param;
    std::string gidBStr, type, param3Str;
    cybozu::util::parseStrVec(args, 0, 4, {&param.volId, &gidBStr, &type, &param3Str});
    verifyVolIdFormat(param.volId);
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


ResizeParam parseResizeParam(const StrVec &args, bool allowZeroClear, bool allowZeroSize)
{
    const char *const FUNC = __func__;
    ResizeParam param;
    std::string newSizeLbStr, doZeroClearStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &newSizeLbStr, &doZeroClearStr});
    verifyVolIdFormat(param.volId);
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


VirtualFullScanParam parseVirtualFullScanParam(const StrVec &args)
{
    VirtualFullScanParam param;
    std::string gidStr, bulkSizeU, sizeU;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &gidStr, &bulkSizeU, &sizeU});
    verifyVolIdFormat(param.volId);
    param.gid = cybozu::atoi(gidStr);
    if (bulkSizeU.empty()) {
        param.bulkLb = DEFAULT_BULK_LB;
    } else {
        param.bulkLb = util::parseBulkLb(bulkSizeU, __func__);
    }
    if (sizeU.empty()) {
        param.sizeLb = 0;
    } else {
        param.sizeLb = util::parseSizeLb(sizeU, __func__);
    }
    return param;
}


VirtualFullScanCmdParam parseVirtualFullScanCmdParam(const StrVec &args)
{
    const char *const FUNC = __func__;
    VirtualFullScanCmdParam param;
    cybozu::util::parseStrVec(args, 0, 1, {&param.devPath});
    if (param.devPath.empty()) {
        throw cybozu::Exception(FUNC) << "devPath is empty";
    }
    StrVec args1(++args.begin(), args.end());
    param.param = parseVirtualFullScanParam(args1);
    return param;
}


SetUuidParam parseSetUuidParam(const StrVec &args)
{
    SetUuidParam param;
    std::string uuidStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &uuidStr});
    verifyVolIdFormat(param.volId);
    param.uuid.set(uuidStr);
    return param;
}


SetStateParam parseSetStateParam(const StrVec &args)
{
    SetStateParam param;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &param.state});
    verifyVolIdFormat(param.volId);
    return param;
}


SetBaseParam parseSetBaseParam(const StrVec &args)
{
    SetBaseParam param;
    std::string metaStStr;
    cybozu::util::parseStrVec(args, 0, 2, {&param.volId, &metaStStr});
    verifyVolIdFormat(param.volId);
    param.metaSt = strToMetaState(metaStStr);
    return param;
}


ChangeSnapshotParam parseChangeSnapshotParam(const StrVec &args)
{
    ChangeSnapshotParam param;
    cybozu::util::parseStrVec(args, 0, 1, {&param.volId});
    verifyVolIdFormat(param.volId);
    for (size_t i = 1; i < args.size(); i++) {
        param.gidL.push_back(cybozu::atoi(args[i]));
    }
    if (param.gidL.empty()) {
        throw cybozu::Exception(__func__) << "specify gid list.";
    }
    return param;
}


ArchiveInfoParam parseArchiveInfoParam(const StrVec &args)
{
    ArchiveInfoParam param;
    cybozu::util::parseStrVec(args, 0, 2, {&param.cmd, &param.volId});
    verifyVolIdFormat(param.volId);
    for (const char *cmd : {"add", "update", "get", "delete", "list"}) {
        if (param.cmd == cmd) return param;
    }
    throw cybozu::Exception(__func__) << "bad command name" << param.cmd;
}


KickParam parseKickParam(const StrVec &args)
{
    KickParam param;
    cybozu::util::parseStrVec(args, 0, 0, {&param.volId, &param.archiveName});
    if (!param.volId.empty()) verifyVolIdFormat(param.volId);
    return param;
}

KickParam parseVolIdAndArchiveNameParamForGet(const StrVec &args)
{
    KickParam param;
    cybozu::util::parseStrVec(args, 1, 1, {&param.volId, &param.archiveName});
    verifyVolIdFormat(param.volId);
    return param;
}

uint64_t parseSetFullScanBps(const StrVec &args)
{
    std::string sizeStr;
    cybozu::util::parseStrVec(args, 0, 1, {&sizeStr});
    return cybozu::util::fromUnitIntString(sizeStr);
}


BackupParam parseBackupParam(const StrVec &args)
{
    BackupParam param;
    std::string bulkLbStr;
    cybozu::util::parseStrVec(args, 0, 1, {&param.volId, &bulkLbStr});
    verifyVolIdFormat(param.volId);
    if (bulkLbStr.empty()) {
        param.bulkLb = DEFAULT_BULK_LB;
    } else {
        param.bulkLb = util::parseBulkLb(bulkLbStr, __func__);
    }
    return param;
}


bool parseShutdownParam(const StrVec &args)
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


} // namespace walb
