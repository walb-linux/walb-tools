/**
 * @file
 * @brief WalB controller tool.
 */
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"
#include "util.hpp"
#include "net_util.hpp"
#include "controller.hpp"
#include "walb_util.hpp"
#include "command_param_parser.hpp"

using namespace walb;

void setupOpt(cybozu::Option& opt, const char *p = "opt...", const char *help = "")
{
    static StrVec sv;
    opt.appendParamVec(&sv, p, help);
}
void setupStrVec(cybozu::Option& opt)
{
    static StrVec sv;
    opt.appendParamVec(&sv, "opt...");
}
void setupVolId(cybozu::Option& opt)
{
    static std::string volId;
    opt.appendParam(&volId, "volId");
}
void setupInitVol(cybozu::Option& opt)
{
    setupVolId(opt);
    static std::string wdevPath;
    opt.appendParamOpt(&wdevPath, "", "wdevPath");
}
void setupResetVol(cybozu::Option& opt)
{
    setupVolId(opt);
    static uint64_t gid;
    opt.appendParamOpt(&gid, 0, "gid");
}
void setupBkp(cybozu::Option& opt)
{
    setupVolId(opt);
    static uint64_t size;
    opt.appendParamOpt(&size, 0, "bulkSize");
}
void setupVolIdGid(cybozu::Option& opt)
{
    setupVolId(opt);
    static uint64_t gid;
    opt.appendParam(&gid, "gid");
}
void setupVolIdLsid(cybozu::Option& opt)
{
    setupVolId(opt);
    static uint64_t lsid;
    opt.appendParam(&lsid, "lsid");
}
void setupStart(cybozu::Option& opt)
{
    setupVolId(opt);
    static std::string target;
    opt.appendParamOpt(&target, "", "target");
}
void setupStop(cybozu::Option& opt)
{
    setupVolId(opt);
    static std::string type;
    opt.appendParamOpt(&type, "", "(opt)", ": graceful|force|empty");
}
void setupArchiveInfo(cybozu::Option& opt)
{
    setupVolId(opt);
    static StrVec sv;
    const char *help =
        "get/delete <voldId> <archiveId>\n"
        "  add/update <volId> <archiveId> <addr>:<port> <cmprType>:<cmprLevel>:<cmprNumCPU> <wdiffSendDelaySec>";
    opt.appendParamVec(&sv, "<subcmd>", help);
}
void setupReplicate(cybozu::Option& opt)
{
    setupVolId(opt);
    setupOpt(opt, "(opt)", ": (sizeMb) (addr:port) (compression) (max diff size) (bulk size)");
}
void setupMerge(cybozu::Option& opt)
{
    setupVolIdGid(opt);
    setupOpt(opt, "opt", "(size|gid) (max size|gidE)");
}
void setupResize(cybozu::Option& opt)
{
    setupVolId(opt);
    static uint64_t size;
    static std::string zero;
    opt.appendParam(&size, "size");
    opt.appendParamOpt(&zero, "", "(opt)", ": zeroclear");
}
void setupKick(cybozu::Option& opt)
{
    setupOpt(opt, "(volId) (archiveName)");
}
void setupBlockHash(cybozu::Option& opt)
{
    setupVolIdGid(opt);
    static uint64_t size;
    opt.appendParamOpt(&size, 0, "(opt)", ": bulk size");
}
void setupUuid(cybozu::Option& opt)
{
    setupVolId(opt);
    static std::string uuid;
    opt.appendParam(&uuid, "uuid");
}
void setupSetState(cybozu::Option& opt)
{
    setupVolId(opt);
    static std::string state;
    opt.appendParam(&state, "state");
}
void setupShutdown(cybozu::Option& opt)
{
    static std::string force;
    opt.appendParamOpt(&force, "", "(opt)", ": force");
}

void setupDisableSnapshot(cybozu::Option& opt)
{
    setupVolId(opt);
    setupStrVec(opt);
}

void setupEnableSnapshot(cybozu::Option& opt)
{
    setupVolId(opt);
    setupStrVec(opt);
}

void setupGet(cybozu::Option& opt)
{
    std::string usage;
    usage += "usage: get NAME ARGUMENTS...\n";
    usage += "NAME list:\n";
    size_t max = 0;
    for (const protocol::GetCommandInfoMap::value_type &p : getGetCommandInfoMap()) {
        max = std::max(max, p.first.size());
    }
    max++;
    for (const protocol::GetCommandInfoMap::value_type &p : getGetCommandInfoMap()) {
        usage += cybozu::util::formatString("  %-*s%s\n", max, p.first.c_str(), p.second.helpMsg.c_str());
    }
    opt.setUsage(usage);
    static StrVec sv;
    opt.appendParamVec(&sv, "");
}

struct CommandInfo {
    std::string name;
    protocol::ClientHandler handler;
    void (*setup)(cybozu::Option&);
    void (*verify)(const StrVec&);
    const char *help;
};
const CommandInfo g_cmdTbl[] = {
    { statusCN, c2xGetStrVecClient, setupStrVec, verifyStatusParam, "print human-readable status." },
    { initVolCN, c2xInitVolClient, setupInitVol, verifyInitVolParam, "initialize a volume." },
    { clearVolCN, c2xClearVolClient, setupVolId, verifyVolIdParam, "clear a volume." },
    { resetVolCN, c2xResetVolClient, setupResetVol, verifyResetVolParam, "reset a volume in a server." },
    { fullBkpCN, c2sBackupClient, setupBkp, verifyBackupParam, "execute full-backup." },
    { hashBkpCN, c2sBackupClient, setupBkp, verifyBackupParam, "execute hash-backup." },
    { restoreCN, c2aRestoreClient, setupVolIdGid, verifyRestoreParam, "restore a volume in an archive server." },
    { delRestoredCN, c2aDelRestoredClient, setupVolIdGid, verifyDelRestoredParam, "delete a restored volume." },
    { startCN, c2xStartClient, setupStart, verifyStartParam, "start a volume in a server." },
    { stopCN, c2xStopClient, setupStop, verifyStopParam, "stop a volume in a server." },
    { archiveInfoCN, c2pArchiveInfoClient, setupArchiveInfo, verifyArchiveInfoParam, "control archive information in a proxy." },
    { snapshotCN, c2sSnapshotClient, setupVolId, verifyVolIdParam, "take a snapshot in a storage." },
    { disableSnapshotCN, c2aDisableSnapshot, setupDisableSnapshot, verifyChangeSnapshotParam, "disable a snapshot in an archive." },
    { enableSnapshotCN, c2aEnableSnapshot, setupEnableSnapshot, verifyChangeSnapshotParam, "enable a snapshot in an archive." },
    { replicateCN, c2aReplicateClient, setupReplicate, verifyReplicateParam, "replicate a volume from an archive to another archive." },
    { applyCN, c2aApplyClient, setupVolIdGid, verifyApplyParam, "apply old wdiff files to the base image of a volume in an archive." },
    { mergeCN, c2aMergeClient, setupMerge, verifyMergeParam, "merge wdiff files for a volume in an archive." },
    { resizeCN, c2xResizeClient, setupResize, verifyResizeParam, "resize a volume in a storage or an archive." },
    { kickCN, c2xKickClient, setupKick, verifyKickParam, "kick background tasks if necessary." },
    { blockHashCN, c2aBlockHashClient, setupBlockHash, verifyBlockHashParam, "calculate block hash of a volume in an archive." },
    { getCN, c2xGetClient, setupGet, verifyNoneParam, "get some information from a server." },
    { execCN, c2xGetStrVecClient, setupStrVec, verifyNoneParam, "execute a command-line at a server's side." },
    { shutdownCN, protocol::shutdownClient, setupShutdown, verifyShutdownParam, "shutdown a server process." },
    { dbgReloadMetadataCN, c2aReloadMetadataClient, setupVolId, verifyVolIdParam, "reload metadata of a volue in an archive (for debug)." },
    { dbgSetUuidCN, c2aSetUuidClient, setupUuid, verifySetUuidParam, "set uuid for a volume in an archive (for debug)." },
    { dbgSetStateCN, c2aSetStateClient, setupSetState, verifySetStateParam, "set state for a volume in an archive (for debug)." },
    { dbgSetBaseCN, c2aSetBaseClient, setupSetState, verifySetBaseParam, "set base(meta-state) for a volume in an archive (for debug)." },
    { dbgDumpLogpackHeaderCN, c2sDumpLogpackHeaderClient, setupVolIdLsid, verifyDumpLogpackHeader, "dump a logpack header block(for debug)." },
};

const CommandInfo* getCommand(const std::string& cmd)
{
    for (const CommandInfo& ci : g_cmdTbl) {
        if (cmd == ci.name) return &ci;
    }
    return nullptr;
}

struct Option
{
    cybozu::Option opt1;
    cybozu::Option opt2;
    const CommandInfo *pci;
    std::string addr;
    uint16_t port;
    std::string cmd;
    StrVec params;
    std::string ctrlId;
    bool isDebug;
    size_t socketTimeout;
    void setup1stOption() {
        size_t maxLen = 0;
        for (const CommandInfo& ci : g_cmdTbl) {
            maxLen = std::max(maxLen, ci.name.size());
        }
        std::string usage =
            "usage: walbc [<opt1>] <command> [<args>]\n\n"
            "Command list:\n";
        maxLen++;
        bool first = true;
        for (const CommandInfo& ci : g_cmdTbl) {
            if (first) {
                opt1.setDelimiter(ci.name, &params);
                first = false;
            } else {
                opt1.appendDelimiter(ci.name);
            }
            usage += cybozu::util::formatString("  %-*s%s\n", maxLen, ci.name.c_str(), ci.help);
        }
        opt1.setUsage(usage, true);
        opt1.appendMust(&addr, "a", "host name or address");
        opt1.appendMust(&port, "p", "port number");
        opt1.appendParamVec(&params, "parameters", "command parameters");
        opt1.appendBoolOpt(&isDebug, "debug", "put debug message.");
        opt1.appendOpt(&socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "to", "Socket timeout [sec].");
        const std::string hostName = cybozu::net::getHostName();
        opt1.appendOpt(&ctrlId, hostName, "id", "controller identfier");

        opt1.appendHelp("h");
    }
    int parse1(int argc, char *argv[]) {
        setup1stOption();
        if (!opt1.parse(argc, argv)) return 0;
        const int cmdPos = opt1.getNextPositionOfDelimiter();
        if (cmdPos == 0) return 0;
        const std::string cmdName = argv[cmdPos - 1];
        pci = getCommand(cmdName);
        if (pci == nullptr) return 0;
        pci->setup(opt2);
        opt2.appendHelp("h");
        return cmdPos;
    }
    int run(int argc, char *argv[]) {
        const int cmdPos = parse1(argc, argv);
        if (cmdPos == 0) {
            opt1.usage();
            return 1;
        }
		if (pci->name == execCN) { // `exec` cmd does not parse opt2
			if (cmdPos == argc) {
				opt2.usage();
				return 1;
			}
		} else {
	        if (!opt2.parse(argc, argv, cmdPos)) {
	            opt2.usage();
	            return 1;
	        }
		}
        util::setLogSetting("-", isDebug);
        pci->verify(params);

        cybozu::Socket sock;
        util::connectWithTimeout(sock, cybozu::SocketAddr(addr, port), socketTimeout);
        std::string serverId = protocol::run1stNegotiateAsClient(sock, ctrlId, pci->name);
        ProtocolLogger logger(ctrlId, serverId);
        protocol::ClientParams clientParams(sock, logger, params);
        pci->handler(clientParams);
        return 0;
    }
};


int main(int argc, char *argv[])
try {
    Option opt;
    return opt.run(argc, argv);
} catch (std::exception &e) {
    LOGe("walbc error: %s", e.what());
    return 1;
}
