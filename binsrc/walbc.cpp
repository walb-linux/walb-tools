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

void setupDelSnapshot(cybozu::Option& opt)
{
    setupVolId(opt);
    setupStrVec(opt);
}
struct CommandInfo {
    std::string name;
    protocol::ClientHandler handler;
    void (*setup)(cybozu::Option&);
    const char *help;
};
const CommandInfo g_cmdTbl[] = {
    { statusCN, c2xGetStrVecClient, setupStrVec, "" },
    { initVolCN, c2xInitVolClient, setupInitVol, "" },
    { clearVolCN, c2xClearVolClient, setupVolId, "" },
    { resetVolCN, c2xResetVolClient, setupResetVol, "" },
    { fullBkpCN, c2sFullBkpClient, setupBkp, "" },
    { hashBkpCN, c2sHashBkpClient, setupBkp, "" },
    { restoreCN, c2aRestoreClient, setupVolIdGid, "" },
    { delRestoredCN, c2aDelRestoredClient, setupVolIdGid, "" },
    { startCN, c2xStartClient, setupStart, "" },
    { stopCN, c2xStopClient, setupStop, "" },
    { archiveInfoCN, c2pArchiveInfoClient, setupArchiveInfo, "" },
    { snapshotCN, c2sSnapshotClient, setupVolId, "" },
    { replicateCN, c2aReplicateClient, setupReplicate, "" },
    { applyCN, c2aApplyClient, setupVolIdGid, "" },
    { mergeCN, c2aMergeClient, setupMerge, "" },
    { resizeCN, c2xResizeClient, setupResize, "" },
    { kickCN, c2xKickClient, setupKick, "" },
    { blockHashCN, c2aBlockHashClient, setupBlockHash, "" },
    { dbgReloadMetadataCN, c2aReloadMetadataClient, setupVolId, "" },
    { dbgSetUuid, c2aSetUuidClient, setupUuid, "" },
    { dbgSetState, c2aSetStateClient, setupSetState, "" },
    { dbgSetBase, c2aSetBaseClient, setupSetState, "" },
    { getCN, c2xGetClient, setupStrVec, "" },
    { execCN, c2xGetStrVecClient, setupStrVec, "" },
    { shutdownCN, protocol::shutdownClient, setupShutdown, "" },
    { delSnapshotCN, protocol::c2aDelSnapshot, setupDelSnapshot, "delete snapshot" },
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
    std::vector<std::string> params;
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
