/**
 * @file
 * @brief WalB archive daemon.
 * @author HOSHINO Takashi
 *
 * (C) 2014 Cybozu Labs, Inc.
 */
#include "thread_util.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/serializer.hpp"
#include "cybozu/option.hpp"
#include "file_path.hpp"
#include "net_util.hpp"
#include "server_util.hpp"
#include "walb_util.hpp"
#include "archive.hpp"
#include "version.hpp"

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb/archive";
const std::string DEFAULT_LOG_FILE = "-";
const std::string DEFAULT_VG = "vg";

using namespace walb;

struct Option
{
    uint16_t port;
    std::string logFileStr;
    std::string discardTypeStr;
    bool isDebug;
    cybozu::Option opt;

    Option(int argc, char *argv[]) {
        opt.setDescription(util::getDescription("walb archive server"));

        opt.appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        opt.appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        opt.appendBoolOpt(&isDebug, "debug", "put debug message.");

        ArchiveSingleton &a = getArchiveGlobal();
        opt.appendOpt(&a.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        opt.appendOpt(&a.volumeGroup, DEFAULT_VG, "vg", "lvm volume group.");
        opt.appendOpt(&a.thinpool, "", "tp", "lvm thinpool (optional).");
        opt.appendOpt(&a.maxConnections, DEFAULT_MAX_CONNECTIONS, "maxconn", "num of max connections.");
        opt.appendOpt(&a.maxForegroundTasks, DEFAULT_MAX_FOREGROUND_TASKS, "fg", "num of max concurrent foreground tasks.");
        std::string hostName = cybozu::net::getHostName();
        opt.appendOpt(&a.nodeId, hostName, "id", "node identifier");
        opt.appendOpt(&a.socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "to", "Socket timeout [sec].");
        opt.appendOpt(&a.maxWdiffSendNr, DEFAULT_MAX_WDIFF_SEND_NR, "wn", "max number of wdiff files to send.");
        opt.appendOpt(&discardTypeStr, DEFAULT_DISCARD_TYPE_STR, "discard", "discard behavior: ignore/passdown/zero.");
        opt.appendOpt(&a.fsyncIntervalSize, DEFAULT_FSYNC_INTERVAL_SIZE, "fi", "fsync interval size [bytes].");
        opt.appendBoolOpt(&a.doAutoResize, "autoresize", "resize base image automatically if necessary");
#ifdef ENABLE_EXEC_PROTOCOL
        opt.appendBoolOpt(&a.allowExec, "allow-exec", "Allow exec protocol for test. This is NOT SECURE.");
#endif
        util::setKeepAliveOptions(opt, a.keepAliveParams);

        opt.appendHelp("h");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        util::verifyNotZero(a.maxConnections, "maxConnections");
        util::verifyNotZero(a.maxForegroundTasks, "maxForegroundTasks");
        util::verifyNotZero(a.maxWdiffSendNr, "maxWdiffSendNr");
        util::verifyNotZero(a.fsyncIntervalSize, "fsyncIntervalSize");
        a.discardType = parseDiscardType(discardTypeStr, __func__);
        a.keepAliveParams.verify();
    }
};

void initArchiveData()
{
    const char *const FUNC = __func__;
    cybozu::FilePath baseDir(ga.baseDirStr);
    if (!baseDir.stat().isDirectory()) {
        throw cybozu::Exception(FUNC) << "base directory not found" << ga.baseDirStr;
    }
    if (!cybozu::lvm::existsVg(ga.volumeGroup)) {
        throw cybozu::Exception(FUNC) << "volume group does not exist" << ga.volumeGroup;
    }
    if (isThinpool() && !cybozu::lvm::existsTp(ga.volumeGroup, ga.thinpool)) {
        throw cybozu::Exception(FUNC) << "thinpool does not exist" << ga.thinpool;
    }
    const StrVec volIdV = util::getDirNameList(ga.baseDirStr);
    LOGs.info() << "run lvs command (it may take long time)";
    const cybozu::lvm::LvList lvL = cybozu::lvm::listLv(ga.volumeGroup);
    LOGs.info() << "lvs command done.";
    const size_t nr = removeTemporaryRestoredSnapshots(lvL);
    if (nr > 0) LOGs.info() << "remove temporary snapshots" << nr;
    VolLvCacheMap map = getVolLvCacheMap(lvL, ga.thinpool, volIdV);

    LOGs.info() << "try to load metadata for volumes" << volIdV.size();
    for (VolLvCacheMap::value_type &p : map) {
        const std::string &volId = p.first;
        VolLvCache &lvC = p.second;
        try {
            getArchiveVolState(volId).lvCache = std::move(lvC);
            verifyArchiveVol(volId);
            gcArchiveVol(volId);
            LOGs.debug() << "init" << volId;
        } catch (std::exception &e) {
            LOGs.error() << __func__ << "start failed" << volId << e.what();
            ::exit(1);
        }
    }
}

int main(int argc, char *argv[]) try
{
    Option opt(argc, argv);
    ArchiveSingleton &g = getArchiveGlobal();
    util::setLogSetting(createLogFilePath(opt.logFileStr, g.baseDirStr), opt.isDebug);
    LOGs.info() << util::getDescription("starting walb archive server");
    LOGs.info() << opt.opt;
    initArchiveData();
    util::makeDir(ga.baseDirStr, "ArchiveServer", false);
    server::MultiThreadedServer server;
    const size_t concurrency = g.maxConnections;
    server.run(g.ps, opt.port, g.nodeId, archiveHandlerMap, g.handlerStatMgr,
               concurrency, g.keepAliveParams, g.socketTimeout);
    LOGs.info() << "shutdown walb archive server";

} catch (std::exception &e) {
    LOGe("ArchiveServer: error: %s", e.what());
    ::fprintf(::stderr, "ArchiveServer: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("ArchiveServer: caught other error.");
    ::fprintf(::stderr, "ArchiveServer: caught other error.");
    return 1;
}

/* end of file */
