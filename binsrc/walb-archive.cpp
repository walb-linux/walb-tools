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

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb/archive";
const std::string DEFAULT_LOG_FILE = "-";
const std::string DEFAULT_VG = "vg";

using namespace walb;

struct Option : cybozu::Option
{
    uint16_t port;
    std::string logFileStr;
    bool isDebug;

    Option() {
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendBoolOpt(&isDebug, "debug", "put debug message.");

        ArchiveSingleton &a = getArchiveGlobal();
        appendOpt(&a.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        appendOpt(&a.volumeGroup, DEFAULT_VG, "vg", "lvm volume group.");
        appendOpt(&a.thinpool, "", "tp", "lvm thinpool (optional).");
        appendOpt(&a.maxForegroundTasks, DEFAULT_MAX_FOREGROUND_TASKS, "fg", "num of max concurrent foreground tasks.");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&a.nodeId, hostName, "id", "node identifier");
        appendOpt(&a.socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "to", "Socket timeout [sec].");

        appendHelp("h");
    }
};

void verifyArchiveData()
{
    const char *const FUNC = __func__;
    cybozu::FilePath baseDir(ga.baseDirStr);
    if (!baseDir.stat().isDirectory()) {
        throw cybozu::Exception(FUNC) << "base directory not found" << ga.baseDirStr;
    }
    if (!cybozu::lvm::vgExists(ga.volumeGroup)) {
        throw cybozu::Exception(FUNC) << "volume group does not exist" << ga.volumeGroup;
    }
    if (isThinpool() && !cybozu::lvm::tpExists(ga.volumeGroup, ga.thinpool)) {
        throw cybozu::Exception(FUNC) << "thinpool does not exist" << ga.thinpool;
    }
    for (const std::string &volId : util::getDirNameList(ga.baseDirStr)) {
          try {
              verifyArchiveVol(volId);
              gcArchiveVol(volId);
          } catch (std::exception &e) {
              LOGs.error() << __func__ << "start failed" << volId << e.what();
              ::exit(1);
          }
     }
}

int main(int argc, char *argv[]) try
{
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    ArchiveSingleton &g = getArchiveGlobal();
    util::setLogSetting(createLogFilePath(opt.logFileStr, g.baseDirStr), opt.isDebug);
    LOGs.info() << "starting walb archive server";
    LOGs.info() << opt;
    verifyArchiveData();
    util::makeDir(ga.baseDirStr, "ArchiveServer", false);
    server::MultiThreadedServer server;
    const size_t concurrency = g.maxForegroundTasks + 5;
    server.run(g.ps, opt.port, g.nodeId, archiveHandlerMap, concurrency);
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
