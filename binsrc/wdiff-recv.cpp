/**
 * @file
 * @brief To send wdiff to a server.
 * @author MITSUNARI Shigeo
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "cybozu/socket.hpp"
#include "file_path.hpp"
#include "thread_util.hpp"
#include "net_util.hpp"
#include "tmp_file.hpp"
#include "server_util.hpp"
#include "protocol.hpp"
#include "uuid.hpp"
#include "meta.hpp"
#include "walb_log_file.hpp"
#include "walb_log_net.hpp"
#include "walb_logger.hpp"
#include "walb_diff_virt.hpp"
#include "walb_diff_pack.hpp"
#include "walb_diff_compressor.hpp"

namespace local
{
std::string baseDirStr;
} // local

using namespace walb;

class WdiffRequestWorker : public server::RequestWorker
{
public:
    using RequestWorker :: RequestWorker;
    void run() override {
        std::string clientId, protocolName;
        if (protocol::run1stNegotiateAsServer(sock_, nodeId_, protocolName, clientId, procStat_)) {
            return;
        }
        /* Original behavior for wdiff-recv command. */
        ProtocolLogger logger(nodeId_, clientId);
        if (protocolName != "wdiff-send") {
            logger.error("Protocol name must be wdiff-send.");
            return;
        }

        packet::Packet packet(sock_);

        std::string name; // not used
        cybozu::Uuid uuid;
        MetaDiff diff;
        uint16_t maxIoBlocks;
        packet.read(name);
        packet.read(uuid);
        packet.read(maxIoBlocks);
        packet.read(diff);

        logger.debug("name %s", name.c_str());
        logger.debug("uuid %s", uuid.str().c_str());
        logger.debug("maxIoBlocks %u", maxIoBlocks);
        logger.debug("diff %s", diff.str().c_str());

        packet::Answer ans(sock_);
        if (!isValidParams(logger, name)) {
            ans.ng(1, "error for test.");
            return;
        }
        ans.ok();
        logger.debug("send ans ok.");

        const std::string fName = createDiffFileName(diff);
        cybozu::TmpFile tmpFile(local::baseDirStr);
        cybozu::FilePath fPath(local::baseDirStr);
        fPath += fName;
        diff::Writer writer(tmpFile.fd());
        diff::FileHeaderRaw fileH;
        fileH.setMaxIoBlocksIfNecessary(maxIoBlocks);
        fileH.setUuid(uuid.rawData());
        writer.writeHeader(fileH);
        logger.debug("write header.");
        recvAndWriteDiffs(sock_, writer, logger);
        logger.debug("close.");
        writer.close();
        tmpFile.save(fPath.str());

        packet::Ack ack(sock_);
        ack.send();
    }
private:
    bool isValidParams(Logger &logger,
        const std::string &name) const {
        if (name.empty()) {
            logger.error("name is empty.");
            return false;
        }
        return true;
    }
    void recvAndWriteDiffs(cybozu::Socket &sock, diff::Writer &writer, Logger &logger) {
        packet::StreamControl ctrl(sock);
        while (ctrl.isNext()) {
            diff::PackHeader packH;
            sock.read(packH.rawData(), packH.rawSize());
            if (!packH.isValid()) {
                logAndThrow(logger, "recvAndWriteDiffs:bad packH");
            }
            for (size_t i = 0; i < packH.nRecords(); i++) {
                diff::IoData io;
                const walb_diff_record& rec = packH.record(i);
                io.set(rec);
                if (rec.data_size == 0) {
                    writer.writeDiff(rec, {});
                    continue;
                }
                sock.read(io.rawData(), rec.data_size);
                if (!io.isValid()) {
                    logAndThrow(logger, "recvAndWriteDiffs:bad io");
                }
                if (io.calcChecksum() != rec.checksum) {
                    logAndThrow(logger, "recvAndWriteDiffs:bad io checksum");
                }
                writer.writeDiff(rec, io.forMove());
            }
            ctrl.reset();
        }
        if (!ctrl.isEnd()) {
            throw cybozu::Exception("recvAndWriteDiffs:bad ctrl not end");
        }
    }
private:
    void logAndThrow(Logger& logger, const std::string& msg)
    {
        logger.error(msg);
        throw cybozu::Exception(msg);
    }
};

struct Option : cybozu::Option
{
    uint16_t port;
    std::string nodeId;
    std::string baseDirStr;

    Option() {
        appendMust(&port, "p", "port to listen");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&nodeId, hostName, "id", "node identifier");
        cybozu::FilePath curDir = cybozu::getCurrentDir();
        appendOpt(&baseDirStr, curDir.str(), "b", "base directory.");
        appendHelp("h");
    }
};

int main(int argc, char *argv[]) try
{
    cybozu::SetLogFILE(::stderr);
    cybozu::SetLogPriority(cybozu::LogDebug);

    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        throw RT_ERR("option error.");
    }
    cybozu::FilePath baseDir(opt.baseDirStr);
    if (!baseDir.stat().isDirectory()) {
        throw RT_ERR("%s is not directory.", baseDir.cStr());
    }
    local::baseDirStr = opt.baseDirStr;

    auto createReqWorker = [&](
        cybozu::Socket &&sock,
        std::atomic<server::ProcessStatus> &flag) {
        return std::make_shared<WdiffRequestWorker>(
            std::move(sock), opt.nodeId, flag);
    };
    std::atomic<bool> forceQuit;
    server::MultiThreadedServer server(forceQuit, 1);
    server.run(opt.port, createReqWorker);
    return 0;
} catch (std::exception &e) {
    LOGe("caught error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("caught other error.");
    return 1;
}
