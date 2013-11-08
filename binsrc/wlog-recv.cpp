/**
 * @file
 * @brief To send wlog to a proxy.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "cybozu/socket.hpp"
#include "walb_log_file.hpp"
#include "walb_log_net.hpp"
#include "walb_logger.hpp"
#include "server_util.hpp"
#include "file_path.hpp"
#include "thread_util.hpp"
#include "net_util.hpp"
#include "protocol.hpp"

namespace walb {

class RequestWorker : public cybozu::thread::Runnable
{
private:
    cybozu::Socket sock_;
    std::string serverId_;
    cybozu::FilePath baseDir_;
    const std::atomic<bool> &forceQuit_;
    std::atomic<cybozu::server::ControlFlag> &ctrlFlag_;
public:
    RequestWorker(cybozu::Socket &&sock, const std::string &serverId,
                  const cybozu::FilePath &baseDir,
                  const std::atomic<bool> &forceQuit,
                  std::atomic<cybozu::server::ControlFlag> &ctrlFlag)
        : sock_(std::move(sock))
        , serverId_(serverId)
        , baseDir_(baseDir)
        , forceQuit_(forceQuit)
        , ctrlFlag_(ctrlFlag) {}
    void operator()() noexcept override try {
        run();
        sock_.close();
        done();
    } catch (...) {
        throwErrorLater();
        sock_.close();
    }
    void run() {
        std::string clientId;
        protocol::Protocol *protocol;
        if (protocol::run1stNegotiateAsServer(sock_, serverId_, clientId, &protocol, ctrlFlag_)) {
            return;
        }
        const auto pName = protocol::ProtocolName::WLOG_SEND;
        const std::string pStr = protocol::PROTOCOL_TYPE_MAP.at(pName);
        assert(protocol == protocol::ProtocolFactory::getInstance().findServer(pStr));

        /* Original behavior for wlog-recv command. */
        ProtocolLogger logger(serverId_, clientId);
        protocol::wlog_send::ServerRunner(pStr, sock_, logger, forceQuit_, {baseDir_.str()});

        packet::Packet packet(sock_);

        std::string name;
        cybozu::Uuid uuid;
        walb::MetaDiff diff;
        uint32_t pbs, salt;
        uint64_t bgnLsid, endLsid;
        packet.read(name);
        packet.read(uuid);
        packet.read(diff);
        packet.read(pbs);
        packet.read(salt);
        packet.read(bgnLsid);
        packet.read(endLsid);

        logger.debug("name %s", name.c_str());
        logger.debug("uuid %s", uuid.str().c_str());
        logger.debug("diff %s", diff.str().c_str());
        logger.debug("pbs %" PRIu32 "", pbs);
        logger.debug("salt %" PRIu32 "", salt);
        logger.debug("bgnLsid %" PRIu64 "", bgnLsid);
        logger.debug("endLsid %" PRIu64 "", endLsid);

        packet::Answer ans(sock_);
        if (!checkParams(logger, name, uuid, diff, pbs, salt, bgnLsid, endLsid)) {
            ans.ng(1, "error for test.");
            return;
        }
        ans.ok();
        logger.debug("send ans ok.");

        std::string fName = createDiffFileName(diff);
        fName += ".wlog";
        cybozu::TmpFile tmpFile(baseDir_.str());
        cybozu::FilePath fPath = baseDir_ + fName;
        log::Writer writer(tmpFile.fd());
        log::FileHeader fileH;
        const uint8_t *uuidP = static_cast<const uint8_t *>(uuid.rawData());
        fileH.init(pbs, salt, uuidP, bgnLsid, endLsid);
        writer.writeHeader(fileH);
        logger.debug("write header.");
        recvAndWriteLogs(sock_, writer, logger);
        logger.debug("close.");
        writer.close();
        tmpFile.save(fPath.str());

        packet::Ack ack(sock_);
        ack.send();
    }
private:
    bool checkParams(Logger &logger,
        const std::string &name,
        const cybozu::Uuid &,
        const walb::MetaDiff &diff,
        uint32_t pbs, uint32_t,
        uint64_t bgnLsid, uint64_t endLsid) const {

        if (name.empty()) {
            logger.error("name is empty.");
            return false;
        }
        if (!diff.isValid()) {
            logger.error("invalid diff.");
            return false;
        }
        if (!::is_valid_pbs(pbs)) {
            logger.error("invalid pbs.");
            return false;
        }
        if (endLsid < bgnLsid) {
            logger.error("invalid lsids.");
            return false;
        }
        return true;
    }
    void recvAndWriteLogs(cybozu::Socket &sock, log::Writer &writer, Logger &logger) {
        uint32_t pbs = writer.pbs();
        uint32_t salt = writer.salt();
        auto blk = cybozu::util::allocateBlocks<uint8_t>(pbs, pbs);
        log::PackHeaderRaw packH(blk, pbs, salt);
        log::Receiver receiver(sock, logger);
        receiver.setParams(pbs, salt);
        receiver.start();
        while (receiver.popHeader(packH.header())) {
            logger.debug("write header %" PRIu64 " %zu"
                         , packH.logpackLsid(), packH.nRecords());
            writer.writePackHeader(packH.header());
            for (size_t i = 0; i < packH.nRecords(); i++) {
                log::BlockDataVec blockD(pbs);
                receiver.popIo(packH.header(), i, blockD);
                const log::RecordWrapConst lrec(&packH, i);
                const log::PackIoWrapConst packIo(&lrec, &blockD);
                if (!packIo.isValid()) throw RT_ERR("packIo invalid.");
                if (lrec.isPadding()) blockD.resize(lrec.ioSizePb());
                logger.debug("write IO %zu %" PRIu32 "", i, blockD.nBlocks());
                writer.writePackIo(blockD);
            }
        }
    }
};

} // namespace walb

struct Option : cybozu::Option
{
    uint16_t port;
    std::string serverId;
    std::string baseDirStr;

    Option() {
        appendMust(&port, "p", "port to listen");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&serverId, hostName, "id", "host identifier");
        cybozu::FilePath curDir = cybozu::getCurrentDir();
        appendOpt(&baseDirStr, curDir.str(), "b", "base directory.");
        appendHelp("h");
    }
};

namespace walb {
namespace protocol {

void registerProtocolsForWlogRecvCommand()
{
    ProtocolFactory &factory = ProtocolFactory::getInstance();
    factory.registerServer<wlog_send::Server>(ProtocolName::WLOG_SEND);
}

}} // namespace walb::protocol

int main(int argc, char *argv[]) try
{
    cybozu::SetLogFILE(::stderr);

    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        throw RT_ERR("option error.");
    }
    cybozu::FilePath baseDir(opt.baseDirStr);
    if (!baseDir.stat().isDirectory()) {
        throw RT_ERR("%s is not directory.", baseDir.cStr());
    }
    walb::protocol::registerProtocolsForWlogRecvCommand();

    auto createReqWorker = [&](
        cybozu::Socket &&sock, const std::atomic<bool> &forceQuit, std::atomic<cybozu::server::ControlFlag> &flag) {
        return std::make_shared<walb::RequestWorker>(
            std::move(sock), opt.serverId, baseDir, forceQuit, flag);
    };
    cybozu::server::MultiThreadedServer server(1);
    server.run(opt.port, createReqWorker);
    return 0;
} catch (std::exception &e) {
    LOGe("caught error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("caught other error.");
    return 1;
}
