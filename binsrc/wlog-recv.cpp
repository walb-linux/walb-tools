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
        uint32_t pbs, salt, sizePb;
        packet.read(name);
        packet.read(uuid);
        packet.read(diff);
        packet.read(pbs);
        packet.read(salt);
        packet.read(sizePb);

        logger.info("name %s", name.c_str());
        logger.info("uuid %s", uuid.str().c_str());
        logger.info("diff %s", diff.str().c_str());
        logger.info("pbs %" PRIu32 "", pbs);
        logger.info("salt %" PRIu32 "", salt);
        logger.info("sizePb %" PRIu32 "", sizePb);

        packet::Answer ans(sock_);
        //ans.ok();
        ans.ng(1, "error for test.");
        /* now editing */
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
