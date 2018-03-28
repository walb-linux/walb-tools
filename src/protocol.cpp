#include "protocol.hpp"
#include "description.hpp"

namespace walb {


cybozu::SocketAddr parseSocketAddr(const std::string &addrPort)
{
    const StrVec v = cybozu::Split(addrPort, ':', 2);
    if (v.size() != 2) {
        throw cybozu::Exception("parseSocketAddr:parse error") << addrPort;
    }
    return cybozu::SocketAddr(v[0], static_cast<uint16_t>(cybozu::atoi(v[1])));
}


std::vector<cybozu::SocketAddr> parseMultiSocketAddr(const std::string &multiAddrPort)
{
    std::vector<cybozu::SocketAddr> ret;
    const StrVec v = cybozu::Split(multiAddrPort, ',');
    for (const std::string &addrPort : v) {
        ret.emplace_back(parseSocketAddr(addrPort));
    }
    return ret;
}


namespace protocol {


std::string run1stNegotiateAsClient(
    cybozu::Socket &sock,
    const std::string &clientId, const std::string &protocolName)
{
    packet::Packet pkt(sock);
    pkt.write(clientId);
    pkt.write(protocolName);
    packet::Version ver(sock);
    ver.send();
    pkt.flush();
    std::string serverId;
    pkt.read(serverId);

    ProtocolLogger logger(clientId, serverId);
    std::string msg;
    pkt.read(msg);
    if (msg != msgOk) throw cybozu::Exception(__func__) << msg;
    return serverId;
}


void run1stNegotiateAsServer(
    cybozu::Socket &sock, const std::string &serverId,
    std::string &protocolName, std::string &clientId)
{
    const char *const FUNC = __func__;
    packet::Packet pkt(sock);

    pkt.read(clientId);
    pkt.read(protocolName);
    packet::Version ver(sock);
    const bool isVersionSame = ver.recv();
    pkt.write(serverId);
    LOGs.debug() << FUNC << clientId << protocolName << ver.get();

    if (!isVersionSame) {
        throw cybozu::Exception(FUNC) << "version differ c/s" << ver.get() << packet::VERSION;
    }
    ProtocolLogger logger(serverId, clientId);
    logger.debug() << "initial negotiation succeeded" << protocolName;
}


void shutdownClient(ClientParams &p)
{
    const bool isForce = parseShutdownParam(p.params);
    packet::Packet pkt(p.sock);
    pkt.write(isForce);
    std::string res;
    pkt.read(res);
    if (res != msgAccept) {
        throw cybozu::Exception(__func__) << res;
    }
}


void shutdownServer(ServerParams &p)
{
    bool isForce;
    packet::Packet pkt(p.sock);
    pkt.read(isForce);
    if (isForce) {
        p.ps.setForceShutdown();
    } else {
        p.ps.setGracefulShutdown();
    }
    LOGs.info() << "shutdown" << (isForce ? "force" : "graceful") << p.clientId;
    pkt.writeFin(msgAccept);
}


void sleepClient(ClientParams &p)
{
    const size_t sec = parseSleepParam(p.params);
    packet::Packet pkt(p.sock);
    pkt.write(sec);
    std::string res;
    pkt.read(res);
    if (res != msgOk) {
        throw cybozu::Exception(__func__) << res;
    }
}


std::atomic<size_t> sleepTaskCount_(0);


void sleepServer(ServerParams &p) try
{
    packet::Packet pkt(p.sock);
    size_t sec;
    pkt.read(sec);

    size_t remaining = sec;
    LOGs.info() << "sleep started" << sec;
    for (; remaining > 0; remaining--) {
        if (p.ps.isForceShutdown()) break;
        util::sleepMs(1000);
    }
    const size_t nr = sleepTaskCount_.fetch_add(1) + 1;
    LOGs.info() << "sleep ended" << sec << remaining << nr;

    pkt.writeFin(msgOk);

} catch (std::exception& e) {
    LOGs.info() << "sleep error" << e.what();
}


void versionClient(ClientParams& p)
{
    packet::Packet pkt(p.sock);
    std::string description;
    pkt.read(description);
    ::printf("%s\n", description.c_str());
}


void versionServer(ServerParams& p) try
{
    packet::Packet pkt(p.sock);
    pkt.writeFin(walb::getDescriptionLtsv());
} catch (std::exception& e) {
    LOGs.info() << "version protocol error" << e.what();
}


StrVec prettyPrintHandlerStat(const HandlerStat& stat)
{
    StrVec ret;
    for (const protocol::HandlerStat::CountMap::value_type &pair0 : stat.countMap) {
        const std::string& protocolName = pair0.first;
        for (const protocol::HandlerStat::Imap::value_type &pair1 : pair0.second) {
            const std::string& clientId = pair1.first;
            const uint64_t count = pair1.second;
            ret.push_back(
                cybozu::util::formatString(
                    "PROTOCOL_COUNT\t%s\t%s\t%" PRIu64 "", protocolName.c_str(), clientId.c_str(), count));
        }
    }
    for (const protocol::HandlerStat::LatencyMap::value_type &pair : stat.latencyMap) {
        const std::string& protocolName = pair.first;
        const protocol::HandlerStat::Latency& latency = pair.second;
        double averageLatency = latency.sum / latency.count;
        ret.push_back(
            cybozu::util::formatString(
                "PROTOCOL_LATENCY\t%s\t%.06f", protocolName.c_str(), averageLatency));
    }
    for (const protocol::HandlerStat::GetCountMap::value_type &pair0 : stat.getCountMap) {
        const std::string& targetName = pair0.first;
        for (const protocol::HandlerStat::Imap::value_type &pair1 : pair0.second) {
            const std::string& clientId = pair1.first;
            const uint64_t count = pair1.second;
            ret.push_back(
                cybozu::util::formatString(
                    "GET_COUNT\t%s\t%s\t%" PRIu64 "", targetName.c_str(), clientId.c_str(), count));
        }
    }
    return ret;
}

ServerHandler findServerHandler(
    const Str2ServerHandler &handlers, const std::string &protocolName, const std::string& clientId)
{
    if (protocolName == shutdownCN) return shutdownServer;
    if (protocolName == sleepCN) return sleepServer;
    if (protocolName == versionCN) return versionServer;
    Str2ServerHandler::const_iterator it = handlers.find(protocolName);
    if (it == handlers.cend()) {
        throw cybozu::Exception(__func__) << "bad protocol" << protocolName << clientId;
    }
    return it->second;
}


void RequestWorker::operator()() noexcept
{
// #define DEBUG_HANDLER
#ifdef DEBUG_HANDLER
    static std::atomic<int> ccc;
    LOGs.info() << "SERVER_START" << nodeId << int(ccc++);
#endif
    try {
        std::string clientId, protocolName;
        packet::Packet pkt(sock);
        bool sendErr = true;
        try {
            run1stNegotiateAsServer(sock, nodeId, protocolName, clientId);
            ServerHandler handler = findServerHandler(handlers, protocolName, clientId);
            ServerParams serverParams(sock, clientId, ps);
            pkt.write(msgOk);
            pkt.flush();
            sendErr = false;
#ifdef DEBUG_HANDLER
            LOGs.info() << "SERVER_HANDLE" << nodeId << protocolName;
#endif
            HandlerStatMgr::Transaction tran = handlerStatMgr.start(protocolName, clientId);
            handler(serverParams);
            tran.succeed();
        } catch (std::exception &e) {
            LOGs.error() << e.what();
            if (sendErr) pkt.write(e.what());
        } catch (...) {
            cybozu::Exception e(__func__);
            e << "other error";
            LOGs.error() << e.what();
            if (sendErr) pkt.write(e.what());
        }
    } catch (std::exception &e) {
        LOGs.error() << e.what();
    } catch (...) {
        LOGs.error() << "other error";
    }
    const bool dontThrow = true;
    sock.close(dontThrow);
#ifdef DEBUG_HANDLER
    LOGs.info() << "SERVER_END  " << nodeId << int(ccc--);
#endif
}


void sendStrVec(
    cybozu::Socket &sock,
    const StrVec &v, size_t numToSend, const char *msg, const char *confirmMsg)
{
    if (numToSend != 0 && v.size() != numToSend) {
        throw cybozu::Exception(msg) << "bad size" << numToSend << v.size();
    }
    packet::Packet packet(sock);
    for (size_t i = 0; i < v.size(); i++) {
        if (v[i].empty()) {
            throw cybozu::Exception(msg) << "empty string" << i;
        }
    }
    packet.write(v);
    packet.flush();

    if (confirmMsg) {
        packet::Packet pkt(sock);
        std::string res;
        pkt.read(res);
        if (res != confirmMsg) {
            throw cybozu::Exception(msg) << res;
        }
    }
}


StrVec recvStrVec(cybozu::Socket &sock, size_t numToRecv, const char *msg)
{
    packet::Packet packet(sock);
    StrVec v;
    packet.read(v);
    if (numToRecv != 0 && v.size() != numToRecv) {
        throw cybozu::Exception(msg) << "bad size" << numToRecv << v.size();
    }
    for (size_t i = 0; i < v.size(); i++) {
        if (v[i].empty()) {
            throw cybozu::Exception(msg) << "empty string" << i;
        }
    }
    return v;
}

const GetCommandInfo &getGetCommandInfo(const std::string &name, const GetCommandInfoMap &infoM, const char *msg)
{
    GetCommandInfoMap::const_iterator it = infoM.find(name);
    if (it == infoM.cend()) {
        throw cybozu::Exception(msg) << "name not found" << name;
    }
    return it->second;
}

namespace local {

template <typename T>
inline T recvValue(cybozu::Socket &sock)
{
    packet::Packet pkt(sock);
    T t;
    pkt.read(t);
    packet::Ack(sock).recv();
    return t;
}

} // namespace local


void recvValueAndPut(cybozu::Socket &sock, ValueType valType, const char *msg)
{
    packet::Packet pkt(sock);
    switch (valType) {
    case protocol::SizeType:
        std::cout << local::recvValue<size_t>(sock) << std::endl;
        return;
    case protocol::StringType:
        std::cout << local::recvValue<std::string>(sock) << std::endl;
        return;
    case protocol::StringVecType:
        for (const std::string &s : local::recvValue<StrVec>(sock)) {
            std::cout << s << std::endl;
        }
        return;
    default:
        throw cybozu::Exception(msg) << "bad ValueType" << int(valType);
    }
}


void runGetCommandServer(ServerParams &p, const std::string &nodeId, const GetCommandHandlerMap &hMap,
                         HandlerStatMgr &handlerStatMgr)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec params = recvStrVec(p.sock, 0, FUNC);
        if (params.empty()) throw cybozu::Exception(FUNC) << "no target specified";
        const std::string &targetName = params[0];
        protocol::GetCommandHandlerMap::const_iterator it = hMap.find(targetName);
        if (it == hMap.cend()) throw cybozu::Exception(FUNC) << "no such target" << targetName;
        protocol::GetCommandHandler handler = it->second;
        GetCommandParams cParams{params, pkt, logger, sendErr};
        handlerStatMgr.recordGetCommand(targetName, p.clientId);
        handler(cParams);
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


std::string runGetHostTypeClient(cybozu::Socket &sock, const std::string &nodeId)
{
    run1stNegotiateAsClient(sock, nodeId, getCN);
    sendStrVec(sock, {hostTypeTN}, 1, __func__, msgOk);
    return local::recvValue<std::string>(sock);
}


#ifndef ENABLE_EXEC_PROTOCOL
void runExecServer(ServerParams &p, const std::string &, bool)
{
    const char *const FUNC = __func__;
    packet::Packet pkt(p.sock);

    recvStrVec(p.sock, 0, FUNC);
    pkt.writeFin("exec command is disabled");
}
#else
void runExecServer(ServerParams &p, const std::string &nodeId, bool allowExec)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = recvStrVec(p.sock, 0, FUNC);
        if (!allowExec) {
            pkt.write("exec command is not allowed");
            return;
        }
        const std::string res = cybozu::process::call(v);
        StrVec ret = cybozu::util::splitString(res, "\r\n");
        cybozu::util::removeEmptyItemFromVec(ret);
        sendValueAndFin(pkt, sendErr, ret);
        logger.info() << "exec done" << ret.size() << cybozu::util::concat(v, " ");
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}
#endif


} // namespace protocol
} // namespace walb
