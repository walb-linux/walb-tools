#include "protocol.hpp"

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


ServerHandler findServerHandler(
    const Str2ServerHandler &handlers, const std::string &protocolName)
{
    if (protocolName == shutdownCN) {
        return shutdownServer;
    }
    Str2ServerHandler::const_iterator it = handlers.find(protocolName);
    if (it == handlers.cend()) {
        throw cybozu::Exception(__func__) << "bad protocol" << protocolName;
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
                ServerHandler handler = findServerHandler(handlers, protocolName);
                ServerParams serverParams(sock, clientId, ps);
                pkt.write(msgOk);
                pkt.flush();
                sendErr = false;
#ifdef DEBUG_HANDLER
                LOGs.info() << "SERVER_HANDLE" << nodeId << protocolName;
#endif
                handler(serverParams);
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


void runGetCommandServer(ServerParams &p, const std::string &nodeId, const GetCommandHandlerMap &hMap)
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
