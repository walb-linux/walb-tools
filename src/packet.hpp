#pragma once
/**
 * @file
 * @brief Packet data for protocol.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/socket.hpp"
#include "cybozu/serializer.hpp"
#include "util.hpp"

// #define PACKET_DEBUG

namespace walb {
namespace packet {

const uint32_t VERSION = 1;
const uint32_t ACK_MSG = 0x626c6177; /* "walb" (little endian). */

const size_t MAX_SIZE_PER_WRITE = 64 * 1024; /* 64KiB */


/**
 * Try to send written data to a socket immediately.
 */
inline void flushSocket(cybozu::Socket &sock, bool isCork = false)
{
    sock.setSocketOption(TCP_NODELAY, 1, IPPROTO_TCP);
    if (!isCork) {
        sock.setSocketOption(TCP_NODELAY, 0, IPPROTO_TCP);
    }
}

/**
 * Base class for client/server communication.
 *
 * (1) send byte array.
 * (2) send objects using serializer.
 */
class Packet
{
private:
    cybozu::Socket &sock_;

public:
    explicit Packet(cybozu::Socket &sock) : sock_(sock) {}
    virtual ~Packet() noexcept = default;

    const cybozu::Socket &sock() const { return sock_; }
    cybozu::Socket &sock() { return sock_; }

    /**
     * Byte-array read/write.
     */
    size_t readSome(void *data, size_t size) { return sock_.readSome(data, size); }
    void read(void *data, size_t size) { sock_.read(data, size); }
    void write(const void *data, size_t size) { sock_.write(data, size, MAX_SIZE_PER_WRITE); }

    /**
     * Serializer.
     */
    template <typename T>
    void read(T &t) { cybozu::load(t, sock_); }
    template <typename T>
    void write(const T &t) { cybozu::save(sock_, t); }

    template <typename T>
    void writeFin(const T &t) {
        write(t);
        sock_.waitForClose();
        sock_.close();
    }
    void flush() { flushSocket(sock_); }

#ifdef PACKET_DEBUG
    void sendDebugMsg(const std::string &msg) {
        write(msg);
        ::printf("SEND %s\n", msg.c_str());
    }
    void recvDebugMsg(const std::string &msg) {
        std::string s;
        read(s);
        ::printf("RECV %s\n", s.c_str());
        if (s != msg) {
            throw RT_ERR("invalid packet: expected '%s' received '%s'."
                         , msg.c_str(), s.c_str());
        }
    }
#else
    void sendDebugMsg(const std::string &) {}
    void recvDebugMsg(const std::string &) {}
#endif /* PACKET_DEBUG */
};

class Ack : public Packet
{
public:
    using Packet :: Packet;
    void send() {
        sendDebugMsg("ACK");
        write(ACK_MSG);
    }
    void sendFin() {
        sendDebugMsg("ACK");
        writeFin(ACK_MSG);
    }
    void recv() {
        recvDebugMsg("ACK");
        uint32_t ack = 0;
        read(ack);
        if (ack != ACK_MSG) {
            throw std::runtime_error("could not receive an acknowledgement message.");
        }
    }
};

class Version : public Packet
{
private:
    uint32_t version_;

public:
    using Packet :: Packet;
    Version(cybozu::Socket &sock) : Packet(sock), version_(UINT32_MAX) {}
    void send() {
        sendDebugMsg("VERSION");
        write(VERSION);
    }
    bool recv() {
        recvDebugMsg("VERSION");
        read(version_);
#if 0
        if (version_ != VERSION) {
            throw RT_ERR("Version number differ: required: %" PRIu32 " received %" PRIu32 "."
                         , VERSION, version_);
        }
#endif
        return version_ == VERSION;
    }
    uint32_t get() const { return version_; }
};

class StreamControl : public Packet
{
private:
    bool received_;
    enum class Msg : uint8_t {
        Next = 0, End = 1, Error = 2, Dummy = 3,
    };
    Msg msg_;

public:
    explicit StreamControl(cybozu::Socket &sock)
        : Packet(sock), received_(false), msg_(Msg::Next) {}
    /**
     * For sender.
     */
    void next() { write(toInt(Msg::Next)); }
    void end() { write(toInt(Msg::End)); }
    void error() { write(toInt(Msg::Error)); }
    void dummy() { write(toInt(Msg::Dummy)); }

    /**
     * For receiver.
     */
    bool isNext() { return getAndEquals(Msg::Next); }
    bool isEnd() { return getAndEquals(Msg::End); }
    bool isError() { return getAndEquals(Msg::Error); }
    bool isDummy() { return getAndEquals(Msg::Dummy); }
    void reset() { received_ = false; }
private:
    static Msg fromInt(uint8_t v) {
        return static_cast<Msg>(v);
    }
    static uint8_t toInt(const Msg &msg) {
        return static_cast<uint8_t>(msg);
    }
    bool getAndEquals(const Msg &msg) {
        if (!received_) {
            uint8_t v;
            read(v);
            msg_ = fromInt(v);
            received_ = true;
        }
        return msg_ == msg;
    }
};

class StreamControl2
{
private:
    enum class Msg : uint8_t {
        Next = 0, End = 1, Error = 2, Dummy = 3,
    };
    Packet pkt_;
    Msg msg_;

public:
    explicit StreamControl2(cybozu::Socket &sock)
        : pkt_(sock), msg_(Msg::Error) {}

    /* Send */
    void sendNext() { pkt_.write(uint8_t(Msg::Next));}
    void sendEnd() { pkt_.write(uint8_t(Msg::End)); }
    void sendError() { pkt_.write(uint8_t(Msg::Error)); }
    void sendDummy() { pkt_.write(uint8_t(Msg::Dummy)); }

    /* Receive and check */
    void recv() { uint8_t u; pkt_.read(u); msg_ = static_cast<Msg>(u); }
    bool isNext() const { return msg_ == Msg::Next; }
    bool isEnd() const { return msg_ == Msg::End; }
    bool isError() const { return msg_ == Msg::Error; }
    bool isDummy() const { return msg_ == Msg::Dummy; }
    const char *toStr() const {
        if (msg_ == Msg::Next) return "Next";
        else if (msg_ == Msg::End) return "End";
        else if (msg_ == Msg::Error) return "Error";
        else if (msg_ == Msg::Dummy) return "Dummy";
        else {
            throw cybozu::Exception("StreamControl2:bad message")
                << uint8_t(msg_);
        }
    }
};

}} //namespace walb::packet
