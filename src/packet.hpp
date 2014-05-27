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
    void write(const void *data, size_t size) { sock_.write(data, size); }

    /**
     * Serializer.
     */
    template <typename T>
    void read(T &t) { cybozu::load(t, sock_); }
    template <typename T>
    void write(const T &t) { cybozu::save(sock_, t); }

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
    void sendDebugMsg(UNUSED const std::string &msg) {}
    void recvDebugMsg(UNUSED const std::string &msg) {}
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
    Version(cybozu::Socket &sock) : Packet(sock) {}
    void send() {
        sendDebugMsg("VERSION");
        write(VERSION);
    }
    bool recv() {
        recvDebugMsg("VERSION");
        version_ = 0;
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
        Next = 0, End = 1, Error = 2,
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

    /**
     * For receiver.
     */
    bool isNext() { return getAndEquals(Msg::Next); }
    bool isEnd() { return getAndEquals(Msg::End); }
    bool isError() { return getAndEquals(Msg::Error); }
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

}} //namespace walb::packet
