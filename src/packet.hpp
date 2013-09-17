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
    size_t readSome(void *data, size_t size) { return sock().readSome(data, size); }
    void read(void *data, size_t size) { sock().read(data, size); }
    void write(const void *data, size_t size) { sock().write(data, size); }

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
public:
    Version(cybozu::Socket &sock) : Packet(sock) {}
    void send() {
        sendDebugMsg("VERSION");
        write(VERSION);
    }
    bool recv() {
        recvDebugMsg("VERSION");
        uint32_t version = 0;
        read(version);
#if 0
        if (version != VERSION) {
            throw RT_ERR("Version number differ: required: %" PRIu32 " received %" PRIu32 "."
                         , VERSION, version);
        }
#endif
        return version == VERSION;
    }
};

class Answer : public Packet
{
public:
    Answer(cybozu::Socket &sock) : Packet(sock) {}
    void ok() { send(true, 0, ""); }
    void ng(int err, const std::string &msg) { send(false, err, msg); }
    void send(bool b, int err, const std::string &msg) {
        sendDebugMsg("ANSWER");
        write(b);
        write(err);
        write(msg);
    }
    bool recv(int *errP = nullptr, std::string *msgP = nullptr) {
        recvDebugMsg("ANSWER");
        bool b;
        read(b);
        int err;
        read(err);
        if (errP) *errP = err;
        std::string msg;
        read(msg);
        if (msgP) *msgP = std::move(msg);
        return b;
    }
};

class MaybeCancel : public Answer
{
public:
    void goAhead() { ok(); }
    void cancel() { ng(0, ""); }
    bool isCanceled() { return !recv(); }
};

}} //namespace walb::packet
