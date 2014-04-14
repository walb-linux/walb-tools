#pragma once
#include "protocol_data.hpp"

namespace walb {

/**
 * Dirty hash-sync.
 *
 * Client: walb-client.
 * Server: walb-server.
 */
namespace dirty_hash_sync {

class SharedData : public walb::protocol::ProtocolData
{
protected:
    std::string name_;
    uint64_t sizeLb_;
    uint16_t bulkLb_;
    uint32_t seed_; /* murmurhash3 seed */
    MetaSnap snap_;
public:
    using ProtocolData::ProtocolData;
    void verifyParams() const {
        if (name_.empty()) logAndThrow("name param empty.");
        if (sizeLb_ == 0) logAndThrow("sizeLb param is zero.");
        if (bulkLb_ == 0) logAndThrow("bulkLb param is zero.");
    }
};

class ClientRunner : public SharedData
{
private:
    std::string path_;
public:
    using SharedData::SharedData;
    void run() {
        loadParams();
        negotiate();
        readAndSendDiffData();
        sendMetaDiff();
    }
private:
    /**
     * path
     * name
     * lv size [bytes]
     * bulk size [bytes]
     * seed
     */
    void loadParams() {
        if (params_.size() != 5) logAndThrow("Five parameters required.");
        path_ = params_[0];
        name_ = params_[1];
        uint64_t size = cybozu::util::fromUnitIntString(params_[2]);
        sizeLb_ = size / LOGICAL_BLOCK_SIZE;
        uint64_t bulk = cybozu::util::fromUnitIntString(params_[3]);
        if ((1U << 16) * LOGICAL_BLOCK_SIZE <= bulk) {
            logAndThrow("bulk size too large. < %u\n", (1U << 16) * LOGICAL_BLOCK_SIZE);
        }
        bulkLb_ = bulk / LOGICAL_BLOCK_SIZE;
        seed_ = cybozu::atoi(params_[4]);
    }
    void negotiate() {
        packet::Packet packet(sock_);
        packet.write(name_);
        packet.write(sizeLb_);
        packet.write(bulkLb_);
        packet.write(seed_);
        packet::Answer ans(sock_);
        int err; std::string msg;
        if (!ans.recv(&err, &msg)) {
            logAndThrow("negotiation failed: %d %s", err, msg.c_str());
        }
        packet.read(snap_);
    }
    /**
     * TODO: send diffs as snappied wdiff format.
     */
    void readAndSendDiffData() {
        packet::Packet packet(sock_);
        cybozu::thread::BoundedQueue<cybozu::murmurhash3::Hash> hashQ(32); /* TODO: do not hardcode. */
        std::vector<char> buf(bulkLb_ * LOGICAL_BLOCK_SIZE);
        cybozu::util::BlockDevice bd(path_, O_RDONLY);
        cybozu::murmurhash3::Hasher hasher(seed_);
        ConverterQueue convQ(4, 2, true, ::WALB_DIFF_CMPR_SNAPPY); /* TODO: do not hardcode. */
        diff::Packer packer;
        packer.setMaxPackSize(2U << 20); /* 2MiB. TODO: do not hardcode. */
        packer.setMaxNumRecords(10); /* TODO: do not hardcode. */
        std::atomic<bool> isError(false);

        /* Read hash data */
        auto receiveHash = [this, &packet, &hashQ](uint64_t num) {
            try {
                for (uint64_t i = 0; i < num; i++) {
                    cybozu::murmurhash3::Hash h;
                    packet.read(h.rawData(), h.rawSize());
                    hashQ.push(h);
                }
                hashQ.sync();
            } catch (...) {
                logger_.error("receiveHash failed.");
                hashQ.fail();
            }
            logger_.info("receiveHash end");
        };

        size_t nPack = 0;
        auto pushPackToQueue = [this, &nPack, &packer, &convQ]() {
            std::unique_ptr<char[]> up = packer.getPackAsUniquePtr();
            diff::MemoryPack mpack(up.get());
            logger_.info("try to push pack %zu %p %zu", nPack, up.get(), mpack.size());
            if (!convQ.push(up)) {
                throw std::runtime_error("convQ push failed.");
            }
            logger_.info("push pack %zu", nPack++);
            assert(packer.empty());
        };

        /* Send updated bulk. */
        auto addToPack = [this, &hashQ, &packer, &buf, &bd, &hasher, &convQ, &pushPackToQueue](
            uint64_t &offLb, uint64_t &remainingLb) {

            uint16_t lb = std::min<uint64_t>(bulkLb_, remainingLb);
            size_t size = lb * LOGICAL_BLOCK_SIZE;
            bd.read(&buf[0], size);
            cybozu::murmurhash3::Hash h0, h1;
            h0 = hasher(&buf[0], size);
            h1 = hashQ.pop();
            if (h0 != h1) {
                //logger_.info("Hash differ %s %s", h0.str().c_str(), h1.str().c_str());
                if (!packer.canAddLb(lb)) pushPackToQueue();
                UNUSED bool r = packer.add(offLb, lb, &buf[0]);
                assert(r);
                //packer.print(); /* debug */
                assert(packer.isValid(false)); /* debug */
            } else {
                //logger_.info("Hash same %s %s", h0.str().c_str(), h1.str().c_str());
            }
            offLb += lb;
            remainingLb -= lb;
        };

        /* Read block device and create pack. */
        auto createPack = [this, &isError, &hashQ, &addToPack, &packer, &convQ, &pushPackToQueue](
            UNUSED uint64_t num) {

            try {
                uint64_t offLb = 0;
                uint64_t remainingLb = sizeLb_;
                uint64_t c = 0;
                while (0 < remainingLb) {
                    addToPack(offLb, remainingLb);
                    c++;
                }
                if (!packer.empty()) pushPackToQueue();
                assert(num == c);
                assert(offLb == sizeLb_);
            } catch (...) {
                logger_.error("createPack failed.");
                isError = true;
                hashQ.fail();
            }
            convQ.quit();
            convQ.join();
            logger_.info("createPack end");
        };

        /* Send compressed pack. */
        auto sendPack = [this, &packet, &convQ, &isError]() {
            try {
                packet::StreamControl ctrl(packet.sock());
                size_t c = 0;
                logger_.info("try to pop from convQ"); /* debug */
                std::unique_ptr<char[]> up = convQ.pop();
                logger_.info("popped from convQ: %p", up.get()); /* debug */
                while (up) {
                    logger_.info("try to send pack %zu", c);
                    ctrl.next();
                    diff::MemoryPack mpack(up.get());
                    uint32_t packSize = mpack.size();
                    packet.write(packSize);
                    packet.write(mpack.rawPtr(), packSize);
                    logger_.info("send pack %zu", c++);
                    up = convQ.pop();
                }
                if (isError) ctrl.error(); else ctrl.end();
                logger_.info("sendPack: end (%d)", isError.load());
            } catch (std::exception &e) {
                logger_.error("sendPack failed: %s", e.what());
                convQ.quit();
                while (convQ.pop());
                isError = true;
            } catch (...) {
                logger_.error("sendPack failed.");
                convQ.quit();
                while (convQ.pop());
                isError = true;
            }
            assert(convQ.pop() == nullptr);
            logger_.info("sendPack end");
        };

        /* Invoke threads. */
        uint64_t num = (sizeLb_ - 1) / bulkLb_ + 1;
        std::thread th0(receiveHash, num);
        std::thread th1(createPack, num);
        std::thread th2(sendPack);
        th0.join();
        th1.join();
        th2.join();

        if (isError) logAndThrow("readAndSendDiffData failed.");
    }
    void sendMetaDiff() {
        packet::Packet packet(sock_);
        MetaDiff diff(snap_.gid0(), snap_.gid1(), snap_.gid1() + 1, snap_.gid1() + 2);
        diff.setTimestamp(::time(0));
        packet.write(diff);
    }
};

class ServerRunner : public SharedData
{
private:
    cybozu::FilePath baseDir_;
public:
    using SharedData::SharedData;
    void run() {
        loadParams();
        negotiate();
        logger_.info("dirty-hash-sync %s %" PRIu64 " %u %u (%" PRIu64 " %" PRIu64")"
                     , name_.c_str(), sizeLb_, bulkLb_, seed_, snap_.gid0(), snap_.gid1());
        recvAndWriteDiffData();
    }
private:
    void loadParams() {
        if (params_.size() != 1) logAndThrow("One parameter required.");
        baseDir_ = cybozu::FilePath(params_[0]);
        if (!baseDir_.stat().isDirectory()) {
            logAndThrow("Base directory %s does not exist.", baseDir_.cStr());
        }
    }
    void negotiate() {
        packet::Packet packet(sock_);
        packet.read(name_);
        packet.read(sizeLb_);
        packet.read(bulkLb_);
        packet.read(seed_);
        packet::Answer ans(sock_);
        ServerData sd(baseDir_.str(), name_);
        if (!sd.lvExists()) {
            std::string msg = cybozu::util::formatString(
                "Lv does not exist for %s. Try full-sync."
                , name_.c_str());
            ans.ng(1, msg);
            logAndThrow(msg.c_str());
        }
        ans.ok();
        MetaSnap snap = sd.getLatestSnapshot();
        packet.write(snap);
    }
    void recvAndWriteDiffData() {
        packet::Packet packet(sock_);
        ServerData sd(baseDir_.str(), name_);
        std::string lvPath = sd.getLv().path().str();
        cybozu::util::BlockDevice bd(lvPath, O_RDWR);
        cybozu::murmurhash3::Hasher hasher(seed_);
        std::atomic<bool> isError(false);

        logger_.info("hoge1");

        /* Prepare virtual full lv */
        std::vector<MetaDiff> diffs = sd.diffsToApply(snap_.gid0());
        std::vector<std::string> diffPaths;
        for (MetaDiff &diff : diffs) {
            cybozu::FilePath fp = sd.getDiffPath(diff);
            diffPaths.push_back(fp.str());
        }
        diff::VirtualFullScanner virt(bd.getFd());
        virt.setWdiffPaths(diffPaths);
        std::vector<char> buf0(bulkLb_ * LOGICAL_BLOCK_SIZE);

        logger_.info("hoge2");

        auto readBulkAndSendHash = [this, &virt, &packet, &buf0, &hasher](
            uint64_t &offLb, uint64_t &remainingLb) {

            uint16_t lb = std::min<uint64_t>(bulkLb_, remainingLb);
            size_t size = lb * LOGICAL_BLOCK_SIZE;
            virt.read(&buf0[0], size);
            cybozu::murmurhash3::Hash h0 = hasher(&buf0[0], size);
            packet.write(h0.rawData(), h0.rawSize());
            offLb += lb;
            remainingLb -= lb;
        };

        auto hashSender = [this, &readBulkAndSendHash, &isError]() {
            try {
                uint64_t offLb = 0;
                uint64_t remainingLb = sizeLb_;
                uint64_t c = 0;
                while (0 < remainingLb) {
#if 0
                    logger_.info("c %" PRIu64 " offLb %" PRIu64 "", c, offLb); /* debug */
#endif
                    readBulkAndSendHash(offLb, remainingLb);
                    c++;
                }
                logger_.info("remainingLb 0"); /* debug */
                assert(offLb = sizeLb_);
            } catch (std::exception &e) {
                logger_.error("hashSender error: %s", e.what());
                isError = true;
            } catch (...) {
                logger_.error("bulkReceiver unknown error");
                isError = true;
            }
            logger_.info("hashSender end");
        };

        DiffFileHeader wdiffH;
        wdiffH.init();
        wdiffH.setMaxIoBlocksIfNecessary(bulkLb_);

        cybozu::TmpFile tmpFile(sd.getDiffDir().str());
        diff::Writer writer(tmpFile.fd());
        writer.writeHeader(wdiffH);
        cybozu::util::FdWriter fdw(tmpFile.fd());

        logger_.info("hoge3");

        auto bulkReceiver = [this, &packet, &writer, &fdw, &isError]() {
            try {
                std::vector<char> buf;
                packet::StreamControl ctrl(sock_);
                size_t c = 0;
                while (ctrl.isNext()) {
                    uint32_t packSize;
                    packet.read(packSize);
                    buf.resize(packSize);
                    packet.read(&buf[0], packSize);
                    logger_.info("received pack %zu", c++);
                    diff::MemoryPack mpack(&buf[0]);
                    if (mpack.size() != packSize) {
                        throw std::runtime_error("pack size invalid.");
                    }
                    if (!mpack.isValid()) {
                        throw std::runtime_error("received pack invalid.");
                    }
                    fdw.write(mpack.rawPtr(), packSize);
                    ctrl.reset();
                }
                if (ctrl.isError()) {
                    logger_.error("CtrlMsg error");
                    isError = true;
                } else {
                    logger_.info("CtrlMsg end");
                }
                logger_.info("bulkRecriver end.");
            } catch (std::exception &e) {
                logger_.error("bulkReceiver error: %s", e.what());
                isError = true;
            } catch (...) {
                logger_.error("bulkReceiver unknown error");
                isError = true;
            }
            logger_.info("bulkReceiver end");
        };

        std::thread th0(hashSender);
        logger_.info("hoge4");
        std::thread th1(bulkReceiver);
        logger_.info("hoge5");
        th0.join();
        th1.join();
        logger_.info("joined: %d", isError.load());

        std::this_thread::sleep_for(std::chrono::milliseconds(500)); /* debug */
        if (isError) logAndThrow("recvAndWriteDiffData failed.");

        writer.close();
        MetaDiff diff;
        packet.read(diff); /* get diff information from the client. */
        cybozu::FilePath fp = sd.getDiffPath(diff);
        assert(fp.isFull());
        tmpFile.save(fp.str());
        sd.add(diff);

        logger_.info("dirty-hash-sync %s done.", name_.c_str());
    }
};

} // namespace dirty_hash_sync

/**
 * @params using cybozu::loadFromStr() to convert.
 *   [0] :: string: full path of lv.
 *   [1] :: string: lv identifier.
 *   [2] :: uint64_t: lv size [byte].
 *   [3] :: uint32_t: bulk size [byte]. less than uint16_t * LOGICAL_BLOCK_SIZE;
 */
inline void clientDirtyHashSync(
    cybozu::Socket &sock, ProtocolLogger &logger,
    const std::atomic<bool> &forceQuit,
    const std::vector<std::string> &params)
{
    dirty_hash_sync::ClientRunner c("dirty-hash-sync", sock, logger, forceQuit, params);
    c.run();
}

/**
 */
inline void serverDirtyHashSync(
    cybozu::Socket &sock, ProtocolLogger &logger,
    const std::string &baseDirStr,
    const std::atomic<bool> &forceQuit,
    std::atomic<walb::server::ProcessStatus> &/*procStat*/)
{
    dirty_hash_sync::ServerRunner s("dirty-hash-sync", sock, logger, forceQuit, { baseDirStr });
    s.run();
}

} // namespace walb
