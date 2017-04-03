/**
 * @file
 * @brief wdiff sender which talks wdiff-transfer client protocol.
 */
#include "cybozu/option.hpp"
#include "walb_util.hpp"
#include "file_path.hpp"
#include "meta.hpp"
#include "walb_diff_file.hpp"
#include "walb_diff_merge.hpp"
#include "wdiff_transfer.hpp"
#include "protocol.hpp"

using namespace walb;

struct Option
{
private:
    uint64_t gid0;
    uint64_t gid1;
    int isMergeable;
    std::string timeStr;
    std::string uuidStr;
public:
    std::string responseMsg;
    std::string volId;
    uint64_t size;
    std::string wdiffPath;

    size_t timeoutSec;
    std::string nodeId;
    bool isDebug;

    std::string addr;
    uint16_t port;

    MetaDiff diff;
    DiffFileHeader fileH;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setDescription("This command connects to an archive server and try to send a wdiff file\n"
                           "using wdiff-transfer protocol.\n");
        opt.appendOpt(&gid0, -1, "g0", ": begin gid (overwrite the diff metadata)");
        opt.appendOpt(&gid1, -1, "g1", ": end gid (overwrite the diff metadata)");
        opt.appendOpt(&isMergeable, -1, "merge", ": isMergeable. 0 is false, 1 is true (overwrite the diff metadata)");
        opt.appendOpt(&timeStr, "", "time", ": time string (overwrite the diff metadata)");
        opt.appendOpt(&uuidStr, "", "uuid", ": uuid string (overwrite the diff metadata)");
        opt.appendOpt(&responseMsg, msgAccept, "msg", ": response message to validate");
        opt.appendOpt(&timeoutSec, 10, "timeout", ": socket timeout [sec]");
        opt.appendOpt(&nodeId, "wdiff-send", "node", ": node identifier");

        opt.appendParam(&addr, "ADDRESS", ": address or name of an archive server");
        opt.appendParam(&port, "PORT", ": port number of an archive server");

        opt.appendParam(&volId, "VOL", ": volume identifier");
        opt.appendParam(&size, "SIZE", ": disk size [byte]. You can specify a size suffix K/M/G/T");
        opt.appendParam(&wdiffPath, "WDIFF_PATH", ": wdiff file path");

        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr");
        opt.appendHelp("h");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        setupDiff();
        LOGs.debug() << "metaDiff" << diff;

        setupFileHeader();
        LOGs.debug() << "diffFileH" << fileH;
    }
private:
    bool isAllDiffFileNameParamsSpecified() const {
        if (gid0 == uint64_t(-1)) return false;
        if (gid1 == uint64_t(-1)) return false;
        if (isMergeable < 0) return false;
        if (timeStr.empty()) return false;
        return true;
    }
    void setupDiff() {
        if (!isAllDiffFileNameParamsSpecified()) {
            cybozu::FilePath fp(wdiffPath);
            if (!fp.stat().isFile()) {
                throw cybozu::Exception(__func__)
                    << "file not found" << wdiffPath;
            }
            diff = parseDiffFileName(fp.baseName());
        }
        if (gid0 != uint64_t(-1)) diff.snapB.set(gid0);
        if (gid1 != uint64_t(-1)) diff.snapE.set(gid1);
        if (isMergeable >= 0) diff.isMergeable = (isMergeable != 0);
        if (!timeStr.empty()) diff.timestamp = cybozu::strToUnixTime(timeStr);
        diff.verify();
    }
    void setupFileHeader() {
        cybozu::util::File file;
        file.open(wdiffPath, O_RDONLY);
        fileH.readFrom(file);
        if (!uuidStr.empty()) {
            cybozu::Uuid uuid;
            uuid.set(uuidStr);
            fileH.setUuid(uuid);
        }
    }
};

void runDummyProxy(const Option &opt)
{
    // connect.
    cybozu::Socket sock;
    util::connectWithTimeout(sock, cybozu::SocketAddr(opt.addr, opt.port), opt.timeoutSec);

    // 1st negotiation.
    const std::string serverId = protocol::run1stNegotiateAsClient(sock, opt.nodeId, wdiffTransferPN);
    packet::Packet pkt(sock);
    ProtocolLogger logger(opt.nodeId, serverId);

    // wdiff-transfer negotiation
    const DiffFileHeader &fileH = opt.fileH;
    pkt.write(opt.volId);
    pkt.write(proxyHT);
    pkt.write(fileH.getUuid());
    pkt.write(fileH.getMaxIoBlocks());
    const uint64_t sizeLb = opt.size >> 9;
    pkt.write(sizeLb);
    pkt.write(opt.diff);
    logger.debug() << "send" << opt.volId << proxyHT << fileH.getUuid()
                   << fileH.getMaxIoBlocks() << sizeLb << opt.diff;
    std::string res;
    pkt.read(res);
    if (res != opt.responseMsg) {
        throw cybozu::Exception(__func__) << "bad response" << res << opt.responseMsg;
    }

    // transfer diff data if necessary.
    if (res != msgAccept) return;
    DiffMerger merger;
    merger.addWdiffs({opt.wdiffPath});
    merger.prepare();
    const CompressOpt cmpr;
    std::atomic<int> stopState(NotStopping);
    ProcessStatus ps;
    DiffStatistics statOut;
    if (!wdiffTransferClient(pkt, merger, cmpr, stopState, ps, statOut)) {
        throw cybozu::Exception(__func__) << "wdiffTransferClient failed";
    }
    packet::Ack(sock).recv();
}

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    runDummyProxy(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-send")
