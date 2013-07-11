/**
 * @file
 * @brief Virtual full image scanner.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <vector>
#include <string>
#include <memory>
#include "cybozu/option.hpp"
#include "walb_diff.hpp"
#include "walb_diff_merge.hpp"
#include "fileio.hpp"

class VirtualFullScanner
{
private:
    cybozu::util::FdReader reader_;
    walb::diff::WalbDiffMerger merger_;
    uint64_t addr_; /* Indicator of previous read amount [logical block]. */
    walb::diff::DiffRecIo recIo_; /* current diff rec IO. */
    uint16_t offInIo_; /* offset in the IO [logical block]. */
    bool isEndDiff_; /* true if there is no more wdiff IO. */

public:
    VirtualFullScanner(int inputFd, const std::vector<std::string> &wdiffPaths)
        : reader_(inputFd)
        , merger_()
        , addr_(0)
        , recIo_()
        , offInIo_(0)
        , isEndDiff_(false) {
        merger_.addWdiffs(wdiffPaths);
        merger_.prepare();
    }
    /**
     * Write all data to a specified fd.
     */
    void readAndWriteTo(int outputFd, size_t bufSize = 4096) {
        cybozu::util::FdWriter writer(outputFd);
        std::vector<char> buf(bufSize);
        size_t rSize;
        while (0 < (rSize = read(&buf[0], bufSize))) {
            writer.write(&buf[0], rSize);
        }
        writer.fdatasync();
    }
    /**
     * Read a specified bytes.
     * @data buffer to be filled.
     * @size size trying to read [byte].
     *   This must be multiples of LOGICAL_BLOCK_SIZE.
     *
     * RETURN:
     *   Read size really [byte].
     *   0 means that the input reached the end.
     */
    size_t read(char *data, size_t size) {
        assert(size % LOGICAL_BLOCK_SIZE == 0);
        /* Read up to 65535 blocks at once. */
        uint16_t blks = uint16_t(-1);
        if (size / LOGICAL_BLOCK_SIZE < blks) {
            blks = size / LOGICAL_BLOCK_SIZE;
        }

        fillDiffIo();
        if (isEndDiff_) {
            /* There is no remaining diff IOs. */
            return readBase(data, size);
        }

        uint64_t diffAddr = currentDiffAddr();
        assert(addr_ <= diffAddr);
        if (addr_ == diffAddr) {
            /* Read wdiff IO partially. */
            uint16_t blks0 = std::min(blks, currentDiffBlocks());
            readWdiff(data, blks0);
            return blks0;
        }
        /* Read the base image. */
        uint16_t blks0 = blks;
        uint64_t blksToIo = diffAddr - addr_;
        if (blksToIo < blks) {
            blks0 = uint16_t(blksToIo);
        }
        return readBase(data, blks0);
    }
private:
    /**
     * Read from the base full image.
     * @data buffer.
     * @blks [logical block].
     * RETURN:
     *   really read size [byte].
     */
    size_t readBase(char *data, size_t blks) {
        size_t size = blks * LOGICAL_BLOCK_SIZE;
        size_t rSize = 0;
        while (rSize < size) {
            try {
                reader_.read(data + rSize, LOGICAL_BLOCK_SIZE);
            } catch (cybozu::util::EofError &e) {
                break;
            }
            rSize += LOGICAL_BLOCK_SIZE;
            addr_++;
        }
        return rSize;
    }
    /**
     * Read from the current diff IO.
     * @data buffer.
     * @blks [logical block]. This must be <= remaining size.
     */
    void readWdiff(char *data, size_t blks) {
        assert(recIo_.isValid());
        const walb::diff::BlockDiffIo &io = recIo_.io();
        assert(offInIo_ < io.ioBlocks());
        assert(!io.isCompressed());
        size_t off = offInIo_ * LOGICAL_BLOCK_SIZE;
        ::memcpy(data, io.rawData() + off, blks * LOGICAL_BLOCK_SIZE);
        offInIo_ += blks;
        assert(offInIo_ <= io.ioBlocks());

        /* Skip to read the base image. */
        for (size_t i = 0; i < blks; i++) {
            char buf[LOGICAL_BLOCK_SIZE];
            reader_.read(buf, LOGICAL_BLOCK_SIZE);
        }

        addr_ += blks;
    }
    /**
     * Set recIo_ approximately.
     */
    void fillDiffIo() {
        if (isEndDiff_) return;
        /* At beginning time, recIo_.io().ioBlocks() returns 0. */
        assert(offInIo_ <= recIo_.io().ioBlocks());
        if (offInIo_ == recIo_.io().ioBlocks()) {
            offInIo_ = 0;
            if (!merger_.pop(recIo_)) {
                isEndDiff_ = true;
                recIo_ = walb::diff::DiffRecIo();
            }
        }
    }
    uint64_t currentDiffAddr() const {
        return recIo_.record().ioAddress() + offInIo_;
    }
    uint16_t currentDiffBlocks() const {
        assert(offInIo_ <= recIo_.record().ioBlocks());
        return recIo_.record().ioBlocks() - offInIo_;
    }
};

struct Option : public cybozu::Option
{
    std::string inputPath;
    std::string outputPath;
    std::vector<std::string> inputWdiffs;
    uint32_t blockSize;
    Option() {
        setDescription("\nvirt-full-cat:\n"
                       "  Full scan of virtul full image that consists\n"
                       "  a base full image and additional wdiff files.\n");
        setUsage("Usage: virt-full-cat (options) -i [input image] -d [input wdiffs] -o [output image]");
        appendOpt(&inputPath, "-", "i", "Input full image path. '-' means stdin. (default '-')");
        appendOpt(&outputPath, "-", "o", "Output full image path. '-' means stdout. (default '-')");
        appendVec(&inputWdiffs, "d", "Input wdiff paths");
        appendOpt(&blockSize, 64U << 10, "b", "Block size [byte].");
        appendHelp("h");
    }
    bool parse(int argc, char *argv[]) {
        if (!cybozu::Option::parse(argc, argv)) {
            goto error;
        }
        return true;

        /* check options. */
      error:
        usage();
        return false;
    }
};

int main(int argc, char *argv[])
{
    try {
#if 0
        for (int i = 0; i < argc; i++) {
            ::printf("%s\n", argv[i]);
        }
#endif
        Option opt;
        if (!opt.parse(argc, argv)) return 1;

        /* File descriptors for input/output. */
        int inFd = 0, outFd = 1;
        std::shared_ptr<cybozu::util::FileOpener> inFo, outFo;
        if (opt.inputPath != "-") {
            inFo = std::make_shared<cybozu::util::FileOpener>(
                opt.inputPath, O_RDONLY);
            inFd = inFo->fd();
        }
        if (opt.outputPath != "-") {
            outFo = std::make_shared<cybozu::util::FileOpener>(
                opt.outputPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            outFd = outFo->fd();
        }
        VirtualFullScanner scanner(inFd, opt.inputWdiffs);
        scanner.readAndWriteTo(outFd);
        if (outFo) outFo->close();

#if 0
        opt.put();

        for (std::string &s : opt.inputWdiffs) {
            ::printf("%s\n", s.c_str());
        }
        ::printf("%u %s\n%s\n"
                 , opt.blockSize
                 , opt.inputPath.c_str()
                 , opt.outputPath.c_str());
#endif

        return 0;
    } catch (std::runtime_error &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (...) {
        ::fprintf(::stderr, "caught other error.\n");
        return 1;
    }
    return 0;
}

/* end of file. */
