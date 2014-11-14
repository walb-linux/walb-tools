#include "compressor.hpp"
#include <cybozu/test.hpp>
#include <cybozu/xorshift.hpp>
#include "walb_diff_compressor.hpp"
#include "walb_types.hpp"

// TODO:
// using namespace walb;

using Buffer = walb::compressor::Buffer;

void test(int mode)
{
    const std::string in = "aaaabbbbccccddddeeeeffffgggghhhhiiiijjjjjaaaaaaaaaaaaabbbcccxxxxxxxxxxxxxxxxxsssssssssssssssssssssssssssssssss";
    walb::Compressor c(mode);
    std::string enc;
    enc.resize(in.size() * 2);
    size_t encSize;
    bool ret = c.run(&enc[0], &encSize, enc.size(), in.data(), in.size());
    CYBOZU_TEST_ASSERT(ret);
    printf("inSize=%d, encSize=%d\n", (int)in.size(), (int)encSize);
    std::string dec;
    walb::Uncompressor d(mode);
    dec.resize(in.size() + 10);
    size_t decSize = d.run(&dec[0], dec.size(), &enc[0], encSize);
    CYBOZU_TEST_EQUAL(decSize, in.size());
    dec.resize(decSize);
    CYBOZU_TEST_EQUAL(dec, in);
}

CYBOZU_TEST_AUTO(testCompressor)
{
    test(WALB_DIFF_CMPR_NONE);
    test(WALB_DIFF_CMPR_GZIP);
    test(WALB_DIFF_CMPR_SNAPPY);
    test(WALB_DIFF_CMPR_LZMA);
}

#include <cstdio>
#include <stdexcept>
#include "walb_diff_compressor.hpp"
#include "walb_diff_gen.hpp"

walb::log::Generator::Config createConfig()
{
    walb::log::Generator::Config cfg;
    cfg.devLb = (10 << 20) >> 9;
    cfg.minIoLb = 512 >> 9;
    cfg.maxIoLb = 262144 >> 9;
    cfg.pbs = 512;
    cfg.maxPackPb = (1 << 20) >> 9;
    cfg.outLogPb = (10 << 20) >> 9;
    //cfg.outLogPb = (1 << 20) >> 9;
    cfg.lsid = 0;
    cfg.isPadding = true;
    cfg.isDiscard = true;
    cfg.isAllZero = true;
    cfg.isVerbose = false;

    cfg.check();
    return cfg;
}

void printPackRaw(char *packRaw)
{
    walb::DiffPackHeader &pack = *(walb::DiffPackHeader *)packRaw;
    ::printf("<<<<<<<<<<<<<<<<<<<<<\n");
    pack.print();
    ::printf(">>>>>>>>>>>>>>>>>>>>>\n");
}

std::vector<std::vector<char>> generateRawPacks()
{
    walb::log::Generator::Config cfg = createConfig();
    walb::diff::Generator g(cfg);
    g.generate();
    walb::DiffMemory &diffMem0 = g.data();

    std::vector<std::vector<char>> packV0;
    walb::diff::Packer packer;

    /* Convert memory data to raw pack list. */
    const walb::DiffMemory::Map& map = diffMem0.getMap();
	for (const auto& i : map) {
		const walb::DiffRecIo& recIo = i.second;
        if (!packer.add(recIo.record(), recIo.io().get())) {
            packV0.push_back(packer.getPackAsVector());
            packer.add(recIo.record(), recIo.io().get());
        }
	}
    if (!packer.empty()) {
        packV0.push_back(packer.getPackAsVector());
    }
    //::printf("Number of packs: %zu\n", packV0.size());

    return packV0;
}

void testPackCompression(int type, const char *rawPack, size_t size)
{
    walb::PackCompressor compr(type);
    walb::PackUncompressor ucompr(type);

    walb::diff::MemoryPack mpack0(rawPack, size);

    Buffer p1 = compr.convert(mpack0.rawPtr());
    walb::diff::MemoryPack mpack1(p1.data(), p1.size());
    Buffer p2 = ucompr.convert(mpack1.rawPtr());
    walb::diff::MemoryPack mpack2(p2.data(), p2.size());

    CYBOZU_TEST_EQUAL(mpack0.size(), mpack2.size());
    int ret = ::memcmp(mpack0.rawPtr(), mpack2.rawPtr(), mpack0.size());
    CYBOZU_TEST_EQUAL(ret, 0);
#if 0
    printPackRaw(mpack0.rawPtr());
    printPackRaw(mpack1.rawPtr());
    printPackRaw(mpack2.rawPtr());
#endif
}

void testDiffCompression(int type)
{
    for (std::vector<char> &pk : generateRawPacks()) {
        testPackCompression(type, &pk[0], pk.size());
    }
}

CYBOZU_TEST_AUTO(walbDiffCompressor)
{
    testDiffCompression(::WALB_DIFF_CMPR_SNAPPY);
    testDiffCompression(::WALB_DIFF_CMPR_GZIP);
    testDiffCompression(::WALB_DIFF_CMPR_LZMA);
}

static const uint32_t headerSize = 4;
std::mutex g_mu;
static cybozu::XorShift g_rg;

size_t size(const Buffer& b)
{
    uint32_t len;
    if (b.empty()) throw cybozu::Exception("size Buffer null");
    memcpy(&len, b.data(), headerSize);
    return len;
}

bool compare(const Buffer& lhs, const Buffer& rhs)
{
    const size_t lhsSize = size(lhs);
    const size_t rhsSize = size(rhs);
    printf("lhsSize=%d, rhsSize=%d\n", (int)lhsSize, (int)rhsSize); fflush(stdout);
    if (lhsSize != rhsSize) return false;
    return memcmp(lhs.data(), rhs.data(), lhsSize) == 0;
}

static Buffer copy(const char *buf)
{
    uint32_t len;
    memcpy(&len, buf, headerSize);
    Buffer ret(headerSize + len);
    memcpy(ret.data(), buf, headerSize + len);
    return ret;
}
static std::string create(uint32_t len, int idx)
{
    std::string ret(headerSize + len, '0');
    char *p = &ret[0];
    memcpy(p, &len, headerSize);
    p += headerSize;
    p[0] = char(idx);
    std::lock_guard<std::mutex> lk(g_mu);
    for (uint32_t i = 1; i < len; i++) {
        p[i] = (char)g_rg();
    }
    return ret;
}

struct NoConverter : walb::compressor::PackCompressorBase {
    NoConverter(int, size_t) {}
    void convertRecord(char *, size_t, walb_diff_record&, const char *, const walb_diff_record&) {}
    Buffer convert(const char *buf)
    {
        g_mu.lock();
        int wait = g_rg() % 100;
        g_mu.unlock();
        cybozu::Sleep(wait);
        return copy(buf);
    }
};

typedef walb::compressor_local::ConverterQueueT<NoConverter, NoConverter> ConvQ;

CYBOZU_TEST_AUTO(ConverterQueue)
{
    const size_t maxQueueNum = 100;
    const size_t threadNum = 10;
    const bool doCompress = false;
    const int type = 0;
    const size_t para = 0;
    ConvQ cv(maxQueueNum, threadNum, doCompress, type, para);
    const uint32_t len = 1000;
    const size_t bufN = 300;
    walb::StrVec inData(bufN);
    std::vector<Buffer> inBuf(bufN);
    puts("CREATE"); fflush(stdout);
    for (size_t i = 0; i < bufN; i++) {
        inData[i] = create(len, i);
        inBuf[i] = copy(&inData[i][0]);
    }
    puts("PUSH"); fflush(stdout);
    std::thread pusht([&] {
        for (size_t i = 0; i < bufN; i++) {
            cv.push(inBuf[i]);
        }
    });
    puts("POP"); fflush(stdout);
    std::thread popt([&] {
        for (size_t i = 0; i < bufN; i++) {
            Buffer c = cv.pop();
            CYBOZU_TEST_EQUAL(size(c), len);
            CYBOZU_TEST_ASSERT(memcmp(c.data(), &inData[i][0], len) == 0);
        }
    });
    pusht.join();
    popt.join();
    puts("-end-"); fflush(stdout);
}

std::vector<Buffer> parallelConverter(
    bool isCompress, std::vector<Buffer> &&packV0,
    size_t maxQueueSize, size_t numThreads, int type, bool isFirstDelay)
{
    const int level = 0;
    walb::ConverterQueue cq(maxQueueSize, numThreads, isCompress, type, level);
    std::exception_ptr ep;
    std::vector<Buffer> packV1;

    std::thread popper([&cq, &ep, &packV1, isFirstDelay]() {
            try {
                if (!isFirstDelay) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                Buffer p = cq.pop();
                while (!p.empty()) {
                    //::printf("poped %p\n", p.get());
                    packV1.push_back(std::move(p));
                    p = cq.pop();
                }
            } catch (...) {
                ::printf("caught error\n"); /* debug */
                ep = std::current_exception();
            }
        });

    /*
     * isFirstDelay value is
     *   true:  init -> pop() -> push().
     *   false: init -> push() -> pop().
     */
    if (isFirstDelay) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    //::printf("number of packes: %zu\n", packV0.size()); /* debug */
    for (Buffer &buf : packV0) {
        bool ret = cq.push(buf);
        CYBOZU_TEST_ASSERT(ret);
    }
    cq.quit();
    cq.join();
    popper.join();
    if (ep) std::rethrow_exception(ep);

    return packV1;
}

CYBOZU_TEST_AUTO(convertNothing)
{
    walb::ConverterQueue cq(4, 2, true, WALB_DIFF_CMPR_SNAPPY, 0);
    std::exception_ptr ep;

    std::thread popper([&cq, &ep]() {
            try {
                Buffer p = cq.pop();
                while (!p.empty()) {
                    /* do nothing */
                    p = cq.pop();
                }
            } catch (...) {
                ep = std::current_exception();
            }
        });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    cq.quit();
    cq.join();
    popper.join();
    if (ep) std::rethrow_exception(ep);
}

std::vector<Buffer> parallelCompress(
    std::vector<Buffer> &&packV, size_t maxQueueSize, size_t numThreads, int type, bool isFirstDelay)
{
    return parallelConverter(true, std::move(packV), maxQueueSize, numThreads, type, isFirstDelay);
}

std::vector<Buffer> parallelUncompress(
    std::vector<Buffer> &&packV, size_t maxQueueSize, size_t numThreads, int type, bool isFirstDelay)
{
    return parallelConverter(false, std::move(packV), maxQueueSize, numThreads, type, isFirstDelay);
}

void testParallelCompressNothing(size_t maxQueueSize, size_t numThreads, int type, bool isFirstDelay)
{
    std::vector<Buffer> v1 = parallelCompress({}, maxQueueSize, numThreads, type, isFirstDelay);
    std::vector<Buffer> v2 = parallelUncompress(std::move(v1), maxQueueSize, numThreads, type, isFirstDelay);
    CYBOZU_TEST_ASSERT(v2.empty());
}

void testParallelCompress(size_t maxQueueSize, size_t numThreads, int type, bool isFirstDelay)
{
    std::vector<Buffer> packV = generateRawPacks();

    /* Convert pack representation. */
    std::vector<Buffer> packV0;
    for (const Buffer &v : packV) {
        packV0.push_back(v);
    }

    std::vector<Buffer> packV1 =
        parallelCompress(std::move(packV0), maxQueueSize, numThreads, type, isFirstDelay);

    std::vector<Buffer> packV2 =
        parallelUncompress(std::move(packV1), maxQueueSize, numThreads, type, isFirstDelay);

    /* Verify */
    CYBOZU_TEST_EQUAL(packV.size(), packV2.size());
    for (size_t i = 0; i < packV.size(); i++) {
        CYBOZU_TEST_ASSERT(::memcmp(&packV[i][0], packV2[i].data(), packV[i].size()) == 0);
    }
}

CYBOZU_TEST_AUTO(parallelCompress)
{
    for (bool isFirstDelay : {true, false}) {
        testParallelCompressNothing(8, 4, ::WALB_DIFF_CMPR_NONE, isFirstDelay);
        testParallelCompress(8, 4, ::WALB_DIFF_CMPR_SNAPPY, isFirstDelay);
        testParallelCompress(8, 4, ::WALB_DIFF_CMPR_GZIP, isFirstDelay);
        testParallelCompress(8, 4, ::WALB_DIFF_CMPR_LZMA, isFirstDelay);
    }
}
