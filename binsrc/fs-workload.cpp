#include <sstream>
#include "cybozu/option.hpp"
#include "walb_util.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "file_path.hpp"
#include "walb_types.hpp"
#include "easy_signal.hpp"

using namespace walb;

struct Params
{
    std::string workDir;
    size_t workdataSize;
    size_t minFileSize;
    size_t maxFileSize;
    size_t sleepMs;
};

struct Option
{
    size_t nrThreads;
    Params params;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("fs-workload: workload in filesystem");
        opt.appendOpt(&nrThreads, 1, "t", ": number of threads");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug meesages to stderr.");
        opt.appendParam(&params.workDir, "WORKING_DIR", ": working directory path.");
        opt.appendOpt(&params.workdataSize, 32 * MEBI, "size", ": workload data size [byte]. (default 32M)");
        opt.appendOpt(&params.minFileSize, 0, "minfs", ": minimum file size [byte]. (default 0)");
        opt.appendOpt(&params.maxFileSize, 1 * MEBI, "maxfs", ": maximum file size [byte]. (default 1M)");
        opt.appendOpt(&params.sleepMs, 0, "sleep", ": sleep time between operations [ms] (default 0)");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (params.minFileSize > params.maxFileSize) {
            cybozu::Exception("bad file size params") << params.minFileSize << params.maxFileSize;
        }
    }
};

void preWork(const Params& params, size_t id)
{
    cybozu::FilePath fp(params.workDir);
    fp += cybozu::itoa(id);
    if (fp.stat().exists()) {
        throw cybozu::Exception("already exists") << fp;
    }
    if (!fp.mkdir()) {
        throw cybozu::Exception("mkdir failed") << fp;
    }
}

void postWork(const Params& params, size_t id)
{
    cybozu::FilePath fp(params.workDir);
    fp += cybozu::itoa(id);
    if (!fp.rmdirRecursive()) {
        LOGs.warn() << "rmdire recursively failed" << fp;
    }
}

/**
 * Each 512 bytes block must be filled at least partially.
 */
template <typename Rand>
void setRandomData(AlignedArray& buf, Rand& rand)
{
    size_t off = 0;
    while (off < buf.size()) {
        const size_t size = std::min<size_t>(buf.size() - off, 16);
        rand.fill(buf.data() + off, size);
        off += 512;
    }
}

// In order to control total file size of all the threads.
std::atomic<size_t> totalSize_(0);

struct Stat
{
    enum {
        CREATE = 0, DELETE = 1, UPDATE = 2, APPEND = 3, MAX = 4,
    };
    std::vector<size_t> nrV;
    std::vector<size_t> sizeV;

    Stat() : nrV(MAX), sizeV(MAX) {
    }
    void update(int type, size_t oldSize, size_t newSize, size_t writeSize) {
        assert(type < MAX);
        if (oldSize < newSize) {
            const size_t delta = newSize - oldSize;;
            totalSize_ += delta;
        } else {
            const size_t delta = oldSize - newSize;
            totalSize_ -= delta;
        }
        nrV[type]++;
        sizeV[type] += writeSize;
    }
    std::string str() const {
        std::stringstream ss;
        ss << cybozu::util::formatString("CREATE %zu %zu\n", nrV[CREATE], sizeV[CREATE]);
        ss << cybozu::util::formatString("DELETE %zu %zu\n", nrV[DELETE], sizeV[DELETE]);
        ss << cybozu::util::formatString("UPDATE %zu %zu\n", nrV[UPDATE], sizeV[UPDATE]);
        ss << cybozu::util::formatString("APPEND %zu %zu\n", nrV[APPEND], sizeV[APPEND]);
        return ss.str();
    }
    friend inline std::ostream& operator<<(std::ostream& os, const Stat& stat) {
        os << stat.str();
        return os;
    }

    void merge(const Stat& rhs) {
        for (size_t i = 0; i < MAX; i++) {
            nrV[i] += rhs.nrV[i];
            sizeV[i] += rhs.sizeV[i];
        }
    }
};


/**
 * This is not thread-safe.
 */
class Files
{
private:
    // key: file name (hex), value: file size.
    using Map = std::map<uint32_t, size_t>;
    const size_t threadId_;
    const cybozu::FilePath workDir_;
    const Params& params_;
    Stat& stat_;
    Map map_;
    mutable cybozu::util::XorShift128 rand_;

public:
    Files(const Params& params, Stat& stat, size_t threadId)
        : threadId_(threadId)
        , workDir_(cybozu::FilePath(params.workDir) + cybozu::itoa(threadId))
        , params_(params)
        , stat_(stat)
        , map_()
        , rand_(::time(0) + threadId) {
    }
    void doStep() {
        if (map_.empty()) {
            create();
            return;
        }
        const size_t val = rand_() % 100;
        if (totalSize_ > params_.workdataSize || val < 20) {
            remove();
        } else if (val < 40) {
            update();
        } else if (val < 60) {
            append();
        } else {
            create();
        }
    }

private:
    static std::string idToStr(uint32_t id) {
        return cybozu::util::intToHexStr(id, true);
    }
    uint32_t getUniqueId() const {
        uint32_t id;
        size_t nrRetry = 100;
        for (size_t i = 0; i < nrRetry; i++) {
            id = rand_();
            if (map_.find(id) == map_.cend()) return id;
        }
        throw cybozu::Exception("getUniqueId: proceeds max retry counts")
            << threadId_ << nrRetry;
    }
    uint32_t getIdRandomlyInMap(size_t* fileSizeP = nullptr) const {
        assert(!map_.empty());
        uint32_t id = rand_();
        auto it = map_.lower_bound(id);
        if (it == map_.cend()) --it; // the last item.
        if (fileSizeP != nullptr) *fileSizeP = it->second;
        return it->first;
    }
    void fsyncRandomly(cybozu::util::File& file) {
        if (rand_() % 100 == 0) {
            file.fsync();
        }
    }
    void create() {
        const uint32_t id = getUniqueId();
        cybozu::FilePath fp(workDir_);
        fp += idToStr(id);
        cybozu::util::File file(fp.str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        const size_t fileSize = rand_.get(params_.minFileSize, params_.maxFileSize + 1);
        AlignedArray buf(fileSize);
        setRandomData(buf, rand_);
        file.write(buf.data(), buf.size());
        fsyncRandomly(file);
        file.close();
        map_.emplace(id, fileSize);
        stat_.update(Stat::CREATE, 0, fileSize, fileSize);
        LOGs.debug() << "create" << id << idToStr(id) << fileSize;
    }
    void remove() {
        assert(!map_.empty());
        size_t fileSize;
        const uint32_t id = getIdRandomlyInMap(&fileSize);
        cybozu::FilePath fp(workDir_);
        fp += idToStr(id);
        int err;
        if (!fp.unlink(&err)) {
            LOGs.warn() << "remove failed" << fp << err << cybozu::ErrorNo(err);
        }
        auto it = map_.find(id);
        assert(it != map_.end());
        assert(it->second == fileSize);
        map_.erase(it);
        stat_.update(Stat::DELETE, fileSize, 0, 0);
        LOGs.debug() << "remove" << id << idToStr(id) << fileSize;
    }
    void update() {
        assert(!map_.empty());
        size_t fileSize;
        const uint32_t id = getIdRandomlyInMap(&fileSize);
        cybozu::FilePath fp(workDir_);
        fp += idToStr(id);
        cybozu::util::File file(fp.str(), O_RDWR);
        const size_t newFileSize = rand_.get(params_.minFileSize, params_.maxFileSize + 1);
        const size_t offset = newFileSize <= 1 ? 0 : rand_.get(newFileSize - 1);
        const size_t size = newFileSize == 0 ? 0 : rand_.get(0, newFileSize - offset);
        AlignedArray buf(size);
        setRandomData(buf, rand_);
        if (newFileSize < fileSize) file.ftruncate(newFileSize);
        file.lseek(offset);
        file.write(buf.data(), buf.size());
        fsyncRandomly(file);
        file.close();
        auto it = map_.find(id);
        it->second = newFileSize;
        stat_.update(Stat::UPDATE, fileSize, newFileSize, size);
        LOGs.debug() << "update" << id << idToStr(id) << fileSize << newFileSize << size;
    }
    void append() {
        assert(!map_.empty());
        size_t fileSize;
        const uint32_t id = getIdRandomlyInMap(&fileSize);
        cybozu::FilePath fp(workDir_);
        fp += idToStr(id);
        cybozu::util::File file(fp.str(), O_RDWR | O_APPEND);
        const size_t deltaSize = rand_.get(params_.minFileSize, params_.maxFileSize + 1);
        AlignedArray buf(deltaSize);
        setRandomData(buf, rand_);
        file.write(buf.data(), buf.size());
        fsyncRandomly(file);
        file.close();
        auto it = map_.find(id);
        it->second = fileSize + deltaSize;
        stat_.update(Stat::APPEND, fileSize, fileSize + deltaSize, deltaSize);
        LOGs.debug() << "append" << id << idToStr(id) << fileSize << deltaSize;
    }
};

void doWork(const Params& params, Stat& stat, size_t threadId)
{
    Files files(params, stat, threadId);
    for (;;) {
        files.doStep();
        if (cybozu::signal::gotSignal()) break;
        if (params.sleepMs > 0) util::sleepMs(params.sleepMs);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    cybozu::signal::setSignalHandler({SIGINT, SIGQUIT, SIGTERM});

    std::vector<Stat> statV(opt.nrThreads);
    cybozu::thread::ThreadRunnerSet thS;
    for (size_t i = 0; i < opt.nrThreads; i++) {
        preWork(opt.params, i);
        thS.add([&, i]() { doWork(opt.params, statV[i], i); } );
    }
    thS.start();

    auto epV = thS.join();
    for (std::exception_ptr ep : epV) {
        if (ep) std::rethrow_exception(ep);
    }
    for (size_t i = 0; i < opt.nrThreads; i++) {
        std::cout << "--------------------" << i << "--------------------" << std::endl;
        std::cout << statV[i];
        if (i != 0) statV[0].merge(statV[i]);
        postWork(opt.params, i);
    }
    std::cout << "--------------------" << "total" << "--------------------" << std::endl;
    std::cout << statV[0];
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("fs-workload")
