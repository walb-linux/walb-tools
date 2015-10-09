#include <sstream>
#include "cybozu/option.hpp"
#include "walb_util.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "file_path.hpp"
#include "walb_types.hpp"
#include "easy_signal.hpp"

using namespace walb;

enum {
    CREATE = 0, DELETE = 1, UPDATE = 2, APPEND = 3, READ = 4, TYPE_MAX = 5,
};

struct IoRatio
{
    std::vector<size_t> ratio; // cumulative.
    size_t totalRatio;

    IoRatio() : ratio(TYPE_MAX), totalRatio(0) {
    }
    void parse(const std::string& s) {
        std::vector<std::string> ss = cybozu::util::splitString(s, ":");
        if (ss.size() != 5) {
            throw cybozu::Exception("bad IoRation representation") << s;
        }
        totalRatio = 0;
        for (size_t i = 0; i < TYPE_MAX; i++) {
            ratio[i] = totalRatio + static_cast<size_t>(cybozu::atoi(ss[i]));
            totalRatio = ratio[i];
        }
        if (totalRatio == 0) {
            throw cybozu::Exception("totalRatio must not be 0");
        }
    }
    template <typename Rand>
    int choose(Rand& rand) const {
        const size_t v = rand.get(totalRatio);
        for (size_t i = 0; i < TYPE_MAX; i++) {
            if (v < ratio[i]) return i;
        }
        throw cybozu::Exception("must not reach here.");
    }
};

struct Params
{
    std::string workDir;
    size_t workdataSize;
    size_t minFileSize;
    size_t maxFileSize;
    size_t sleepMs;

    IoRatio ioRatio;
};

struct Option
{
    size_t nrThreads;
    Params params;
    bool isDebug;
    std::string ratioStr;

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
        opt.appendOpt(&ratioStr, "30:20:10:10:30", "ratio", ": create:delete:update:append:read ratio (default: 30:20:10:10:30)");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (params.minFileSize > params.maxFileSize) {
            cybozu::Exception("bad file size params") << params.minFileSize << params.maxFileSize;
        }
        params.ioRatio.parse(ratioStr);
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
    std::vector<size_t> nrV;
    std::vector<size_t> sizeV;

    Stat() : nrV(TYPE_MAX), sizeV(TYPE_MAX) {
    }
    void update(int type, size_t oldSize, size_t newSize, size_t accessSize) {
        assert(type < TYPE_MAX);
        if (oldSize < newSize) {
            const size_t delta = newSize - oldSize;;
            totalSize_ += delta;
        } else {
            const size_t delta = oldSize - newSize;
            totalSize_ -= delta;
        }
        nrV[type]++;
        sizeV[type] += accessSize;
    }
    std::string str() const {
        std::stringstream ss;
        ss << cybozu::util::formatString("CREATE %zu %zu\n", nrV[CREATE], sizeV[CREATE]);
        ss << cybozu::util::formatString("DELETE %zu %zu\n", nrV[DELETE], sizeV[DELETE]);
        ss << cybozu::util::formatString("UPDATE %zu %zu\n", nrV[UPDATE], sizeV[UPDATE]);
        ss << cybozu::util::formatString("APPEND %zu %zu\n", nrV[APPEND], sizeV[APPEND]);
        ss << cybozu::util::formatString("READ   %zu %zu\n", nrV[READ], sizeV[READ]);
        return ss.str();
    }
    friend inline std::ostream& operator<<(std::ostream& os, const Stat& stat) {
        os << stat.str();
        return os;
    }

    void merge(const Stat& rhs) {
        for (size_t i = 0; i < TYPE_MAX; i++) {
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
        if (totalSize_ > params_.workdataSize) {
            remove();
            return;
        }
        const int type = params_.ioRatio.choose(rand_);
        switch (type) {
        case CREATE: create(); break;
        case DELETE: remove(); break;
        case UPDATE: update(); break;
        case APPEND: append(); break;
        case READ:   read();   break;
        default:
            throw cybozu::Exception("must not reach here");
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
        stat_.update(CREATE, 0, fileSize, fileSize);
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
        stat_.update(DELETE, fileSize, 0, 0);
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
        const size_t offset = newFileSize == 0 ? 0 : rand_.get(newFileSize);
        const size_t size = newFileSize == 0 ? 0 : rand_.get(0, newFileSize - offset + 1);
        AlignedArray buf(size);
        setRandomData(buf, rand_);
        if (newFileSize < fileSize) file.ftruncate(newFileSize);
        file.lseek(offset);
        file.write(buf.data(), buf.size());
        fsyncRandomly(file);
        file.close();
        auto it = map_.find(id);
        it->second = newFileSize;
        stat_.update(UPDATE, fileSize, newFileSize, size);
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
        stat_.update(APPEND, fileSize, fileSize + deltaSize, deltaSize);
        LOGs.debug() << "append" << id << idToStr(id) << fileSize << deltaSize;
    }
    void read() {
        assert(!map_.empty());
        size_t fileSize;
        const uint32_t id = getIdRandomlyInMap(&fileSize);
        cybozu::FilePath fp(workDir_);
        fp += idToStr(id);
        cybozu::util::File file(fp.str(), O_RDONLY);
        const size_t offset = fileSize == 0 ? 0 : rand_.get(fileSize);
        const size_t size = fileSize == 0 ? 0 : rand_.get(0, fileSize - offset + 1);
        AlignedArray buf(size);
        file.lseek(offset);
        file.read(buf.data(), buf.size());
        file.close();
        stat_.update(READ, fileSize, fileSize, size);
        LOGs.debug() << "read" << id << idToStr(id) << fileSize << size;
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
