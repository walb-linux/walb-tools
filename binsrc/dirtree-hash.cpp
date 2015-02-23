/**
 * Calculate hash value(s) of files in a directory tree.
 */
#include "cybozu/option.hpp"
#include "fileio.hpp"
#include "file_path.hpp"
#include "walb_types.hpp"
#include "walb_util.hpp"
#include "siphash.hpp"
#include <vector>
#include <algorithm>
#include <cstdlib>

struct Flags
{
    bool useTime;
    bool useOwner;
    bool usePerm;
    bool isEach;
};

struct Option
{
    std::string dirStr;
    size_t ppInterval;
    Flags flags;
    bool isAll;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Calculate hash value(s) of files in a directory tree.\n"
                           "Currently only file name and contents will be scanned.\n");
        opt.appendParamOpt(&dirStr, ".", "DIR_PATH", ": directory path (default: current directory)");
        opt.appendBoolOpt(&flags.useTime, "time", ": use modified time stamp.");
        opt.appendBoolOpt(&flags.useOwner, "owner", ": use uid/gid information.");
        opt.appendBoolOpt(&flags.usePerm, "perm", ": use permission(st_mode) information.");
        opt.appendBoolOpt(&flags.isEach, "each", ": put hash for each file.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug logs.");
        opt.appendBoolOpt(&isAll, "all", ": the same as -time -owner -perm.");
        opt.appendOpt(&ppInterval, 0, "pp", ": progress printer interval (default off).");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (isAll) {
            flags.useTime = true;
            flags.useOwner = true;
            flags.usePerm = true;
        }
    }
};

enum Type
{
    File, Dir, Other,
};

struct DirEntry
{
    std::string name;
    cybozu::FileStat st;

    bool operator<(const DirEntry& rhs) const {
        return this->name < rhs.name;
    }
};

using DirEntryVec = std::vector<DirEntry>;

DirEntryVec getSortedListInDir(const cybozu::FilePath& dirPath)
{
    DirEntryVec v;
    cybozu::Directory dir(dirPath.str());
    while (!dir.isEnd()) {
        std::string name = dir.next();
        if (name == "." || name == "..") continue;
        const cybozu::FilePath path = dirPath + name;
        v.emplace_back();
        v.back().name = std::move(name);
        v.back().st = path.lstat();
    }
    std::sort(v.begin(), v.end());
    return v;
}

constexpr const size_t BULK_SIZE = 64 * 1024;

void readFileToHasher(const cybozu::FilePath& filePath, cybozu::SipHash24& hasher)
{
    walb::AlignedArray buf(BULK_SIZE);
    cybozu::util::File file(filePath.str(), O_RDONLY);
    for (;;) {
        const size_t r = file.readsome(buf.data(), buf.size());
        if (r == 0) return;
        hasher.compress(buf.data(), r);
    }
}

class ProgressPrinter
{
private:
    size_t c_;
    size_t interval_;
public:
    ProgressPrinter()
        : c_(0), interval_(0) {
    }
    void setInterval(size_t interval) {
        interval_ = interval;
    }
    void inc() {
        c_++;
        if (interval_ == 0) return;
        if (c_ % interval_ == 0) {
            ::fprintf(::stderr, ".");
            ::fflush(::stderr);
            if (c_ % (interval_ * 64) == 0) {
                ::fprintf(::stderr, "\n");
            }
        }
    }
} pp_;

using NameVec = std::vector<std::string>;

void walk(const cybozu::FilePath& dirPath, const NameVec& dirNameV, const Flags& flags, cybozu::SipHash24& sharedHasher)
{
    for (const DirEntry& ent : getSortedListInDir(dirPath)) {
        cybozu::SipHash24 localHasher;
        cybozu::SipHash24& hasher = flags.isEach ? localHasher : sharedHasher;

        cybozu::FilePath path = dirPath + ent.name;
        NameVec nameV = dirNameV;
        nameV.push_back(ent.name);
        if (ent.st.isDirectory() && !ent.st.isSimlink()) {
            walk(path, nameV, flags, hasher);
        } else if (ent.st.isFile() && !ent.st.isSimlink()) {
            readFileToHasher(path, hasher);
        }
        const std::string name = cybozu::util::concat(nameV, "/");
        hasher.compress(name.data(), name.size());
        const struct stat& st = ent.st.getStat();
        if (flags.useTime) {
            hasher.compress(&st.st_mtime, sizeof(time_t));
        }
        if (flags.useOwner) {
            hasher.compress(&st.st_uid, sizeof(uid_t));
            hasher.compress(&st.st_gid, sizeof(gid_t));
        }
        if (flags.usePerm) {
            hasher.compress(&st.st_mode, sizeof(mode_t));
        }
        if (flags.isEach) {
            const cybozu::Hash128 hash = localHasher.finalize128();
            ::printf("%s %s\n", hash.str().c_str(), name.c_str());
        }
        pp_.inc();
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    walb::util::setLogSetting("-", opt.isDebug);
    pp_.setInterval(opt.ppInterval);
    const cybozu::FilePath targetDir = cybozu::FilePath(opt.dirStr);
    if (!targetDir.stat().isDirectory()) {
        throw cybozu::Exception(__func__) << "not directory" << targetDir;
    }
    cybozu::SipHash24 hasher;
    walk(targetDir, {}, opt.flags, hasher);
    if (!opt.flags.isEach) {
        const cybozu::Hash128 hash = hasher.finalize128();
        ::printf("%s %s\n", hash.str().c_str(), opt.dirStr.c_str());
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("dirtree-hash");
