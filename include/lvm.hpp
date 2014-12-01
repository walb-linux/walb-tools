#pragma once
/**
 * @file
 * @brief lvm manager.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <cstdio>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <thread>
#include <chrono>
#include "cybozu/file.hpp"
#include "cybozu/atoi.hpp"
#include "cybozu/itoa.hpp"
#include "cybozu/string_operation.hpp"
#include "cybozu/exception.hpp"
#include "fileio.hpp"
#include "file_path.hpp"
#include "process.hpp"

namespace cybozu {
namespace lvm {

const unsigned int LBS = 512;

using StrVec = std::vector<std::string>;

class LvAttr;
class Lv;
class Vg;

using LvMap = std::map<std::string, Lv>;
using LvList = std::vector<Lv>;
using VgList = std::vector<Vg>;

/**
 * Discard option.
 */
enum
{
    IGNORE,
    NOPASSDOWN,
    PASSDOWN
};

namespace local {

inline bool isSpace(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

inline void trim(std::string &str)
{
    cybozu::Trim(str, isSpace);
}

inline std::vector<std::string> splitAndTrim(const std::string &str, char sep)
{
    std::vector<std::string> v = cybozu::Split(str, sep);
    for (std::string &s : v) trim(s);
    return v;
}

inline bool isDeviceAvailable(const cybozu::FilePath &path) {
    if (!path.stat().exists()) return false;
    std::string s = cybozu::process::call("/sbin/dmsetup", {
            "info", "-c", "--noheadings", "-o", "Open", path.str() });
    trim(s);
    uint64_t i = cybozu::atoi(s);
    return i == 0;
}

/**
 * Call lvm command.
 *
 * @lvmCommand lvm binary path like "/usr/bin/lvs" or "/usr/bin/vgs".
 * @options options like "lv_name,lv_size,origin,vg_name".
 * @args another arguments.
 * RETURN:
 *   stdout of lvm command.
 */
inline std::string callLvm(
    const std::string &cmd, const std::string &options,
    const std::vector<std::string> &args = {})
{
    std::vector<std::string> args0 = {
        "--units=b", "--nosuffix", "--options=" + options,
        "--separator=,", "--noheadings", "--unbuffered",
    };
    args0.insert(args0.end(), args.cbegin(), args.cend());
    return cybozu::process::call(cmd, args0);
}

inline void sleepMs(unsigned int ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

/**
 * @s size string [byte].
 * RETURN:
 *   parsed size [logical block].
 */
inline uint64_t parseSizeLb(const std::string &s) {
    uint64_t size = cybozu::atoi(s);
    if (size % LBS != 0) {
        throw cybozu::Exception(__func__)
            << "size must be multiples of logical block size"
            << s << size;
    }
    return size / LBS;
}

/**
 * Create lvm size option.
 */
inline std::string getSizeOpt(uint64_t sizeLb)
{
    return std::string("--size=") + cybozu::itoa(sizeLb * LBS) + "b";
}

inline std::string getVirtualSizeOpt(uint64_t sizeLb)
{
    return std::string("--virtualsize=") + cybozu::itoa(sizeLb * LBS) + "b";
}

inline std::string getNameOpt(const std::string &name)
{
    return std::string("--name=") + name;
}

inline std::string getThinpoolOpt(const std::string &vgName, const std::string &poolName)
{
    return std::string("--thinpool=") + vgName + "/" + poolName;
}

inline std::string getPermissionOpt(bool isWritable)
{
    return std::string("--permission=") + (isWritable ? "rw" : "r");
}

inline std::string getDiscardsOpt(int opt)
{
    struct Pair {
        int opt;
        const char *name;
    } tbl[] = {
        {IGNORE, "ignore"},
        {NOPASSDOWN, "nopassdown"},
        {PASSDOWN, "passdown"},
    };
    for (const Pair &p : tbl) {
        if (p.opt == opt) {
            return std::string("--discards=") + p.name;
        }
    }
    throw cybozu::Exception(__func__) << "bad discards opt" << opt;
}

/**
 * RETURN:
 *   true when available,
 *   false in timeout.
 */
inline void waitForDeviceAvailable(cybozu::FilePath &path, size_t timeoutMs = 5000)
{
    const size_t intervalMs = 100;
    for (size_t i = 0; i < timeoutMs / intervalMs + 1; i++) {
        if (isDeviceAvailable(path)) return;
        local::sleepMs(intervalMs);
    }
    throw cybozu::Exception(__func__) << "wait for device timeout" << path;
}

} //namespace local

/**
 * Prototypes.
 */
cybozu::FilePath getLvmPath(const std::string &vgName, const std::string &name);
Lv createLv(const std::string &vgName, const std::string &lvName, uint64_t sizeLb);
Lv createTv(const std::string &vgName, const std::string &poolName, const std::string &lvName, uint64_t sizeLb);
Lv createTp(const std::string &vgName, const std::string &poolName, uint64_t sizeLb);
Lv createSnap(
    const std::string &vgName, const std::string &lvName, const std::string &snapName,
    bool isWritable, uint64_t sizeLb);
Lv createTSnap(
    const std::string &vgName, const std::string &lvName, const std::string &snapName,
    bool isWritable);
void remove(const std::string &pathStr);
void resize(const std::string &pathStr, uint64_t newSizeLb);
LvList listLv(const std::string &arg);
LvMap getLvMap(const std::string &arg);
bool exists(const std::string &vgName, const std::string &name);
bool lvExists(const std::string &vgName, const std::string &lvName);
bool snapExists(const std::string &vgName, const std::string &snapName);
Lv locate(const std::string &arg);
Lv locate(const std::string &vgName, const std::string &name);
VgList listVg(const std::string &vgName);
Vg getVg(const std::string &vgName);
bool vgExists(const std::string &vgName);
LvAttr getLvAttr(const std::string lvPathStr);
bool tpExists(const std::string &vgName, const std::string &poolName);


/**
 * Logical volume attributes.
 */
class LvAttr
{
private:
    std::string data_;
public:
    void set(const std::string &data) {
        data_ = data;
    }
    bool isTypeThinpool() const { return get(0) == 't'; }
    bool isTypeSnapshot() const { return get(0) == 's'; }
    bool isTypeThinVolume() const { return get(0) == 'V'; }
    bool isTypeOrigin() const { return get(0) == 'o'; }
    bool isTypeNone() const { return get(0) == '-'; }

    bool isReadOnly() const { return get(1) == 'r'; }
    bool isWritable() const { return get(1) == 'w'; }

    friend inline std::ostream &operator<<(std::ostream& os, const LvAttr& attr) {
        os << attr.data_;
        return os;
    }
private:
    char get(size_t i) const {
        if (data_.size() <= i) {
            throw cybozu::Exception(__func__)
                << "invalid index" << data_.size() << i;
        }
        return data_[i];
    }
};

/**
 * Logical volume manager.
 */
class Lv
{
private:
    std::string vgName_; /* volume group name. */
    std::string lvName_; /* logical volume name. */
    std::string snapName_; /* snapshot name. "" if not snapshot. */
    uint64_t sizeLb_; /* [logical block]. */
    std::string poolName_; /* thinpool name.
                              "" if the lv or snapshot does not in thinpool. */
    LvAttr attr_;
public:
    Lv() = default;
    Lv(const std::string &vgName, const std::string &lvName,
       const std::string &snapName, uint64_t sizeLb,
       const std::string &poolName, const LvAttr &attr)
        : vgName_(vgName), lvName_(lvName), snapName_(snapName)
        , sizeLb_(sizeLb), poolName_(poolName), attr_(attr) {
    }
    const std::string &vgName() const { return vgName_; }
    const std::string &lvName() const { return lvName_; }
    const std::string &snapName() const { return snapName_; }
    const std::string &poolName() const { return poolName_; }
    const std::string &name() const {
        return isSnapshot() ? snapName() : lvName();
    }
    uint64_t sizeLb() const { return sizeLb_; }
    bool isSnapshot() const { return !snapName_.empty(); }
    bool isThinVolume() const { return !poolName_.empty(); }
    cybozu::FilePath path() const {
        return getLvmPath(vgName_, name());
    }
    bool exists() const {
        return cybozu::lvm::exists(vgName(), name());
    }
    const LvAttr &attr() const { return attr_; }
    Lv createSnapshot(const std::string &snapName, bool isWritable, uint64_t sizeLb) const {
        verifyVolume();
        if (isThinVolume()) {
            throw cybozu::Exception(__func__) << "sizeLb parameter not required";
        }
        return cybozu::lvm::createSnap(vgName_, lvName_, snapName, isWritable, sizeLb);
    }
    Lv createSnapshot(const std::string &snapName, bool isWritable) const {
        verifyVolume();
        if (!isThinVolume()) {
            throw cybozu::Exception(__func__) << "sizeLb parameter required";
        }
        return cybozu::lvm::createTSnap(vgName_, lvName_, snapName, isWritable);
    }
    /**
     * @snapName specify an empty string for wildcard.
     */
    bool hasSnapshot(const std::string &snapName = "") const {
        verifyVolume();
        if (snapName.empty()) {
            for (Lv &lv : listLv(vgName_)) {
                if (lv.lvName() == lvName_ && lv.isSnapshot()) return true;
            }
            return false;
        } else {
            if (!cybozu::lvm::exists(vgName_, snapName)) return false;
            Lv lv = locate(vgName_, snapName);
            return lv.isSnapshot() && lv.lvName() == lvName_;
        }
    }
    LvList snapshotList() const {
        verifyVolume();
        LvList v;
        for (Lv &lv : listLv(vgName_)) {
            if (lv.isSnapshot() && lv.lvName_ == lvName_) {
                v.push_back(lv);
            }
        }
        return v;
    }
    Lv getSnapshot(const std::string &snapName) const {
        Lv lv = locate(vgName_, snapName);
        if (lv.isSnapshot() && lv.lvName_ == lvName_) {
            return lv;
        }
        throw cybozu::Exception(__func__) << "Not found" << vgName_ << lvName_ << snapName;
    }
    Lv parent() const {
        verifySnapshot();
        return locate(vgName_, lvName_);
    }
    void resize(uint64_t newSizeLb) {
        cybozu::lvm::resize(path().str(), newSizeLb);
        sizeLb_ = newSizeLb;
    }
    void remove() {
        cybozu::lvm::remove(path().str());
    }
    friend inline std::ostream& operator<<(std::ostream& os, const Lv& lv) {
        const std::string parent = lv.isSnapshot() ? lv.lvName_ : "";
        os << lv.vgName_ << "/" << lv.name() << " sizeLb " << lv.sizeLb_
           << " parent (" << parent << ") "
           << " thinpool (" << lv.poolName_ << ")";
        return os;
    }
    void print(::FILE *fp = ::stdout) const {
        std::stringstream ss;
        ss << *this;
        ::fprintf(fp, "%s\n", ss.str().c_str());
    }
private:
    void verifyVolume() const {
        if (isSnapshot()) {
            throw cybozu::Exception(__func__)
                << "Must be logical volume" << name();
        }
    }
    void verifySnapshot() const {
        if (!isSnapshot()) {
            throw cybozu::Exception(__func__)
                << "Must be snapshot" << name();
        }
    }
};

/**
 * Volume group manager.
 */
class Vg
{
private:
    std::string vgName_;
    uint64_t sizeLb_;
    uint64_t freeLb_;

public:
    Vg(const std::string &vgName, uint64_t sizeLb, uint64_t freeLb)
        : vgName_(vgName), sizeLb_(sizeLb), freeLb_(freeLb) {
    }
    Lv create(const std::string &lvName, uint64_t sizeLb) {
        verifyFreeSize(sizeLb);
        Lv lv = createLv(vgName_, lvName, sizeLb);
        freeLb_ -= sizeLb;
        return lv;
    }
    Lv createThinpool(const std::string &poolName, uint64_t sizeLb) {
        verifyFreeSize(sizeLb);
        Lv lv = createTp(vgName_, poolName, sizeLb);
        freeLb_ -= sizeLb;
        return lv;
    }
    Lv createThin(const std::string &poolName, const std::string &lvName, uint64_t sizeLb) {
        if (!tpExists(vgName_, poolName)) {
            throw cybozu::Exception(__func__) << "thinpool not found" << vgName_ << poolName;
        }
        /* sizeLb is virtual size so capacity check is not necessary. */
        return createTv(vgName_, poolName, lvName, sizeLb);
    }
    const std::string &name() const { return vgName_; }
    uint64_t sizeLb() const { return sizeLb_; }
    uint64_t freeLb() const { return freeLb_; }
    void print(::FILE *fp = ::stdout) const {
        ::fprintf(
            fp,
            "vg %s sizeLb %" PRIu64 " freeLb %" PRIu64 "\n"
            , vgName_.c_str(), sizeLb_, freeLb_);
    }
private:
    void verifyFreeSize(uint64_t sizeLb) const {
        if (freeLb_ < sizeLb) {
            throw cybozu::Exception(__func__)
                << "free size not enough" << freeLb_ << sizeLb;
        }
    }
};

/**
 * Get lvm path.
 */
inline cybozu::FilePath getLvmPath(
    const std::string &vgName, const std::string &name)
{
    return cybozu::FilePath("/dev") + cybozu::FilePath(vgName)
        + cybozu::FilePath(name);
}

/**
 * Create a volume.
 */
inline Lv createLv(const std::string &vgName, const std::string &lvName, uint64_t sizeLb)
{
    cybozu::process::call("/sbin/lvcreate", {
            local::getNameOpt(lvName),
            local::getSizeOpt(sizeLb),
            vgName
    });

    cybozu::FilePath lvPath = getLvmPath(vgName, lvName);
    local::waitForDeviceAvailable(lvPath);

    Lv lv = locate(lvPath.str());
    if (lv.vgName() == vgName && lv.lvName() == lvName &&
        lv.sizeLb() == sizeLb && !lv.isSnapshot() && !lv.isThinVolume()) {
        return lv;
    }
    throw cybozu::Exception(__func__)
        << "volume creation failed" << vgName << lvName << sizeLb;
}

/**
 * Create a thin volume.
 */
inline Lv createTv(const std::string &vgName, const std::string &poolName, const std::string &lvName, uint64_t sizeLb)
{
    cybozu::process::call("/sbin/lvcreate", {
            local::getNameOpt(lvName),
            local::getThinpoolOpt(vgName, poolName),
            local::getVirtualSizeOpt(sizeLb)
        });

    cybozu::FilePath lvPath = getLvmPath(vgName, lvName);
    local::waitForDeviceAvailable(lvPath);

    Lv lv = locate(lvPath.str());
    if (lv.vgName() == vgName && lv.lvName() == lvName &&
        lv.poolName() == poolName && !lv.isSnapshot() && lv.isThinVolume() &&
        lv.attr().isTypeThinVolume()) {
        return lv;
    }
    throw cybozu::Exception(__func__)
        << "thinvolume creation failed" << vgName << poolName << lvName << sizeLb;
}

/**
 * Create a thin pool.
 */
inline Lv createTp(const std::string &vgName, const std::string &poolName, uint64_t sizeLb)
{
    cybozu::process::call("/sbin/lvcreate", {
            local::getThinpoolOpt(vgName, poolName),
            local::getSizeOpt(sizeLb),
            local::getDiscardsOpt(NOPASSDOWN)
        });

    cybozu::FilePath lvPath = getLvmPath(vgName, poolName);
    local::waitForDeviceAvailable(lvPath);

    Lv lv = locate(lvPath.str());
    if (lv.vgName() == vgName && lv.lvName() == poolName &&
        lv.attr().isTypeThinpool()) {
        return lv;
    }
    throw cybozu::Exception(__func__)
        << "thinpool creation failed" << vgName << poolName << sizeLb;
}

/**
 * Create a snapshot.
 * @sizeLb data size for snapshot area [logical block].
 */
inline Lv createSnap(
    const std::string &vgName, const std::string &lvName, const std::string &snapName,
    bool isWritable, uint64_t sizeLb)
{
    cybozu::FilePath lvPath = getLvmPath(vgName, lvName);
    Lv lv = locate(lvPath.str());
    if (!lv.attr().isTypeOrigin() && !lv.attr().isTypeNone()) {
        throw cybozu::Exception(__func__) << "bad lv to be origin" << lvPath;
    }
    const StrVec args = {
        "-s",
        local::getSizeOpt(sizeLb),
        local::getNameOpt(snapName),
        local::getPermissionOpt(isWritable),
        lvPath.str()
    };
    cybozu::process::call("/sbin/lvcreate", args);

    cybozu::FilePath snapPath = getLvmPath(vgName, snapName);
    local::waitForDeviceAvailable(snapPath);

    Lv snap = locate(snapPath.str());
    if (snap.snapName() == snapName && snap.isSnapshot() && snap.lvName() == lvName &&
        !snap.isThinVolume() && snap.attr().isTypeSnapshot()) {
        return lv;
    }
    throw cybozu::Exception(__func__)
        << "failed" << vgName << lvName << snapName << isWritable << sizeLb;
}

/**
 * Create a snapshot for a thin volume.
 */
inline Lv createTSnap(
    const std::string &vgName, const std::string &lvName, const std::string &snapName,
    bool isWritable)
{
    cybozu::FilePath lvPath = getLvmPath(vgName, lvName);
    Lv lv = locate(lvPath.str());
    if (!lv.attr().isTypeThinVolume()) {
        throw cybozu::Exception(__func__) << "not thin volume" << lvPath;
    }

    const StrVec args = {
        "-s",
        local::getNameOpt(snapName),
        local::getPermissionOpt(isWritable),
        lvPath.str()
    };
    cybozu::process::call("/sbin/lvcreate", args);

    cybozu::FilePath snapPath = getLvmPath(vgName, snapName);
    local::waitForDeviceAvailable(snapPath);

    Lv snap = locate(snapPath.str());
    if (snap.snapName() == snapName && snap.isSnapshot() && snap.lvName() == lvName &&
        snap.isThinVolume() && snap.attr().isTypeThinVolume()) {
        return snap;
    }
    throw cybozu::Exception(__func__)
        << "failed" << vgName << lvName << snapName << isWritable;
}

/**
 * Rename a volume or snapshot.
 * "lvrename vg oldlv newlv"
 */
inline void renameLv(
    const std::string &vgName,
    const std::string &oldLvName, const std::string &newLvName)
{
    cybozu::process::call("/sbin/lvrename", { vgName, oldLvName, newLvName });
}

/**
 * Remove a volume or a snapshot.
 */
inline void remove(const std::string &pathStr)
{
    cybozu::FilePath path(pathStr);
    if (!path.stat().exists()) {
        throw cybozu::Exception(__func__) << "not found" << pathStr;
    }

    cybozu::process::call("/sbin/lvremove", { "-f", path.str() });
    local::sleepMs(100); /* for safety. */
}

/**
 * Resize a volume.
 */
inline void resize(const std::string &pathStr, uint64_t newSizeLb)
{
    cybozu::process::call("/sbin/lvresize", {
        "-f", /* force volume shrink */
        local::getSizeOpt(newSizeLb),
        pathStr
    });
    local::sleepMs(100); /* for safety. */
}

/**
 * RETURN:
 *   Volume list including snapshots.
 * @arg "" or vgName or volumePath.
 */
inline LvList listLv(const std::string &arg = "")
{
    LvList list;
    std::vector<std::string> args;
    if (!arg.empty()) args.push_back(arg);
    const std::string result
        = local::callLvm("/sbin/lvs", "lv_name,origin,lv_size,vg_name,pool_lv,lv_attr", args);
    for (const std::string &s0 : local::splitAndTrim(result, '\n')) {
        if (s0.empty()) continue; /* last '\n' */
        const std::vector<std::string> v = local::splitAndTrim(s0, ',');
        if (v.size() != 6) {
            throw cybozu::Exception(__func__) << "invalid output" << s0;
        }
        const bool isSnapshot = !v[1].empty();
        const std::string &lvName = isSnapshot ? v[1] : v[0];
        const std::string &snapName = isSnapshot ? v[0] : "";
        const uint64_t sizeLb = local::parseSizeLb(v[2]);
        const std::string &vgName = v[3];
        const std::string &poolName = v[4];
        LvAttr attr;
        attr.set(v[5]);

        list.emplace_back(vgName, lvName, snapName, sizeLb, poolName, attr);
    }
    return list;
}

/**
 * RETURN:
 *   Volume list not including snapshots.
 * @arg "" or vgName or volumePath.
 */
inline LvMap getLvMap(const std::string &arg)
{
    LvMap map;
    for (Lv &lv : listLv(arg)) {
        if (lv.isSnapshot()) continue;
        auto pair = map.emplace(lv.name(), lv);
        if (!pair.second) assert(false);
    }
    return map;
}

/**
 * RETURN:
 *   True when the volume with the name exists.
 */
inline bool exists(const std::string &vgName, const std::string &name)
{
    return getLvmPath(vgName, name).stat().exists();
}

inline bool lvExists(const std::string &vgName, const std::string &lvName)
{
    if (!exists(vgName, lvName)) return false;
    return !locate(vgName, lvName).isSnapshot();
}

inline bool snapExists(const std::string &vgName, const std::string &snapName)
{
    if (!exists(vgName, snapName)) return false;
    return locate(vgName, snapName).isSnapshot();
}

/**
 * Find snapshots of a lv volume.
 */
inline LvList getAllSnapshots(const std::string &vgName, const std::string &lvName)
{
    LvList list;
    for (Lv &lv : listLv(vgName)) {
        if (lv.isSnapshot() && lv.lvName() == lvName) {
            list.push_back(lv);
        }
    }
    return list;
}

/**
 * Get logical volume object using an argument.
 * @arg 'vgName/lvName' or '/dev/vgName/lvName'.
 */
inline Lv locate(const std::string &arg)
{
    LvList list = listLv(arg);
    if (list.empty()) {
        throw cybozu::Exception(__func__) << "failed to detect LV" << arg;
    }
    return list.front();
}

inline Lv locate(const std::string &vgName, const std::string &name)
{
    return locate(getLvmPath(vgName, name).str());
}

/**
 * RETURN:
 *   volume group list.
 */
inline VgList listVg(const std::string &arg = "")
{
    VgList list;
    std::vector<std::string> args;
    if (!arg.empty()) args.push_back(arg);
    std::string result
        = local::callLvm("/sbin/vgs", "vg_name,vg_size,vg_free", args);
    for (const std::string &s0 : local::splitAndTrim(result, '\n')) {
        if (s0.empty()) continue;
        std::vector<std::string> v = local::splitAndTrim(s0, ',');
        if (v.size() != 3) {
            throw cybozu::Exception(__func__) << "invalid output" << s0;
        }
        std::string vgName = v[0];
        uint64_t sizeLb = local::parseSizeLb(v[1]);
        uint64_t freeLb = local::parseSizeLb(v[2]);
        list.push_back(Vg(vgName, sizeLb, freeLb));
    }
    return list;
}

inline Vg getVg(const std::string &vgName)
{
    VgList vgs = listVg(vgName);
    if (vgs.empty()) throw cybozu::Exception(__func__) << "not found" << vgName;
    assert(vgs.front().name() == vgName);
    return vgs.front();
}

inline bool vgExists(const std::string &vgName)
{
    VgList vgs = listVg(vgName);
    if (vgs.empty()) return false;
    assert(vgs.front().name() == vgName);
    return true;
}

inline LvAttr getLvAttr(const std::string lvPathStr)
{
    LvAttr attr;
    const std::string result
        = local::callLvm("/sbin/lvs", "lv_attr", {lvPathStr});
    size_t i = 0;
    for (const std::string &s0 : local::splitAndTrim(result, '\n')) {
        if (s0.empty()) continue; /* last '\n' */
        attr.set(s0);
        i++;
    }
    if (i != 1) throw cybozu::Exception(__func__) << "result is not oneline" << i << result;
    return attr;
}

inline bool tpExists(const std::string &vgName, const std::string &poolName)
{
    if (!exists(vgName, poolName)) return false;
    return locate(vgName, poolName).attr().isTypeThinpool();
}

}} //namespace cybozu::lvm
