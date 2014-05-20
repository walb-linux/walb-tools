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
#include <exception>
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
#include "fileio.hpp"
#include "file_path.hpp"
#include "process.hpp"

namespace cybozu {
namespace lvm {

const unsigned int LBS = 512;

class Lv;
class Vg;

using LvMap = std::map<std::string, Lv>;
using LvList = std::vector<Lv>;
using VgList = std::vector<Vg>;

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
 * @lvmCommand lvm binary path like "/usr/bin/lvm".
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
        throw std::runtime_error("size must be multiples of logical block size.");
    }
    return size / LBS;
}

/**
 * Create lvm size option.
 */
inline std::string getSizeOpt(uint64_t sizeLb) {
    return std::string("--size=") + cybozu::itoa(sizeLb * LBS) + "b";
}

/**
 * RETURN:
 *   true when available,
 *   false in timeout.
 */
inline bool waitForDeviceAvailable(cybozu::FilePath &path)
{
    const size_t timeoutMs = 5000;
    const size_t intervalMs = 100;
    for (size_t i = 0; i < timeoutMs / intervalMs; i++) {
        if (isDeviceAvailable(path)) return true;
        local::sleepMs(intervalMs);
    }
    return false;
}

} //namespace local

/**
 * Prototypes.
 */
cybozu::FilePath getLvmPath(const std::string &vgName, const std::string &name);
Lv createLv(const std::string &vgName, const std::string &lvName, uint64_t sizeLb);
Lv createSnapshot(
    const std::string &vgName, const std::string &lvName, const std::string &snapName,
    bool isWritable, uint64_t sizeLb);
void remove(const std::string &pathStr);
void resize(const std::string &pathStr, uint64_t newSizeLb);
LvList listLv(const std::string &arg);
LvMap getLvMap(const std::string &arg);
bool exists(const std::string &vgName, const std::string &name);
bool lvExists(const std::string &vgName, const std::string &lvName);
bool snapExists(const std::string &vgName, const std::string &snapName);
LvList find(const std::string &vgName, const std::string &name);
LvList findLv(const std::string &vgName, const std::string &lvName);
LvList findSnap(const std::string &vgName, const std::string &snapName);
Lv locate(const std::string &lvPathStr);
Lv locate(const std::string &vgName, const std::string &name);
VgList listVg();
Vg getVg(const std::string &vgName);
bool vgExists(const std::string &vgName);

/**
 * Logical volume manager.
 */
class Lv
{
private:
    std::string vgName_; /* volume group name. */
    std::string lvName_; /* logical volume name. */
    std::string snapName_; /* snapshot name. "" if isSnaphsot_ is false. */
    uint64_t sizeLb_; /* [logical block]. */
    bool isSnapshot_;
public:
    Lv() {
        throw std::runtime_error("default constructor invalid.");
    }
    Lv(const std::string &vgName, const std::string &lvName,
                  const std::string &snapName, uint64_t sizeLb, bool isSnapshot)
        : vgName_(vgName), lvName_(lvName), snapName_(snapName)
        , sizeLb_(sizeLb), isSnapshot_(isSnapshot) {
    }
    const std::string &vgName() const { return vgName_; }
    const std::string &lvName() const { return lvName_; }
    const std::string &snapName() const { return snapName_; }
    const std::string &name() const {
        return isSnapshot() ? snapName() : lvName();
    }
    uint64_t sizeLb() const { return sizeLb_; }
    bool isSnapshot() const { return isSnapshot_; }
    cybozu::FilePath path() const {
        return getLvmPath(vgName_, name());
    }
    bool exists() const {
        return cybozu::lvm::exists(vgName(), name());
    }
    Lv takeSnapshot(const std::string &snapName, bool isWritable = false, uint64_t sizeLb = 0) const {
        checkVolume();
        if (sizeLb == 0 || sizeLb_ < sizeLb / 2) sizeLb = sizeLb_;
        return createSnapshot(vgName_, lvName_, snapName, isWritable, sizeLb);
    }
    /**
     * @snapName specify an empty string for wildcard.
     */
    bool hasSnapshot(const std::string &snapName = "") const {
        checkVolume();
        for (Lv &lv : listLv(vgName())) {
            if (lv.isSnapshot() &&
                lv.lvName() == lvName() &&
                (snapName.empty() || snapName == lv.snapName())) {
                return true;
            }
        }
        return false;
    }
    LvList snapshotList() const {
        checkVolume();
        LvList v;
        for (Lv &lv : listLv(vgName())) {
            if (lv.isSnapshot() && lv.lvName() == lvName()) {
                v.push_back(lv);
            }
        }
        return v;
    }
    Lv getSnapshot(const std::string &snapName) const {
        for (Lv &lv : snapshotList()) {
            if (lv.snapName() == snapName) {
                return lv;
            }
        }
        throw std::runtime_error("Not found.");
    }
    Lv parent() const {
        checkSnapshot();
        for (Lv &lv : listLv(vgName())) {
            if (!lv.isSnapshot() && lv.lvName() == lvName()) {
                return lv;
            }
        }
        throw std::runtime_error("Every snapshot must have parent logical volume.");
    }
    void resize(uint64_t newSizeLb) {
        cybozu::lvm::resize(path().str(), newSizeLb);
        sizeLb_ = newSizeLb;
    }
    void remove() {
        cybozu::lvm::remove(path().str());
    }
    template <typename OutputStream>
    void print(OutputStream &os) const {
        os << vgName_ << "/" << name() << " sizeLb " << sizeLb_ << " snapshot "
           << isSnapshot_ << " (" << lvName_ << ")" << std::endl;
    }
    void print(::FILE *fp) const {
        std::stringstream ss;
        print(ss);
        std::string s(ss.str());
        if (::fwrite(&s[0], 1, s.size(), fp) < s.size()) {
            throw std::runtime_error("fwrite failed.");
        }
        if (::fflush(fp) != 0) {
            throw std::runtime_error("fflush failed.");
        }
    }
    void print() const { print(::stdout); }
private:
    void checkVolume() const {
        if (isSnapshot_) {
            throw std::logic_error("This must be logical volume.");
        }
    }
    void checkSnapshot() const {
        if (!isSnapshot_) {
            throw std::logic_error("This must be snapshot.");
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
    Vg() {
        throw std::runtime_error("default constructor invalid.");
    }
    Vg(const std::string &vgName, uint64_t sizeLb, uint64_t freeLb)
        : vgName_(vgName), sizeLb_(sizeLb), freeLb_(freeLb) {
    }
    Lv create(const std::string &lvName, uint64_t sizeLb) {
        if (freeLb_ < sizeLb) {
            throw std::runtime_error("VG free size not enough.");
        }
        Lv lv = createLv(vgName_, lvName, sizeLb);
        freeLb_ -= sizeLb;
        return lv;
    }
    const std::string &name() const { return vgName_; }
    uint64_t sizeLb() const { return sizeLb_; }
    uint64_t freeLb() const { return freeLb_; }
    void print(::FILE *fp) const {
        ::fprintf(
            fp,
            "vg %s sizeLb %" PRIu64 " freeLb %" PRIu64 "\n"
            , vgName_.c_str(), sizeLb_, freeLb_);
    }
    void print() const { print(::stdout); }
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
Lv createLv(const std::string &vgName, const std::string &lvName, uint64_t sizeLb)
{
    cybozu::process::call("/sbin/lvcreate", {
        std::string("--name=") + lvName,
        local::getSizeOpt(sizeLb),
        vgName
    });

    cybozu::FilePath lvPath = getLvmPath(vgName, lvName);
    if (local::waitForDeviceAvailable(lvPath)) {
        return Lv(vgName, lvName, "", sizeLb, false);
    }
    /* creation failed. */
    throw std::runtime_error("LV seems invalid: abort.");
}

/**
 * Create a snapshot.
 * @sizeLb data size for snapshot area [logical block].
 */
Lv createSnapshot(
    const std::string &vgName, const std::string &lvName, const std::string &snapName,
    bool isWritable, uint64_t sizeLb)
{
    cybozu::FilePath lvPath = getLvmPath(vgName, lvName);
    cybozu::process::call("/sbin/lvcreate", {
        "-s",
        local::getSizeOpt(sizeLb),
        std::string("--name=") + snapName,
        "-p", isWritable ? "rw" : "r",
        lvPath.str()
    });

    cybozu::FilePath snapPath = getLvmPath(vgName, snapName);
    if (local::waitForDeviceAvailable(snapPath)) {
        return Lv(vgName, lvName, snapName, sizeLb, true);
    }
    /* possible invalid snapshot. */
    throw std::runtime_error("Snapshot seems invalid; abort.");
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
        throw std::runtime_error("not found.");
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

/* now editing */

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
        = local::callLvm("/sbin/lvs", "lv_name,origin,lv_size,vg_name", args);
    for (const std::string &s0 : local::splitAndTrim(result, '\n')) {
        if (s0.empty()) continue; /* last '\n' */
        const std::vector<std::string> v = local::splitAndTrim(s0, ',');
        if (v.size() != 4) {
            throw std::runtime_error("invalid output of lvs.");
        }
        const bool isSnapshot = !v[1].empty();
        const std::string &lvName = isSnapshot ? v[1] : v[0];
        const std::string &snapName = isSnapshot ? v[0] : "";
        const uint64_t sizeLb = local::parseSizeLb(v[2]);
        const std::string &vgName = v[3];

        list.push_back(Lv(vgName, lvName, snapName, sizeLb, isSnapshot));
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
    return !find(vgName, name).empty();
}

inline bool lvExists(const std::string &vgName, const std::string &lvName)
{
    return !findLv(vgName, lvName).empty();
}

inline bool snapExists(const std::string &vgName, const std::string &snapName)
{
    return !findSnap(vgName, snapName).empty();
}

/**
 * Find logical volume objects with a name.
 *
 * @vgName vg identifier. "" means wildcard.
 * @name lv or snapshot name. you must specify this.
 * RETURN:
 *   logical volume or snapshot list.
 */
inline LvList find(const std::string &vgName, const std::string &name)
{
    LvList list;
    for (Lv &lv : listLv(vgName)) {
        if (lv.name() == name) list.push_back(lv);
    }
    return list;
}

/**
 * Find logical volumes.
 */
inline LvList findLv(const std::string &vgName, const std::string &lvName)
{
    LvList list;
    for (Lv &lv : listLv(vgName)) {
        if (!lv.isSnapshot() && lv.lvName() == lvName) {
            list.push_back(lv);
        }
    }
    return list;
}

/**
 * Find snapshots.
 */
inline LvList findSnap(const std::string &vgName, const std::string &snapName)
{
    LvList list;
    for (Lv &lv : listLv(vgName)) {
        if (lv.isSnapshot() && lv.snapName() == snapName) {
            list.push_back(lv);
        }
    }
    return list;
}

/**
 * Get logical volume object using a device path.
 */
inline Lv locate(const std::string &pathStr)
{
    LvList list = listLv(pathStr);
    if (list.empty()) {
        throw std::runtime_error("failed to detect LV " + pathStr);
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
inline VgList listVg()
{
    VgList list;
    std::string result
        = local::callLvm("/sbin/vgs", "vg_name,vg_size,vg_free");
    for (const std::string &s0 : local::splitAndTrim(result, '\n')) {
        if (s0.empty()) continue;
        std::vector<std::string> v = local::splitAndTrim(s0, ',');
        if (v.size() != 3) {
            throw std::runtime_error("invalid output of vgs.");
        }
        std::string vgName = v[0];
        uint64_t sizeLb = local::parseSizeLb(v[1]);
        uint64_t freeLb = local::parseSizeLb(v[2]);
        list.push_back(Vg(vgName, sizeLb, freeLb));
    }
    return list;
}

/**
 * Get a volume group.
 */
inline Vg getVg(const std::string &vgName)
{
    for (Vg &vg : listVg()) {
        if (vg.name() == vgName) return vg;
    }
    throw std::runtime_error("VG not found.");
}

inline bool vgExists(const std::string &vgName)
{
    for (Vg &vg : listVg()) {
        if (vg.name() == vgName) return true;
    }
    return false;
}

}} //namespace cybozu::lvm
