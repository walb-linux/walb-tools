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
#include <sstream>
#include <thread>
#include <chrono>
#include "cybozu/file.hpp"
#include "cybozu/atoi.hpp"
#include "cybozu/itoa.hpp"
#include "fileio.hpp"
#include "file_path.hpp"
#include "process.hpp"

#ifndef CYBOZU_LVM_HPP
#define CYBOZU_LVM_HPP

namespace cybozu {
namespace lvm {

const std::string VOLUME_PREFIX = "i_";
const std::string SNAP_PREFIX = "snap_";
const std::string RESTORE_PREFIX = "r_";
const uint64_t MAX_SNAPSHOT_SIZE = 1024ULL << 30; /* 1 TiB. */
const unsigned int LBS = 512;

class LogicalVolume;
class VolumeGroup;

using LvMap = std::map<std::string, LogicalVolume>;
using LvList = std::vector<LogicalVolume>;
using VgList = std::vector<VolumeGroup>;

namespace {

static cybozu::FilePath getLvmPath(
    const std::string &vgName, const std::string &prefix, const std::string &name)
{
    return cybozu::FilePath("/dev") + cybozu::FilePath(vgName)
        + cybozu::FilePath(prefix + name);
}

static bool isDeviceAvailable(const cybozu::FilePath &path) {
    if (!path.stat().exists()) return false;

    std::vector<std::string> args;
    args.push_back("info");
    args.push_back("-c");
    args.push_back("--noheadings");
    args.push_back("-o");
    args.push_back("Open");
    args.push_back(path.str());

    std::string s0 = cybozu::process::call("/sbin/dmsetup", args);
    std::string s1 = cybozu::util::trimSpace(s0);
    uint64_t i = cybozu::atoi(s1);
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
std::string callLvm(
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

void sleepMs(unsigned int ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

bool hasPrefix(const std::string &name, const std::string &prefix)
{
    return name.substr(0, prefix.size()) == prefix;
}

/**
 * @name like "XXX_YYYYY"
 * @base like "YYYYY"
 * RETURN:
 *   prefix like "XXX_".
 */
std::string getPrefix(const std::string &name, const std::string &base)
{
    size_t s0 = name.size();
    size_t s1 = base.size();
    if (s0 <= s1) {
        throw std::runtime_error("There is no prefix.");
    }
    if (name.substr(s0 - s1) != base) {
        throw std::runtime_error("Base name differs.");
    }
    return name.substr(0, s0 - s1);
}

/**
 * @s size string [byte].
 * RETURN:
 *   parsed size [logical block].
 */
uint64_t parseSizeLb(const std::string &s) {
    uint64_t size = cybozu::atoi(s);
    if (size % LBS != 0) {
        throw std::runtime_error("size must be multiples of logical block size.");
    }
    return size / LBS;
}

/**
 * Create lvm size option.
 */
std::string getSizeOpt(uint64_t sizeLb) {
    std::string opt("--size=");
    opt += cybozu::itoa(sizeLb * LBS);
    opt += "b";
    return std::move(opt);
}

/**
 * RETURN:
 *   true when available,
 *   false in timeout.
 */
bool waitForDeviceAvailable(cybozu::FilePath &path)
{
    for (int i = 0; i < 10; i++) {
        if (isDeviceAvailable(path)) return true;
        sleepMs(1000);
    }
    return false;
}

} //namespace anonumous

/**
 * Prototypes.
 */
LogicalVolume createLv(const std::string &vgName, const std::string &name, uint64_t sizeLb);
LogicalVolume createSnapshot(
    const std::string &vgName, const std::string &prefix, const std::string &name,
    uint64_t sizeLb);
void removeLv(const std::string &pathStr);
void removeLvAll(const std::string &vgName, const std::string &name);
void resizeLv(const std::string &pathStr, uint64_t newSizeLb);
LvList listLv(const std::string &arg);
LvMap getLvMap(const std::string &arg);
LvList findLv(const std::string &name, bool isSnapshot);
LogicalVolume locateLv(const std::string &volPathStr);
VgList listVg();
VolumeGroup findVg(const std::string &vgName);

/**
 * Logical volume manager.
 */
class LogicalVolume
{
private:
    std::string vgName_;
    std::string prefix_;
    std::string name_;
    uint64_t sizeLb_; /* [logical block]. */
    bool isSnapshot_;
public:
    LogicalVolume() {
        throw std::runtime_error("default constructor invalid.");
    }
    LogicalVolume(const std::string &vgName, const std::string &prefix,
                  const std::string &name,
                  uint64_t sizeLb, bool isSnapshot)
        : vgName_(vgName), prefix_(prefix), name_(name)
        , sizeLb_(sizeLb), isSnapshot_(isSnapshot) {
    }
    const std::string &name() const { return name_; }
    const std::string &vgName() const { return vgName_; }
    uint64_t sizeLb() const { return sizeLb_; }
    bool isSnapshot() const { return isSnapshot_; }
    cybozu::FilePath path() const {
        return getLvmPath(vgName_, prefix_, name_);
    }
    LogicalVolume snapshot(const std::string &prefix) const {
        checkVolume();
        return createSnapshot(vgName_, prefix, name_, sizeLb_);
    }
    bool hasSnapshot(const std::string &prefix) const {
        checkVolume();
        return getLvmPath(vgName_, prefix, name_).stat().exists();
    }
    LvList snapshotList() const {
        checkVolume();
        LvList v;
        for (LogicalVolume &lv : listLv(vgName())) {
            if (lv.name() == name_ && lv.isSnapshot()) {
                v.push_back(lv);
            }
        }
        return std::move(v);
    }
    void resize(uint64_t newSizeLb) {
        resizeLv(path().str(), newSizeLb);
        sizeLb_ = newSizeLb;
    }
    void remove() {
        removeLv(path().str());
    }
    void print(::FILE *fp) const {
        ::fprintf(
            fp,
            "%s/%s%s sizeLb %" PRIu64 " snapshot %d\n"
            , vgName_.c_str(), prefix_.c_str(), name_.c_str()
            , sizeLb_, isSnapshot_);
    }
    void print() const { print(::stdout); }
private:
    void checkVolume() const {
        if (isSnapshot_) {
            throw std::logic_error("tried to create a snapshot of a snapshot.");
        }
    }
};

/**
 * Volume group manager.
 */
class VolumeGroup
{
private:
    std::string vgName_;
    uint64_t sizeLb_;
    uint64_t freeLb_;

public:
    VolumeGroup() {
        throw std::runtime_error("default constructor invalid.");
    }
    VolumeGroup(const std::string &vgName, uint64_t sizeLb, uint64_t freeLb)
        : vgName_(vgName), sizeLb_(sizeLb), freeLb_(freeLb) {
    }
    LogicalVolume create(const std::string &name, uint64_t sizeLb) {
        if (freeLb_ < sizeLb) {
            throw std::runtime_error("VG free size not enough.");
        }
        LogicalVolume lv = createLv(vgName_, name, sizeLb);
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
 * Create a volume.
 */
LogicalVolume createLv(const std::string &vgName, const std::string &name, uint64_t sizeLb)
{
    std::vector<std::string> args;
    args.push_back("--name=" + VOLUME_PREFIX + name);
    args.push_back(getSizeOpt(sizeLb));
    args.push_back(vgName);
    cybozu::process::call("/sbin/lvcreate", args);

    cybozu::FilePath lvPath = getLvmPath(vgName, VOLUME_PREFIX, name);
    if (waitForDeviceAvailable(lvPath)) {
        return LogicalVolume(vgName, VOLUME_PREFIX, name, sizeLb, false);
    }
    /* creation failed. */
    throw std::runtime_error("Lv seems invalid: abort.");
}

/**
 * Create a snapshot.
 */
LogicalVolume createSnapshot(
    const std::string &vgName, const std::string &prefix,
    const std::string &name, uint64_t sizeLb)
{
    uint64_t snapSizeLb = std::min(sizeLb, MAX_SNAPSHOT_SIZE);
    std::vector <std::string> args;
    args.push_back("-s");
    args.push_back(getSizeOpt(snapSizeLb));
    args.push_back("--name=" + prefix + name);
    cybozu::FilePath lvPath = getLvmPath(vgName, VOLUME_PREFIX, name);
    args.push_back(lvPath.str());
    cybozu::process::call("/sbin/lvcreate", args);

    cybozu::FilePath snapPath = getLvmPath(vgName, prefix, name);
    if (waitForDeviceAvailable(snapPath)) {
        return LogicalVolume(vgName, prefix, name, snapSizeLb, true);
    }
    /* possible invalid snapshot. */
    throw std::runtime_error("Snapshot seems invalid; abort.");
}

/**
 * Remove a volume or a snapshot.
 */
void removeLv(const std::string &pathStr)
{
    cybozu::FilePath path(pathStr);
    if (!path.stat().exists()) {
        throw std::runtime_error("not found.");
    }

    std::vector<std::string> args;
    args.push_back("-f");
    args.push_back(path.str());
    cybozu::process::call("/sbin/lvremove", args);
    sleepMs(1000); /* for safety. */
}

/**
 * Remove a volume and its all snapshots.
 */
void removeLvAll(const std::string &vgName, const std::string &name)
{
    cybozu::FilePath path = getLvmPath(vgName, VOLUME_PREFIX, name);
    LogicalVolume lv = locateLv(path.str());
    for (LogicalVolume &snap : lv.snapshotList()) {
        removeLv(snap.path().str());
    }
    removeLv(path.str());
}

/**
 * Resize a volume.
 */
void resizeLv(const std::string &pathStr, uint64_t newSizeLb)
{
    std::vector<std::string> args;
    args.push_back("-f"); /* force volume shrink */
    args.push_back(getSizeOpt(newSizeLb));
    args.push_back(pathStr);
    cybozu::process::call("/sbin/lvcreate", args);
    sleepMs(1000); /* for safety. */
}

/**
 * RETURN:
 *   Volume list including snapshots.
 * @arg vgName or volumePath.
 */
LvList listLv(const std::string &arg = "")
{
    LvList list;
    std::vector<std::string> args;
    if (!arg.empty()) args.push_back(arg);
    std::string result
        = callLvm("/sbin/lvs", "lv_name,origin,lv_size,vg_name", args);
    for (const std::string &s0 : cybozu::util::splitString(result, "\n")) {
        std::vector<std::string> v = cybozu::util::splitString(s0, ",");
        if (v.size() != 4) {
            throw std::runtime_error("invalid output from lvs.");
        }
        bool isSnapshot = !v[1].empty();
        std::string volName = isSnapshot ? v[1] : v[0];
        if (!hasPrefix(volName, VOLUME_PREFIX)) continue;
        uint64_t sizeLb = parseSizeLb(v[2]);
        std::string name = volName.substr(VOLUME_PREFIX.size());
        std::string prefix = isSnapshot ? getPrefix(v[0], name) : VOLUME_PREFIX;
        std::string &vgName = v[3];

        list.push_back(LogicalVolume(vgName, prefix, name, sizeLb, isSnapshot));
    }
    return std::move(list);
}

/**
 * RETURN:
 *   Volume list not including snapshots.
 * @arg vgName or volumePath.
 */
LvMap getLvMap(const std::string &arg)
{
    LvMap map;
    for (LogicalVolume &lv : listLv(arg)) {
        if (lv.isSnapshot()) continue;
        auto pair = map.insert(std::make_pair(lv.name(), lv));
        if (!pair.second) assert(false);
    }
    return std::move(map);
}

/**
 * Find logical volume objects with a name.
 *
 * @name volume identifier.
 * @isSnapshot
 * RETURN:
 *   if isSnapshot is true, only snapshots will be added.
 *   else, only volumes will be added.
 */
LvList findLv(const std::string &name, bool isSnapshot = false)
{
    LvList list;
    for (LogicalVolume &lv : listLv()) {
        if (lv.name() == name && isSnapshot == lv.isSnapshot()) {
            list.push_back(lv);
        }
    }
    return std::move(list);
}

/**
 * Get logical volume object using a device path.
 */
LogicalVolume locateLv(const std::string &volPathStr)
{
    LvList list = listLv(volPathStr);
    if (list.empty()) {
        throw std::runtime_error("failed to detect LV " + volPathStr);
    }
    return list.front();
}

/**
 * RETURN:
 *   volume group list.
 */
VgList listVg()
{
    VgList list;
    std::string result
        = callLvm("/sbin/vgs", "vg_name,vg_size,vg_free");
    for (const std::string &s0 : cybozu::util::splitString(result, "\n")) {
        std::vector<std::string> v = cybozu::util::splitString(s0, ",");
        if (v.size() != 3) {
            throw std::runtime_error("failed to detect VG.");
        }
        std::string vgName = v[0];
        uint64_t sizeLb = parseSizeLb(v[1]);
        uint64_t freeLb = parseSizeLb(v[2]);
        list.push_back(VolumeGroup(vgName, sizeLb, freeLb));
    }
    return std::move(list);
}

/**
 * Find a volume group.
 */
VolumeGroup findVg(const std::string &vgName)
{
    for (VolumeGroup &vg : listVg()) {
        if (vg.name() == vgName) return vg;
    }
    throw std::runtime_error("VG not found.");
}

}} //namespace cybozu::lvm

#endif /* CYBOZU_LVM_HPP */
