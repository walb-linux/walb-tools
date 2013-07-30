/**
 * @file
 * @brief lvm manager.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <exception>
#include <string>
#include <map>
#include <sstream>
#include "cybozu/file.hpp"
#include "fileio.hpp"
#include "file_path.hpp"
#include "process.hpp"
// #include "cybozu/process.hpp"

#ifndef CYBOZU_LVM_HPP
#define CYBOZU_LVM_HPP

namespace cybozu {

namespace lvm {

const std::string VOLUME_PREFIX = "i_";

cybozu::FilePath getLvmPath(const std::string &vgName, const std::string &name)
{
    return cybozu::FilePath("/dev") + cybozu::FilePath(vgName)
        + cybozu::FilePath(VOLUME_PREFIX + name);
}

/**
 * Call lvm command.
 *
 * @lvmCommand lvm binary path like "/usr/bin/lvm".
 * @options options like "lv_name,lv_size,origin,vg_name".
 * @args another arguments.
 * @result result will be filled.
 */
void callLvm(const std::string &options,
             const std::vector<std::string> &args,
             std::string &result)
{
    std::vector<std::string> args0 = {
        "--units=b", "--nosuffix", "--options=" + options,
        "--separator=,", "--noheadings", "--unbuffered",
    };
    args0.insert(args0.end(), args.cbegin(), args.cend());
    result = cybozu::process::call("/usr/bin/lvm", args0);
}

/**
 * Logical volume.
 */
class LogicalVolume
{
private:
    std::string name_;
    std::string vgName_;
    uint64_t sizeLb_; /* [logical block]. */
    bool isSnapshot_;
    cybozu::FilePath path_;
public:
    const std::string &name() const { return name_; }
    const std::string &vgName() const { return vgName_; }
    uint64_t sizeLb() const { return sizeLb_; }
    bool isSnapshot() const { return isSnapshot_; }
    const cybozu::FilePath &path() const { return path_; }

    LogicalVolume snapshot(const std::string &prefix) const {
        /* now editing */
        return LogicalVolume();
    }
    bool hasSnapshot(const std::string &prefix) const {
        /* now editing */
        return false;
    }
    void resize(uint64_t newSizeLb) {
        /* now editing */
    }
    void remove() {
        /* now editing */
    }
    static LogicalVolume find(const std::string &name) {
        /* now editing */
        return LogicalVolume();
    }
    LogicalVolume() : path_("") {
        throw std::runtime_error("default constructor invalid.");
    }
    LogicalVolume(const std::string &name, const std::string &vgName,
                  uint64_t sizeLb, bool isSnapshot, const cybozu::FilePath &path)
        : name_(name), vgName_(vgName), sizeLb_(sizeLb)
        , isSnapshot_(isSnapshot), path_(path) {
    }
private:
};

class VolumeGroup
{
private:
    std::string vgName_;
    uint64_t sizeLb_;
    uint64_t freeLb_;

public:
    /**
     * Auto detect the volume.
     */
    VolumeGroup() {
        /* now editing */
    }
    /**
     * Use a volume group.
     */
    explicit VolumeGroup(const std::string &vgName) {
        /* now editing */
    }
    std::map<std::string, LogicalVolume> list() const {
        /* now editing */
        return {};
    }
    LogicalVolume create(const std::string &name, uint64_t sizeLb) {
        /* now editing */
        return LogicalVolume();
    }
    const std::string &name() const { return vgName_; }
    uint64_t sizeLb() const { return sizeLb_; }
    uint64_t freeLb() const { return freeLb_; }
};

}} //namespace cybozu::lvm

#endif /* CYBOZU_LVM_HPP */
