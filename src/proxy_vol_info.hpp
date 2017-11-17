#pragma once
#include <string>
#include <vector>
#include "util.hpp"
#include "walb_util.hpp"
#include "file_path.hpp"
#include "host_info.hpp"
#include "meta.hpp"
#include "atomic_map.hpp"
#include "wdiff_data.hpp"
#include "proxy_constant.hpp"

namespace walb {

/**
 * Data manager for a volume in a proxy daemon.
 * This is not thread-safe.
 */
class ProxyVolInfo
{
public:
    const cybozu::FilePath volDir;
    const std::string volId;
private:
    MetaDiffManager &diffMgr_;
    AtomicMap<MetaDiffManager> &diffMgrMap_;
    std::set<std::string> &archiveSet_;

public:
    /**
     * @baseDirStr base directory path string.
     * @volId volume identifier.
     */
    ProxyVolInfo(const std::string &baseDirStr, const std::string &volId,
                 MetaDiffManager &diffMgr, AtomicMap<MetaDiffManager> &diffMgrMap,
                 std::set<std::string> &archiveSet)
        : volDir(cybozu::FilePath(baseDirStr) + volId)
        , volId(volId)
        , diffMgr_(diffMgr), diffMgrMap_(diffMgrMap)
        , archiveSet_(archiveSet) {
        cybozu::FilePath baseDir(baseDirStr);
        if (!baseDir.stat().isDirectory()) {
            throw cybozu::Exception("ProxyVolInfo:Directory not found") << baseDirStr;
        }
        if (volId.empty()) {
            throw cybozu::Exception("ProxyVolInfo:volId is empty");
        }
    }
    /**
     * Create volume directory
     */
    void init() {
        util::makeDir(volDir.str(), "ProxyVolInfo::init:makdir failed", true);
        setSizeLb(0);
        util::makeDir(getReceivedDir().str(), "ProxyVolInfo::init:makedir failed", true);
        util::makeDir(getSendtoDir().str(), "ProxyVolInfo::init:makedir failed", true);
    }
    /**
     * Load wdiff meta data for received and each sendto directory.
     */
    void loadAllArchiveInfo() {
        reloadReceivedWdiffs();
        for (const std::string &name : getArchiveNameList()) {
            archiveSet_.insert(name);
            reloadSendtoWdiffs(name);
        }
    }
    bool notExistsArchiveInfo() const {
        return archiveSet_.empty();
    }
    bool existsArchiveInfo(const std::string &name) const;
    void addArchiveInfo(const std::string& name, const HostInfoForBkp &hi, bool ensureNotExistance) {
        util::saveFile(volDir, name + ArchiveSuffix, hi);
        util::makeDir(getSendtoDir(name).str(),
                      "ProxyVolInfo::addArchiveInfo", ensureNotExistance);
        archiveSet_.insert(name);
#if 1
        // Mgr must be empty here. But some bugs exist so we must delete garbages.
        MetaDiffManager &mgr = diffMgrMap_.get(name);
        if (ensureNotExistance && !mgr.empty()) {
            LOGs.warn() << "MetaDiffManager is not empty" << volId << name;
            for (const MetaDiff& d : mgr.getAll()) {
                LOGs.info() << "diff" << d;
            }
            mgr.clear();
        }
#endif
    }
    void deleteArchiveInfo(const std::string &name) {
        diffMgrMap_.get(name).clear();
        getSendtoDir(name).rmdirRecursive();
        getArchiveInfoPath(name).remove();
        archiveSet_.erase(name);
    }
    HostInfoForBkp getArchiveInfo(const std::string &name) const {
        HostInfoForBkp hi;
        util::loadFile(volDir, name + ArchiveSuffix, hi);
        return hi;
    }
    /**
     * CAUSION:
     *   The volume will be removed if exists.
     *   All data inside the directory will be removed.
     */
    void clear();
    void setSizeLb(uint64_t sizeLb) {
        util::saveFile(volDir, "size", sizeLb);
    }
    uint64_t getSizeLb() const {
        uint64_t sizeLb;
        util::loadFile(volDir, "size", sizeLb);
        return sizeLb;
    }
    bool existsVolDir() const {
        return volDir.stat().isDirectory();
    }
    /**
     * Get diff list to send.
     *
     * @name archive host name.
     * @size maximum total size [byte].
     * @nr   maximum number of wdiffs.
     *
     * RETURN:
     *   MetaDiff list that can be merged.
     *   which will be sent to the server.
     */
    MetaDiffVec getDiffListToSend(const std::string &archiveName, uint64_t size, size_t nr) const {
        MetaDiffManager &mgr = diffMgrMap_.get(archiveName);
        WalbDiffFiles wdiffs(mgr, getSendtoDir(archiveName).str());
        return wdiffs.getDiffListToSend(size, nr);
    }
    MetaDiffVec getAllDiffsInReceivedDir() const {
        return diffMgr_.getAll();
    }
    /**
     * Call this after settle the corresponding wdiff file.
     */
    void addDiffToReceivedDir(const MetaDiff &diff) {
        if (!diffMgr_.exists(diff)) {
            diffMgr_.add(diff);
        }
    }
    MetaDiffVec tryToMakeHardlinkInSendtoDir();
    /**
     * Try make a hard link of a diff file in all the archive directories.
     * If the diff file already exists in an archive directory, it will do nothing.
     */
    void tryToMakeHardlinkInSendtoDir(const MetaDiff &diff) {
        for (const std::string &archiveName : archiveSet_) {
            tryToMakeHardlinkForArchive(diff, archiveName);
        }
    }
    /**
     * Remove all temporary files in the received directory.
     */
    size_t gcTmpFiles() {
        return cybozu::removeAllTmpFiles(getReceivedDir().str());
    }
    /**
     * Delete a diff file from the received directory.
     * Before that, delete the corresponding MetaDidf from diffMgr.
     */
    void deleteDiffs(const MetaDiffVec &diffV, const std::string& archiveName = "") {
        const bool isReceived = archiveName.empty();
        MetaDiffManager& mgr = isReceived ? diffMgr_ : diffMgrMap_.get(archiveName);
        WalbDiffFiles wdiffs(mgr, isReceived ? getReceivedDir().str() : getSendtoDir(archiveName).str());
        wdiffs.removeDiffs(diffV);
    }
    cybozu::FilePath getReceivedDir() const {
        return volDir + "received";
    }
    cybozu::FilePath getSendtoDir() const {
        return volDir + "sendto";
    }
    cybozu::FilePath getSendtoDir(const std::string &archiveName) const {
        return getSendtoDir() + archiveName;
    }
    /**
     * Get total diff size.
     * getTotalDiffFileSize() means received wdiff files.
     *
     * RETURN:
     *   [byte]
     */
    uint64_t getTotalDiffFileSize(const std::string &archiveName = "") const;
    cybozu::FilePath getDiffPath(const MetaDiff &diff, const std::string &archiveName = "") const;
private:
    cybozu::FilePath getArchiveInfoPath(const std::string &name) const {
        return volDir + cybozu::FilePath(name + ArchiveSuffix);
    }
    /**
     * Get list of name of all the archive servers.
     */
    StrVec getArchiveNameList() const;
    /**
     * Reload metada for the mater.
     */
    void reloadReceivedWdiffs() {
        WalbDiffFiles wdiffs(diffMgr_, getReceivedDir().str());
        wdiffs.reload();
    }
    /**
     * Reload meta data for an archive.
     */
    void reloadSendtoWdiffs(const std::string &archiveName) {
        WalbDiffFiles wdiffs(diffMgrMap_.get(archiveName), getSendtoDir(archiveName).str());
        wdiffs.reload();
    }
    void tryToMakeHardlinkForArchive(const MetaDiff &diff, const std::string &archiveName);
};

} //namespace walb
