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
        util::makeDir(getMasterDir().str(), "ProxyVolInfo::init:makedir failed", true);
        util::makeDir(getSlaveDir().str(), "ProxyVolInfo::init:makedir failed", true);
    }
    /**
     * Load wdiff meta data for master and each archive directory.
     */
    void loadAllArchiveInfo() {
        reloadMaster();
        for (const std::string &name : getArchiveNameList()) {
            archiveSet_.insert(name);
            reloadSlave(name);
        }
    }
    bool notExistsArchiveInfo() const {
        return archiveSet_.empty();
    }
    bool existsArchiveInfo(const std::string &name) const {
        if (archiveSet_.find(name) == archiveSet_.cend()) {
            return false;
        }
        if (!getArchiveInfoPath(name).stat().isFile()) {
            return false;
        }
        if (!getSlaveDir(name).stat().isDirectory()) {
            return false;
        }
        return true;
    }
    void addArchiveInfo(const std::string& name, const HostInfoForBkp &hi, bool ensureNotExistance) {
        util::saveFile(volDir, name + ArchiveSuffix, hi);
        util::makeDir(getSlaveDir(name).str(),
                      "ProxyVolInfo::addArchiveInfo", ensureNotExistance);
        archiveSet_.insert(name);
    }
    void deleteArchiveInfo(const std::string &name) {
        diffMgrMap_.get(name).clear();
        getSlaveDir(name).rmdirRecursive();
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
    void clear() {
        for (const std::string &archiveName : archiveSet_) {
            diffMgrMap_.get(archiveName).clear();
        }
        diffMgr_.clear();
        archiveSet_.clear();
        if (!volDir.rmdirRecursive()) {
            throw cybozu::Exception("ProxyVolInfo::clear:rmdir recursively failed.");
        }
    }
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
    StrVec getStatusAsStrVec() const {
        StrVec v;
        //auto &fmt = cybozu::util::formatString;

        // QQQ

        return v;
    }
    /**
     * Get diff list to send.
     *
     * @name archive host name.
     * @size maximum total size [byte].
     *
     * RETURN:
     *   MetaDiff list that can be merged.
     *   which will be sent to the server.
     */
    MetaDiffVec getDiffListToSend(const std::string &archiveName, uint64_t size) const {
        MetaDiffManager &mgr = diffMgrMap_.get(archiveName);
        WalbDiffFiles wdiffs(mgr, getSlaveDir(archiveName).str());
        return wdiffs.getDiffListToSend(size);
    }
    MetaDiffVec getAllDiffsInMaster() const {
        return diffMgr_.getAll();
    }
    /**
     * Call this after settle the corresponding wdiff file.
     */
    void addDiffToMaster(const MetaDiff &diff) {
        diffMgr_.add(diff);
    }
    /**
     * Try make a hard link of a diff file in all the archive directories.
     * If the diff file already exists in an archive directory, it will do nothing.
     */
    void tryToMakeHardlinkInSlave(const MetaDiff &diff) {
        for (const std::string &archiveName : archiveSet_) {
            tryToMakeHardlinkForArchive(diff, archiveName);
        }
    }
    /**
     * Delete a diff file from the master directory.
     * Before that, delete the corresponding MetaDidf from diffMgr.
     */
    void deleteDiffs(const MetaDiffVec &diffV, const std::string& archiveName = "") {
		const bool isMaster = archiveName.empty();
        MetaDiffManager& mgr = isMaster ? diffMgr_ : diffMgrMap_.get(archiveName);
        mgr.erase(diffV);
        WalbDiffFiles wdiffs(mgr, isMaster ? getMasterDir().str() : getSlaveDir(archiveName).str());
        wdiffs.removeDiffFiles(diffV);
    }
    cybozu::FilePath getMasterDir() const {
        return volDir + "master";
    }
    cybozu::FilePath getSlaveDir() const {
        return volDir + "slave";
    }
    cybozu::FilePath getSlaveDir(const std::string &archiveName) const {
        return getSlaveDir() + archiveName;
    }
    /**
     * Get total diff size.
     * getTotalDiffFileSize() means master wdiff files.
     *
     * RETURN:
     *   [byte]
     */
    uint64_t getTotalDiffFileSize(const std::string &archiveName = "") const {
        const MetaDiffManager *p = &diffMgr_;
        if (!archiveName.empty()) p = &diffMgrMap_.get(archiveName);
        uint64_t total = 0;
        for (const MetaDiff &d : p->getAll()) {
            total += getDiffFileSize(d, archiveName);
            LOGs.debug() << "total size" << total; // debug
        }
        return total;
    }
    /**
     * get diff file size.
     * RETURN:
     *   [byte]
     */
    uint64_t getDiffFileSize(const MetaDiff &diff, const std::string &archiveName = "") const {
        cybozu::FileStat st = getDiffPath(diff, archiveName).stat();
        return st.isFile() ? st.size() : 0;
    }
    cybozu::FilePath getDiffPath(const MetaDiff &diff, const std::string &archiveName = "") const {
        const std::string fname = createDiffFileName(diff);
        if (archiveName.empty()) {
            return getMasterDir() + fname;
        } else {
            return getSlaveDir(archiveName) + fname;
        }
    }
private:
    cybozu::FilePath getArchiveInfoPath(const std::string &name) const {
        return volDir + cybozu::FilePath(name + ArchiveSuffix);
    }
    /**
     * Get list of name of all the archive servers.
     */
    StrVec getArchiveNameList() const {
        StrVec bnameV, fnameV;
        fnameV = util::getFileNameList(volDir.str(), ArchiveExtension);
        for (const std::string &fname : fnameV) {
            bnameV.push_back(cybozu::GetBaseName(fname));
        }
        return bnameV;
    }
    /**
     * Reload metada for the mater.
     */
    void reloadMaster() {
        WalbDiffFiles wdiffs(diffMgr_, getMasterDir().str());
        wdiffs.reload();
    }
    /**
     * Reload meta data for an archive.
     */
    void reloadSlave(const std::string &archiveName) {
        WalbDiffFiles wdiffs(diffMgrMap_.get(archiveName), getSlaveDir(archiveName).str());
        wdiffs.reload();
    }
    void tryToMakeHardlinkForArchive(const MetaDiff &diff, const std::string &archiveName) {
        std::string fname = createDiffFileName(diff);
        cybozu::FilePath oldPath = getMasterDir() + fname;
        cybozu::FilePath newPath = getSlaveDir(archiveName) + fname;
        if (!oldPath.stat().exists()) {
            // Do nothing.
            return;
        }
        if (!oldPath.link(newPath)) {
            throw cybozu::Exception("ProxyVolInfo::tryToMakeHardlinkInSlave")
                << "make hardlink failed" << oldPath.str() << newPath.str();
        }
        diffMgrMap_.get(archiveName).add(diff);
    }
};

} //namespace walb
