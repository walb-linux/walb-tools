#pragma once
#include <string>
#include <vector>
#include "util.hpp"
#include "walb_util.hpp"
#include "file_path.hpp"
#include "host_info.hpp"
#include "meta.hpp"
#include "state_map.hpp"
#include "wdiff_data.hpp"

namespace walb {

const char *const pClear = "Clear";
const char *const pStopped = "Stopped";
const char *const pStarted = "Started";

// temporary state
const char *const ptStart = "Start";
const char *const ptStop = "Stop";
const char *const ptClearVol = "ClearVol";
const char *const ptAddArchiveInfo = "AddArchiveInfo";
const char *const ptDeleteArchiveInfo = "DeleteArchiveInfo";
const char *const ptUpdateArchiveInfo = "UpdateArchiveInfo";
const char *const ptWlogRecv = "WlogRecv";

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
    StateMap<MetaDiffManager> &diffMgrMap_;

public:
    /**
     * @baseDirStr base directory path string.
     * @volId volume identifier.
     */
    ProxyVolInfo(const std::string &baseDirStr, const std::string &volId,
                 MetaDiffManager &diffMgr, StateMap<MetaDiffManager> &diffMgrMap)
        : volDir(cybozu::FilePath(baseDirStr) + volId)
        , volId(volId)
        , diffMgr_(diffMgr), diffMgrMap_(diffMgrMap) {
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
        // QQQ
    }
    bool existsArchiveInfo(const std::string &name) const {
        if (!getArchiveInfoPath(name).stat().isFile()) {
            return false;
        }
        if (!getSlaveDir(name).stat().isDirectory()) {
            return false;
        }
        return true;
    }
    void addArchiveInfo(const HostInfo &hi) {
        hi.verify();
        util::saveFile(volDir, hi.name + ".archive", hi);
        util::makeDir(getSlaveDir(hi.name).str(),
                      "ProxyVolInfo::addArchiveInfo", true);
    }
    void deleteArchiveInfo(const std::string &name) {
        diffMgrMap_.get(name).clear();
        getSlaveDir(name).rmdirRecursive();
        getArchiveInfoPath(name).remove();
    }
    void updateArchiveInfo(const HostInfo &hi) {
        hi.verify();
        util::saveFile(volDir, hi.name + ".archive", hi);
    }
    HostInfo getArchiveInfo(const std::string &name) const {
        HostInfo hi;
        util::loadFile(volDir, name + ".archive", hi);
        hi.verify();
        return hi;
    }
    /**
     * Get list of name of all the archive servers.
     */
    std::vector<std::string> getArchiveNameList() const {
        std::vector<std::string> ret, fnameV;
        fnameV = util::getFileNameList(volDir.str(), "archive");
        for (const std::string &fname : fnameV) {
            size_t n = fname.find(".archive");
            if (n == std::string::npos) {
                throw cybozu::Exception("ProxyVolInfo::getArchiveNameList")
                    << "filename extention is not '.archive'";
            }
            ret.push_back(fname.substr(0, n));
        }
        return ret;
    }
    /**
     * CAUSION:
     *   The volume will be removed if exists.
     *   All data inside the directory will be removed.
     */
    void clear() {
        // Delete all related lvm volumes and snapshots.
        if (!volDir.rmdirRecursive()) {
            throw cybozu::Exception("ProxyVolInfo::clear:rmdir recursively failed.");
        }
    }
    void setState(const std::string& newState)
    {
        const char *tbl[] = {
            pStarted, pStopped,
        };
        for (const char *p : tbl) {
            if (newState == p) {
                util::saveFile(volDir, "state", newState);
                return;
            }
        }
        throw cybozu::Exception("ProxyVolInfo::setState:bad state") << newState;
    }
    std::string getState() const {
        std::string st;
        util::loadFile(volDir, "state", st);
        return st;
    }
    bool existsVolDir() const {
        return volDir.stat().isDirectory();
    }
    std::vector<std::string> getStatusAsStrVec() const {
        std::vector<std::string> v;
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
    std::vector<MetaDiff> getDiffListToSend(const std::string &/*archiveName*/, uint64_t /*size*/) {
        // QQQ
        return {};
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
    void reloadSlave(const std::string &name) {
        WalbDiffFiles wdiffs(diffMgrMap_.get(name), getSlaveDir(name).str());
        wdiffs.reload();
    }
    std::vector<MetaDiff> getAllDiffsInMaster() const {
        return diffMgr_.getAll();
    }
    /**
     * Try make a hard link of a diff file in an archive directory.
     * If already exists, do nothing.
     */
    void tryToMakeHardlinkInSlave(const MetaDiff &diff, const std::string &name) {
        std::string fname = createDiffFileName(diff);
        cybozu::FilePath oldPath = getMasterDir() + fname;
        cybozu::FilePath newPath = getSlaveDir(name) + fname;
        if (oldPath.stat().exists()) {
            // Do nothing.
            return;
        }
        if (!oldPath.link(newPath)) {
            throw cybozu::Exception("ProxyVolInfo::tryToMakeHardlinkInSlave")
                << "make hardlink failed" << oldPath.str() << newPath.str();
        }
        diffMgrMap_.get(name).add(diff);
    }
    /**
     * Delete a diff file from the master directory.
     * Before that, delete the corresponding MetaDidf from diffMgr.
     */
    void deleteDiffsFromMaster(const std::vector<MetaDiff> &diffV) {
        diffMgr_.erase(diffV);
        WalbDiffFiles wdiffs(diffMgr_, getMasterDir().str());
        wdiffs.removeDiffFiles(diffV);
    }
    cybozu::FilePath getMasterDir() const {
        return volDir + "master";
    }
    cybozu::FilePath getSlaveDir(const std::string &name) const {
        return volDir + "slave" + name;
    }
private:
    /**
     * Full path of the wdiff file of a corresponding meta diff.
     */
    cybozu::FilePath getDiffPath(const MetaDiff &diff) const {
        return volDir + cybozu::FilePath(createDiffFileName(diff));
    }
    cybozu::FilePath getArchiveInfoPath(const std::string &name) const {
        return volDir + cybozu::FilePath(name + ".archive");
    }
};

} //namespace walb
