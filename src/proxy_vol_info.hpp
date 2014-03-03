#pragma once
#include <string>
#include <vector>
#include "util.hpp"
#include "walb_util.hpp"
#include "file_path.hpp"
#include "host_info.hpp"
#include "meta.hpp"
#include "state_map.hpp"

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
private:
    const cybozu::FilePath volDir_;
    const std::string volId_;

    MetaDiffManager &diffMgr_;
    StateMap<MetaDiffManager> &diffMgrMap_;

public:
    /**
     * @baseDirStr base directory path string.
     * @volId volume identifier.
     */
    ProxyVolInfo(const std::string &baseDirStr, const std::string &volId,
                 MetaDiffManager &diffMgr, StateMap<MetaDiffManager> &diffMgrMap)
        : volDir_(cybozu::FilePath(baseDirStr) + volId)
        , volId_(volId)
        , diffMgr_(diffMgr), diffMgrMap_(diffMgrMap) {
        // QQQ
    }
    /**
     * Create volume directory
     */
    void init() {
        // QQQ
    }
    bool existsArchiveInfo(const std::string &/*name*/) const {
        // QQQ
        return false;
    }
    void addArchiveInfo(const HostInfo &/*host*/) {
        // QQQ
    }
    void deleteArchiveInfo(const HostInfo &/*host*/) {
        // QQQ
    }
    void updateArchiveInfo(const HostInfo &/*host*/) {
        // QQQ
    }
    HostInfo getArchiveInfo(const std::string &/*name*/) const {
        // QQQ
        return HostInfo();
    }
    /**
     * Get list of name of all the archive servers.
     */
    std::vector<std::string> getArchiveNameList() const {
        // QQQ
        return {};
    }
    /**
     * CAUSION:
     *   The volume will be removed if exists.
     *   All data inside the directory will be removed.
     */
    void clear() {
        // Delete all related lvm volumes and snapshots.
        if (!volDir_.rmdirRecursive()) {
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
                util::saveFile(volDir_, "state", newState);
                return;
            }
        }
        throw cybozu::Exception("ProxyVolInfo::setState:bad state") << newState;
    }
    std::string getState() const {
        std::string st;
        util::loadFile(volDir_, "state", st);
        return st;
    }
    bool existsVolDir() const {
        return volDir_.stat().isDirectory();
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
     * Remove diffs that do not have a specified uuid.
     */
    void removeDiffsNotHavingUuid(const std::string &/*archiveName*/, const cybozu::Uuid &/*uuid*/) {
        // QQQ
    }
    /**
     * Remove diffs where its snapE.gidE <= gid.
     */
    void removeDiffsBeforeGid(const std::string &/*archiveName*/, uint64_t /*gid*/) {
        // QQQ
    }
private:
    /**
     * Full path of the wdiff file of a corresponding meta diff.
     */
    cybozu::FilePath getDiffPath(const MetaDiff &diff) const {
        return volDir_ + cybozu::FilePath(createDiffFileName(diff));
    }
};

} //namespace walb
