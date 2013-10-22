#pragma once
/**
 * @file
 * @brief Proxy data management.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <stdexcept>
#include <iostream>
#include <time.h>
#include "cybozu/serializer.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "fileio.hpp"
#include "fileio_serializer.hpp"
#include "meta.hpp"
#include "wdiff_data.hpp"
#include "server_info.hpp"

namespace walb {

/**
 * Data manager for a volume in a proxy.
 *
 * There is a wdiff data and
 * several managers of its replicas using hardlinks.
 *
 * Proxy data does not merge wdiff files in place,
 * but will merge wdiffs and transfer them to servers.
 *
 * This is thread safe.
 *
 * TODO:
 *   * use mutex_ for exclusive accesses.
 *   * implement mergeCandidates().
 */
class ProxyData
{
private:
    cybozu::FilePath baseDir_; /* base directory. */
    std::string name_; /* volume identifier. */
    std::shared_ptr<WalbDiffFiles> wdiffsP_; /* primary wdiff data. */
    std::map<std::string, ServerInfo> serverMap_;
    std::map<std::string, WalbDiffFiles> wdiffsMap_; /* server wdiff data. */
    std::mutex mutex_;

public:
    ProxyData(const std::string &baseDirStr, const std::string &name)
        : baseDir_(baseDirStr)
        , name_(name)
        , wdiffsP_()
        , serverMap_()
        , wdiffsMap_()
        , mutex_() {
        if (!baseDir_.stat().isDirectory()) {
            throw std::runtime_error("Does not exist: " + baseDir_.str());
        }
        mkdirIfNotExists(getDir());
        wdiffsP_ = std::make_shared<WalbDiffFiles>(getMasterDir().str(), false);
        mkdirIfNotExists(getServerDir());
        reloadServerRecords();
    }
    const WalbDiffFiles &getWdiffFiles() const {
        assert(wdiffsP_);
        return *wdiffsP_;
    }
    /**
     * For temporary file.
     */
    cybozu::FilePath getDiffDirToAdd() const {
        return getMasterDir();
    }
    cybozu::FilePath getDiffPathToAdd(const MetaDiff &diff) const {
        return getMasterDir() + cybozu::FilePath(createDiffFileName(diff));
    }
    /**
     * Before calling this, you must create a wdiff file in a master directory
     * that is corresponding to a given metadiff.
     *
     * This member function will make hardlinks of the file
     * to server directories. Then, the original file will be removed.
     */
    void add(const MetaDiff &diff) {
        if (!wdiffsP_->add(diff)) {
            throw std::runtime_error("add() error.");
        }
        cybozu::FilePath fPath(createDiffFileName(diff));
        cybozu::FilePath oldPath = getMasterDir() + fPath;
        for (const auto &pair : serverMap_) {
            const std::string &name = pair.first;
            checkServer(name);
            cybozu::FilePath newPath = getServerDir(name) + fPath;
            if (!oldPath.link(newPath)) {
                throw std::runtime_error("link() failed: " + newPath.str());
            }
            WalbDiffFiles &wdiffs = getWdiffFiles(name);
            if (!wdiffs.add(diff)) {
                throw std::runtime_error("wdiffs add failed.");
            }
        }
        wdiffsP_->removeBeforeGid(diff.snap1().gid0());
    }
    /**
     * @name server name.
     * @gid all wdiffs before gid will be removed.
     */
    void removeBeforeGid(const std::string &name, uint64_t gid) {
        checkServer(name);
        getWdiffFiles(name).removeBeforeGid(gid);
    }
    /**
     * Get transfer candidates.
     * @name server name.
     * @size maximum total size [byte].
     * RETURN:
     *   MetaDiff list that can be merged to a diff
     *   which will be transferred to the server.
     */
    std::vector<MetaDiff> getTransferCandidates(const std::string &name, uint64_t size) {
        assert(existsServer(name));
        return getWdiffFiles(name).getTransferCandidates(size);
    }
    bool existsServer(const std::string &name) const {
        return serverMap_.find(name) != serverMap_.end()
            && wdiffsMap_.find(name) != wdiffsMap_.end();
    }
    const ServerInfo &getServer(const std::string &name) const {
        return serverMap_.at(name);
    }
    void addServer(const ServerInfo &server) {
        const std::string &name = server.name();
        assert(!existsServer(name));
        emplace(name, server);
        saveServerRecord(name);
    }
    void removeServer(const std::string &name) {
        auto it0 = serverMap_.find(name);
        auto it1 = wdiffsMap_.find(name);
        assert(it0 != serverMap_.end() && it1 != wdiffsMap_.end());
        serverMap_.erase(it0);
        wdiffsMap_.erase(it1);
        cybozu::FilePath dp = getServerDir(name);
        if (!dp.rmdirRecursive()) {
            throw std::runtime_error("failed to remove directory: " + dp.str());
        }
    }
    std::vector<std::string> getServerNameList() const {
        std::vector<std::string> ret;
        for (const auto &p : serverMap_) {
            ret.push_back(p.first);
        }
        return ret;
    }
private:
    static std::string removeSuffix(const std::string &str, const std::string &suffix) {
        size_t pos = str.find(suffix);
        if (pos == std::string::npos || pos == 0) {
            throw std::runtime_error("does not have suffix: " + suffix);
        }
        return str.substr(0, pos);
    }
    static void mkdirIfNotExists(const cybozu::FilePath &path) {
        if (!path.stat().exists() && !path.mkdir()) {
            throw std::runtime_error("mkdir failed: " + path.str());
        }
        if (!path.stat().isDirectory()) {
            throw std::runtime_error("Not directory: " + path.str());
        }
    }
    void checkServer(const std::string &name) const {
        if (!existsServer(name)) {
            throw std::runtime_error("server does not exist: " + name);
        }
    }
    cybozu::FilePath getDir() const {
        return baseDir_ + cybozu::FilePath(name_);
    }
    cybozu::FilePath getMasterDir() const {
        return getDir() + cybozu::FilePath("master");
    }
    cybozu::FilePath getServerDir() const {
        return getDir() + cybozu::FilePath("slave");
    }
    cybozu::FilePath getServerDir(const std::string &name) const {
        return getServerDir() + cybozu::FilePath(name);
    }
    cybozu::FilePath serverRecordPath(const std::string &name) const {
        return getDir() + cybozu::FilePath(name + ".server");
    }
    void saveServerRecord(const std::string &name) const {
        assert(existsServer(name));
        const ServerInfo &server = getServer(name);
        cybozu::FilePath fp = serverRecordPath(name);
        cybozu::TmpFile tmpFile(fp.parent().str());
        cybozu::save(tmpFile, server);
        tmpFile.save(fp.str());
    }
    void reloadServerRecords() {
        serverMap_.clear();
        wdiffsMap_.clear();
        cybozu::FilePath dir = getDir();
        std::vector<cybozu::FileInfo> list;
        if (!cybozu::GetFileList(list, dir.str(), "server")) {
            throw std::runtime_error("GetFileList failed.");
        }
        for (cybozu::FileInfo &info : list) {
            if (info.name == ".." || info.name == "." || !info.isFile)
                continue;
            ::printf("hoge: [%s]\n", info.name.c_str()); /* debug */
            cybozu::FilePath fp = getDir() + cybozu::FilePath(info.name);
            cybozu::util::FileReader reader(fp.str(), O_RDONLY);
            ServerInfo server;
            cybozu::load(server, reader);
            if (removeSuffix(info.name, ".server") != server.name()) {
                throw std::runtime_error("server name invalid.");
            }
            emplace(server.name(), server);
        }
    }
    void emplace(const std::string &name, const ServerInfo &server) {
        cybozu::FilePath dp = getServerDir(name);
        auto res0 = serverMap_.emplace(name, server);
        auto res1 = wdiffsMap_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(name),
            std::forward_as_tuple(dp.str(), false));
        if (!res0.second || !res1.second) {
            throw std::runtime_error("map emplace failed.");
        }
    }
    WalbDiffFiles &getWdiffFiles(const std::string &name) {
        return wdiffsMap_.at(name);
    }
};

} //namespace walb.
