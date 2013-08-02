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
#include <time.h>
#include "cybozu/serializer.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "meta.hpp"
#include "wdiff_data.hpp"

#ifndef WALB_TOOLS_PROXY_DATA_HPP
#define WALB_TOOLS_PROXY_DATA_HPP

namespace walb {

/**
 * Server identifier for connection.
 */
class Server
{
private:
    std::string name_; /* must be unique in the system. */
    std::string addr_; /* what cybozu::SocketAddr can treat. */
    uint16_t port_;
public:
    Server(const std::string &name, const std::string &addr, uint16_t port)
        : name_(name), addr_(addr), port_(port) {
    }
    const std::string &name() const { return name_; }
    const std::string &addr() const { return addr_; }
    uint16_t port() const { return port_; }
    bool operator==(const Server &rhs) const {
        return name_ == rhs.name_;
    }
    bool operator!=(const Server &rhs) const {
        return name_ != rhs.name_;
    }
    bool operator<(const Server &rhs) const {
        return name_ < rhs.name_;
    }
    bool operator<=(const Server &rhs) const {
        return name_ <= rhs.name_;
    }
    bool operator>(const Server &rhs) const {
        return name_ > rhs.name_;
    }
    bool operator>=(const Server &rhs) const {
        return name_ >= rhs.name_;
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, name_);
        cybozu::save(os, addr_);
        cybozu::save(os, port_);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(name_, is);
        cybozu::load(addr_, is);
        cybozu::load(port_, is);
    }
};

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
 */
class ProxyData
{
private:
    cybozu::FilePath baseDir_; /* base directory. */
    std::string name_; /* volume identifier. */
    WalbDiffFiles wdiffFiles_; /* primary wdiff data. */
    std::map<Server, WalbDiffFiles> map_;
    std::mutex mutex_;

public:
    ProxyData(const std::string &baseDirStr, const std::string &name)
        : baseDir_(baseDirStr)
        , name_(name)
        , wdiffFiles_(getMasterDirStatic(baseDir_, name))
        , map_()
        , mutex_() {
    }
    void initialize() {
        /* now editing */
    }
    bool add(const MetaDiff &diff, const std::vector<Server> &servers) {
        /*
         * TODO:
         * add primary wdiff directory.
         * make hard links of wdiffs in servers' directory.
         * remove the wdiff from the primary wdiff directory.
         */

        /* now editing */
        return false;
    }
    void removeBeforeGid(const Server &server, uint64_t gid) {
        /* now editing */
    }
    void removeServer(const Server &server) {
        serverRecordPath(server).remove();
        auto it = map_.find(server);
        if (it == map_.end()) return;
        map_.erase(it);
        if (!getServerDir(server).rmdirRecursive()) {
            throw std::runtime_error("failed to remove directory.");
        }
    }
    void addServer(const Server &server) {
        /* now editing */
    }
    /**
     * THe merge candidates.
     */
    std::vector<MetaDiff> mergeCandidates(uint64_t size) {
        /* now editing */
        return {};
    }
private:
    static std::string getMasterDirStatic(
        const cybozu::FilePath &baseDir,
        const std::string &name) {
        return (baseDir + cybozu::FilePath(name) + cybozu::FilePath("master")).str();
    }
    cybozu::FilePath getServerDir(const Server &server) const {
        return baseDir_ + cybozu::FilePath(name_)
            + cybozu::FilePath("slave") + cybozu::FilePath(server.name());
    }
    cybozu::FilePath getMasterDir() const {
        return baseDir_ + cybozu::FilePath(name_)
            + cybozu::FilePath("master");
    }
    cybozu::FilePath serverRecordPath(const Server &server) const {
        return getServerDir(server) + cybozu::FilePath("server");
    }
    void saveServerRecord(const Server &server) const {

        /* now editing */
    }
    void reloadServerRecords() {

        /* now editing */
    }
};

} //namespace walb.

#endif /* WALB_TOOLS_PROXY_DATA_HPP */
