#include "proxy_vol_info.hpp"

namespace walb {


bool ProxyVolInfo::existsArchiveInfo(const std::string &name) const
{
    if (archiveSet_.find(name) == archiveSet_.cend()) {
        return false;
    }
    if (!getArchiveInfoPath(name).stat().isFile()) {
        return false;
    }
    if (!getSendtoDir(name).stat().isDirectory()) {
        return false;
    }
    return true;
}


void ProxyVolInfo::clear()
{
    for (const std::string &archiveName : archiveSet_) {
        diffMgrMap_.get(archiveName).clear();
    }
    diffMgr_.clear();
    archiveSet_.clear();
    if (!volDir.rmdirRecursive()) {
        throw cybozu::Exception("ProxyVolInfo::clear:rmdir recursively failed.");
    }
}


MetaDiffVec ProxyVolInfo::tryToMakeHardlinkInSendtoDir()
{
    MetaDiffVec diffV = getAllDiffsInReceivedDir();
    LOGs.debug() << "found diffs" << volId << diffV.size();
    for (const MetaDiff &d : diffV) {
        LOGs.debug() << "try to make hard link" << volId << d;
        tryToMakeHardlinkInSendtoDir(d);
    }
    return diffV;
}


uint64_t ProxyVolInfo::getTotalDiffFileSize(const std::string &archiveName) const
{
    const MetaDiffManager *p = &diffMgr_;
    if (!archiveName.empty()) p = &diffMgrMap_.get(archiveName);
    uint64_t total = 0;
    for (const MetaDiff &d : p->getAll()) {
        total += d.dataSize;
    }
    return total;
}


cybozu::FilePath ProxyVolInfo::getDiffPath(const MetaDiff &diff, const std::string &archiveName) const
{
    const std::string fname = createDiffFileName(diff);
    if (archiveName.empty()) {
        return getReceivedDir() + fname;
    } else {
        return getSendtoDir(archiveName) + fname;
    }
}


StrVec ProxyVolInfo::getArchiveNameList() const
{
    StrVec bnameV, fnameV;
    fnameV = util::getFileNameList(volDir.str(), ArchiveExtension);
    for (const std::string &fname : fnameV) {
        bnameV.push_back(cybozu::GetBaseName(fname));
    }
    return bnameV;
}


void ProxyVolInfo::tryToMakeHardlinkForArchive(const MetaDiff &diff, const std::string &archiveName)
{
    std::string fname = createDiffFileName(diff);
    cybozu::FilePath oldPath = getReceivedDir() + fname;
    cybozu::FilePath newPath = getSendtoDir(archiveName) + fname;
    if (!oldPath.stat().exists() || newPath.stat().exists()) {
        // Do nothing.
        return;
    }
    if (!oldPath.link(newPath)) {
        throw cybozu::Exception("ProxyVolInfo")
            << "make hardlink failed" << oldPath.str() << newPath.str() << cybozu::ErrorNo();
    }
    diffMgrMap_.get(archiveName).add(diff);
}


} // namespace walb
