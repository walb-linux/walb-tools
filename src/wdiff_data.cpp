#include "wdiff_data.hpp"

namespace walb {

MetaDiffVec loadWdiffMetadata(const std::string &dirStr)
{
    MetaDiffVec ret;
    cybozu::FilePath dirPath(dirStr);
    for (const std::string &fname : util::getFileNameList(dirStr, "wdiff")) {
        MetaDiff d = parseDiffFileName(fname);
        d.dataSize = (dirPath + fname).stat().size();
        ret.push_back(d);
    }
    return ret;
}

void clearWdiffFiles(const std::string &dirStr)
{
    cybozu::FilePath dir(dirStr);
    for (const std::string &fname : util::getFileNameList(dirStr, "wdiff")) {
        cybozu::FilePath p = dir + fname;
        if (!p.unlink()) {
            LOGs.error() << "clearWdiffFiles:unlink failed" << p.str() << cybozu::ErrorNo();
        }
    }
}

size_t WalbDiffFiles::removeDiffFiles(const MetaDiffVec &v)
{
    for (const MetaDiff &d : v) {
        cybozu::FilePath p = dir_ + createDiffFileName(d);
        if (!p.stat().isFile()) continue;
        if (!p.unlink()) {
            LOGs.error() << "removeDiffFiles:unlink failed" << p.str();
        }
    }
    return v.size();
}

void WalbDiffFiles::truncateDiffVecBySize(MetaDiffVec &v, uint64_t size) const
{
    if (v.empty()) return;
    uint64_t total = v[0].dataSize;
    size_t i = 1;
    while (i < v.size()) {
        const uint64_t s = v[i].dataSize;
        if (size < total + s) break;
        total += s;
        i++;
    }
    v.resize(i);
}

} //namespace walb
