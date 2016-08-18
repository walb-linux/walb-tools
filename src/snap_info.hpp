#pragma once

#include <string>
#include <cinttypes>
#include "cybozu/exception.hpp"
#include "cybozu/serializer.hpp"
#include "linux/walb/walb.h"

struct SnapshotInfo
{
    std::string volId;
    uint64_t gid; // UINT64_MAX means unknown.
    uint64_t timestamp; // undefined if gid is UINT64_MAX.
    uint64_t lsid; // undefined if gid is UINT64_MAX. unused by archive.

    void init() {
        volId.clear();
        gid = UINT64_MAX;
        timestamp = 0;
        lsid = INVALID_LSID;
    }
    bool isUnknown() const {
        return gid == UINT64_MAX;
    }
    void verify() const {
        if (volId.empty()) {
            throw cybozu::Exception("SnapshotInfo:volId is empty.");
        }
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(volId, is);
        cybozu::load(gid, is);
        cybozu::load(timestamp, is);
        cybozu::load(lsid, is);
        verify();
    }
    template <typename OutputStream>
    void save(OutputStream& os) const {
        cybozu::save(os, volId);
        cybozu::save(os, gid);
        cybozu::save(os, timestamp);
        cybozu::save(os, lsid);
    }
};
