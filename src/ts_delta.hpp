#pragma once

#include <string>
#include <cinttypes>
#include "time.hpp"
#include "util.hpp"

/**
 * Timestamp delta.
 */
struct TsDelta
{
    std::string volId;
    std::string dstId;

    // [0]: src, [1]: dst
    std::string kind[2];
    uint64_t gid[2]; // UINT64_MAX means empty.
    uint64_t ts[2];

    void init() {
        volId.clear();
        dstId.clear();
        kind[0].clear();
        kind[1].clear();
        ts[0] = ts[1] = 0;
        gid[0] = gid[1] = UINT64_MAX;
    }
    std::string toStr() const {
        auto fmt = cybozu::util::formatString;
        std::string ret = fmt(
            "name:%s\t"
            "dest:%s\t"
            "kind:%s\t"
            "dest_kind:%s\t"
            "gid:%" PRIu64 "\t"
            "ts:%s\t"
            , volId.c_str(), dstId.c_str(), kind[0].c_str(), kind[1].c_str()
            , gid[0], cybozu::unixTimeToPrettyStr(ts[0]).c_str());
        if (gid[1] == UINT64_MAX) {
            ret += fmt(
                "dest_ts:\t"
                "dest_gid:\t"
                "ts_delta:");
        } else {
            int64_t delta;
            if (ts[0] >= ts[1]) {
                delta = ts[0] - ts[1];
            } else {
                delta = -(ts[1] - ts[0]);
            }
            ret += fmt(
                "dest_gid:%" PRIu64 "\t"
                "dest_ts:%s\t"
                "ts_delta:%" PRId64 ""
                , gid[1], cybozu::unixTimeToPrettyStr(ts[1]).c_str(), delta);
        }
        return ret;
    }
};
