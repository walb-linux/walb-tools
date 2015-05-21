#pragma once
#include "meta.hpp"
#include "walb_util.hpp"
#include "cybozu/serializer.hpp"
#include <sstream>
#include <iostream>

namespace walb {

/**
 * For full replication resume.
 */
struct FullReplState
{
    MetaState metaSt;
    uint64_t progressLb;
    uint64_t timestamp;

    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(metaSt, is);
        cybozu::load(progressLb, is);
        cybozu::load(timestamp, is);
        metaSt.verify();
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, metaSt);
        cybozu::save(os, progressLb);
        cybozu::save(os, timestamp);
    }
    std::string str() const {
        std::stringstream ss;
        ss << metaSt << " " << progressLb << " " << util::timeToPrintable(timestamp);
        return ss.str();
    }
    friend inline std::ostream& operator<<(std::ostream& os, const FullReplState& fullReplSt) {
        os << fullReplSt.str();
        return os;
    }
};

} // namespace walb
