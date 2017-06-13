#include "meta.hpp"


namespace walb {


MetaDiff parseDiffFileName(const std::string &name)
{
    const char * const FUNC = __func__;
    MetaDiff diff;
    const std::string minName("YYYYMMDDhhmmss-MC-0-1.wdiff");
    std::string s = name;
    if (s.size() < minName.size()) {
        throw cybozu::Exception(FUNC) << "too short name" << name;
    }
    /* timestamp */
    std::string ts = s.substr(0, 14);
    diff.timestamp = cybozu::strToUnixTime(ts);
    if (s[14] != '-') {
        throw cybozu::Exception(FUNC) << "invalid timestamp str" << name;
    }
    /* isMergeable and isCompDiff */
    diff.isMergeable = s[15] == 'M';
    diff.isCompDiff = s[16] == 'C';
    if (s[17] != '-') {
        throw cybozu::Exception(FUNC) << "must be - at 17th char" << name;
    }
    s = s.substr(18);
    /* gid0, gid1(, gid2, gid3). */
    std::vector<uint64_t> gidV;
    bool isLast = false;
    for (int i = 0; i < 4; i++) {
        size_t n = s.find("-");
        if (n == std::string::npos) {
            isLast = true;
            n = s.find(".wdiff");
            if (n == std::string::npos) {
                throw cybozu::Exception(FUNC) << "wrong suffix" << name;
            }
        }
        bool b;
        const uint64_t gid = cybozu::atoi(&b, s.c_str(), n);
        if (!b) {
            throw cybozu::Exception(FUNC) << "wrong digit value" << name << i;
        }
        gidV.push_back(gid);
        if (isLast) break;
        s = s.substr(n + 1);
    }
    switch (gidV.size()) {
    case 2:
        diff.snapB.set(gidV[0]);
        diff.snapE.set(gidV[1]);
        break;
    case 4:
        diff.snapB.set(gidV[0], gidV[1]);
        diff.snapE.set(gidV[2], gidV[3]);
        break;
    default:
        throw cybozu::Exception(FUNC) << "number of gids must be 2 or 4" << name;
    }
    diff.verify();
    return diff;
}


std::string createDiffFileName(const MetaDiff &diff)
{
    std::string s;
    s += cybozu::unixTimeToStr(diff.timestamp);
    s += '-';
    s += diff.isMergeable ? 'M' : '-';
    s += diff.isCompDiff ? 'C' : '-';
    std::vector<uint64_t> v;
    if (diff.isDirty()) {
        v.push_back(diff.snapB.gidB);
        v.push_back(diff.snapB.gidE);
        v.push_back(diff.snapE.gidB);
        v.push_back(diff.snapE.gidE);
    } else {
        v.push_back(diff.snapB.gidB);
        v.push_back(diff.snapE.gidB);
    }
    for (uint64_t gid : v) {
        s += '-';
        s += cybozu::itoa(gid);
    }
    s += ".wdiff";
    return s;
}


MetaDiff getMaxProgressDiff(const MetaDiffVec &v)
{
    if (v.empty()) throw cybozu::Exception("getMaxProgressDiff:empty");
    MetaDiff diff = v[0];
    for (size_t i = 1; i < v.size(); i++) {
        const bool cond0 = diff.snapE.gidB < v[i].snapE.gidB;
        const bool cond1 = diff.snapE.gidB == v[i].snapE.gidB && diff.snapB.gidB < v[i].snapB.gidB;
        if (cond0 || cond1) {
            diff = v[i];
        }
    }
    return diff;
}


void GidRangeManager::add(MetaDiffMmap::iterator it)
{
    const MetaDiff& d = it->second;
    GidRange rng{d.snapB.gidB, d.snapE.gidB, {it}};
    std::list<GidRange> rngL = getRange(rng.gidB, rng.gidE);
    merge(rngL, std::move(rng));
    putRange(rngL);
}


void GidRangeManager::remove(MetaDiffMmap::iterator it)
{
    const MetaDiff& d = it->second;
    uint64_t gid = d.snapB.gidB + 1;
    auto itR = map_.lower_bound(gid);
    while (gid < d.snapE.gidB && itR != map_.end()) {
        GidRange& rng = itR->second;
        std::vector<MetaDiffMmap::iterator>& its = rng.its;
        its.erase(std::remove(its.begin(), its.end(), it), its.end());
        gid = rng.gidE + 1;
        if (its.empty()) {
            itR = map_.erase(itR);
        } else {
            ++itR;
        }
    }
}


std::vector<MetaDiffMmap::iterator> GidRangeManager::search(uint64_t gid) const
{
    auto itR = map_.lower_bound(gid + 1);
    if (itR == map_.end()) return {};
    const GidRange& rng = itR->second;
    if (rng.gidB < gid) return {};
    return rng.its; // copy
}


std::list<GidRange> GidRangeManager::getRange(uint64_t gidB, uint64_t gidE)
{
    std::list<GidRange> li;
    auto itR = map_.lower_bound(gidB + 1);
    while (itR != map_.end()) {
        GidRange& rng = itR->second;
        if (rng.gidB >= gidE) break;
        li.push_back(std::move(rng));
        itR = map_.erase(itR);
    }
    return li;
}


void GidRangeManager::putRange(std::list<GidRange>& rngL)
{
    for (GidRange& rng : rngL) {
        Map::iterator it;
        bool ret;
        const uint64_t key = rng.gidE;
        std::tie(it, ret) = map_.insert(std::make_pair(key, std::move(rng)));
        assert(ret);
    }
    rngL.clear();
}


void GidRangeManager::merge(std::list<GidRange>& rngL, GidRange&& rng)
{
    if (rngL.empty()) {
        rngL.push_back(std::move(rng));
        return;
    }

    /*
     * rngL  |-----| ...
     * rng      |----------|
     * rngL' |--|--| ...
     */
    if (rngL.front().gidB < rng.gidB) {
        assert(rng.gidB < rngL.front().gidE);
        GidRange rng2 = std::move(rngL.front());
        rngL.pop_front();
        auto it = rngL.begin();
        for (GidRange& rng3 : rng2.split(rng.gidB)) {
            rngL.insert(it, std::move(rng3));
        }
    }

    /*
     * rngL   ... |------|
     * rng   |--------|
     * rngL'  ... |---|--|
     */
    if (rngL.back().gidE > rng.gidE) {
        assert(rngL.back().gidB < rng.gidE);
        GidRange rng2 = std::move(rngL.back());
        rngL.pop_back();
        for (GidRange& rng3 : rng2.split(rng.gidE)) {
            rngL.push_back(std::move(rng3));
        }
    }

    auto it = rngL.begin();
    while (rng.gidB < rng.gidE && it != rngL.end()) {
        if (it->gidB == rng.gidB) {
            // merge.
            for (auto it2 : rng.its) it->its.push_back(it2);
            rng.gidB = it->gidE; // consume.
            ++it;
        } else if (it->gidB < rng.gidB) {
            // skip.
            assert(it->gidE <= rng.gidB);
            ++it;
        } else {
            // insert.
            rngL.insert(it, {rng.gidB, it->gidB, rng.its});
            rng.gidB = it->gidB; // consume.
        }
    }
    if (rng.gidB < rng.gidE) {
        // insert.
        rngL.push_back(std::move(rng));
    } else {
        // Do nothing.
    }
}


void GidRangeManager::print() const
{
    for (auto pair : map_) {
        ::printf("%s", pair.second.str().c_str());
    }
}


void GidRangeManager::validateExistence(Mmap::const_iterator it, int line) const
{
    const MetaDiff& diff = it->second;
    uint64_t gid = diff.snapB.gidB + 1;
    auto itR = map_.lower_bound(gid);
    uint64_t gidE;
    {
        // begin point check.
        if (itR == map_.end()) {
            throw cybozu::Exception(__func__) << "not found" << line;
        }
        const GidRange& rng = itR->second;
        if (rng.gidB != diff.snapB.gidB) {
            throw cybozu::Exception(__func__) << "gidB differs." << line;
        }
        gidE = rng.gidB; // dummy.
    }
    while (itR != map_.end() && gidE < diff.snapE.gidB) {
        const GidRange& rng = itR->second;
        if (rng.gidB != gidE) {
            throw cybozu::Exception(__func__) << "lack of GidRange." << line << rng << gidE;
        }
        bool found = false;
        for (const auto itM : rng.its) {
            const MetaDiff& d = itM->second;
            if (d == diff) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw cybozu::Exception(__func__) << "not found the iterator." << line;
        }
        gidE = rng.gidE;
        ++itR;
    }
    if (gidE != diff.snapE.gidB) {
        throw cybozu::Exception(__func__) << "gidE differs" << line;
    }
}


bool MetaDiffManager::changeSnapshot(uint64_t gid, bool enable, MetaDiffVec &diffV)
{
    AutoLock lk(mu_);
    auto range = mmap_.equal_range(gid);
    if (range.first == range.second) {
        return false; // not found.
    }
    for (Mmap::iterator i = range.first; i != range.second; ++i) {
        MetaDiff& diff = i->second;
        if (enable) {
            if (diff.isMergeable) {
                diff.isMergeable = false;
                diffV.push_back(diff);
            }
        } else {
            if (!diff.isMergeable) {
                diff.isMergeable = true;
                diffV.push_back(diff);
            }
        }
    }
    return true;
}


MetaDiffVec MetaDiffManager::gc(const MetaSnap &snap)
{
    AutoLock lk(mu_);
    MetaDiffVec garbages;

    /* Remove non-garbage diffs from mmap_. */
    MetaDiffVec v = getApplicableDiffList(snap);
    for (const MetaDiff &d : v) eraseNolock(d);

    /* All the remaining diffs in mmap_ are garbage. */
    for (Mmap::value_type &p : mmap_) garbages.push_back(p.second);
    rangeMgr_.clear();
    mmap_.clear();

    // Place back non-garbage diffs to mmap_.
    for (const MetaDiff &d : v) addNolock(d);

    return garbages;
}


MetaDiffVec MetaDiffManager::gcRange(uint64_t gidB, uint64_t gidE)
{
    AutoLock lk(mu_);
    MetaDiffVec garbages;
    Mmap::iterator it = mmap_.lower_bound(gidB);
    while (it != mmap_.end()) {
        const MetaDiff &d = it->second;
        if (d.snapB.gidB >= gidE) break;
        if (gidB <= d.snapB.gidB && d.snapE.gidB <= gidE &&
            !(gidB == d.snapB.gidB && gidE == d.snapE.gidB)) {
            garbages.push_back(d);
            rangeMgr_.remove(it);
            it = mmap_.erase(it);
        } else {
            ++it;
        }
    }
    return garbages;
}



MetaDiffVec MetaDiffManager::eraseBeforeGid(uint64_t gid)
{
    AutoLock lk(mu_);
    MetaDiffVec v;
    auto it = mmap_.begin();
    while (it != mmap_.end()) {
        const MetaDiff &d = it->second;
        if (gid <= d.snapB.gidB) {
            // There are no matching diffs after this.
            break;
        }
        if (d.snapE.gidB <= gid) {
            v.push_back(d);
            rangeMgr_.remove(it);
            it = mmap_.erase(it);
        } else {
            ++it;
        }
    }
    return v;
}


MetaDiffVec MetaDiffManager::getMergeableDiffList(
    uint64_t gid, const std::function<bool(const MetaDiff &)> &pred) const
{
    AutoLock lk(mu_);
    MetaDiffVec v = getFirstDiffsNolock(gid);
    if (v.empty()) return {};
    MetaDiff diff = getMaxProgressDiff(v);
    v = {diff};
    MetaDiff mdiff = diff;
    for (;;) {
        MetaDiffVec u = getMergeableCandidatesNolock(mdiff);
        if (u.empty()) break;
        diff = getMaxProgressDiff(u);
        if (!pred(diff)) break;
        mdiff = merge(mdiff, diff);
        v.push_back(diff);
    }
    return v;
}


MetaDiffVec MetaDiffManager::getApplicableDiffList(
    const MetaSnap &snap,
    const std::function<bool(const MetaDiff &, const MetaSnap &)> &pred) const
{
    AutoLock lk(mu_);
    MetaSnap s = snap;
    MetaDiffVec v;
    for (;;) {
        MetaDiff d;
        if (!getApplicableDiff(s, d)) break;
        s = apply(s, d);
        if (!pred(d, s)) break;
        v.push_back(d);
    }
    return v;
}


MetaDiffVec MetaDiffManager::getApplicableAndMergeableDiffList(const MetaSnap &snap) const
{
    MetaDiffVec v = getApplicableDiffList(snap);
    if (v.empty()) return {};

    MetaDiff diff = v[0];
    size_t i = 1;
    while (i < v.size()) {
        if (!canMerge(diff, v[i])) break;
        diff.merge(v[i]);
        i++;
    }
    v.resize(i);
    return v;
}


MetaState MetaDiffManager::getOldestCleanState(const MetaState &st0) const
{
    AutoLock lk(mu_);
    MetaDiffVec minV = getMinimumApplicableDiffList(st0);
    MetaState st = apply(st0, minV);
    assert(!st.isApplying);
    for (;;) {
        if (st.snapB.isClean()) break;
        MetaDiff d;
        if (!getApplicableDiff(st.snapB, d)) {
            throw cybozu::Exception("MetaDiffManager::getOldestCleanState:there is no clean snapshot.");
        }
        st = apply(st, d);
    }
    return st;
}


std::vector<uint64_t> MetaDiffManager::getCleanSnapshotList(const MetaState &st) const
{
    const bool isAll = true;
    const std::vector<MetaState> v = getRestorableList(st, isAll); // using lock.
    std::vector<uint64_t> ret;
    for (const MetaState &st : v) {
        ret.push_back(st.snapB.gidB);
    }
    return ret;
}


std::vector<MetaState> MetaDiffManager::getRestorableList(const MetaState &st, bool isAll) const
{
    std::vector<MetaState> ret;
    MetaDiffVec applicableV, minV;
    getTargetDiffLists(applicableV, minV, st); // using lock.
    MetaState st0 = apply(st, minV);
    if (st0.snapB.isClean()) ret.push_back(st0);
    for (size_t i = minV.size(); i < applicableV.size(); i++) {
        st0 = apply(st0, applicableV[i]);
        const bool isLast = (i + 1 == applicableV.size());
        const bool isExplicit = isLast || !applicableV[i + 1].isMergeable;
        st0.isExplicit = isExplicit;
        if (st0.snapB.isClean() && (isAll || isExplicit)) ret.push_back(st0);
    }
    return ret;
}



void MetaDiffManager::getTargetDiffLists(
    MetaDiffVec& applicableV, MetaDiffVec& minV, const MetaState &st, uint64_t gid) const
{
    AutoLock lk(mu_);
    applicableV = getApplicableDiffListByGid(st.snapB, gid);
    // use this if timestamp
    // ret = getApplicableDiffListByTime(st.snapB, timestamp);
    if (applicableV.empty()) return;

    minV = getMinimumApplicableDiffList(st);
}


void MetaDiffManager::getTargetDiffLists(MetaDiffVec& applicableV, MetaDiffVec& minV, const MetaState &st) const
{
    AutoLock lk(mu_);
    applicableV = getApplicableDiffList(st.snapB);
    minV = getMinimumApplicableDiffList(st);
    if (applicableV.size() < minV.size()) {
        throw cybozu::Exception(__func__) << "size bug" << applicableV.size() << minV.size();
    }
#ifdef DEBUG
    for (size_t i = 0; i < minV.size(); i++) {
        assert(applicableV[i] == minV[i]);
    }
#endif
}



MetaDiffVec MetaDiffManager::getDiffListToSync(const MetaState &st, const MetaSnap &snap) const
{
    MetaDiffVec applicableV, minV;
    getTargetDiffLists(applicableV, minV, st, snap.gidB); // using lock.
    if (minV.size() > applicableV.size()) return {};
    const MetaState appliedSt = apply(st, applicableV);
    if (appliedSt.snapB == snap) {
        return applicableV;
    } else {
        return {};
    }
}


MetaDiffVec MetaDiffManager::getAll(uint64_t gid0, uint64_t gid1) const
{
    if (gid0 >= gid1) {
        throw cybozu::Exception("MetaDiffManager::getAll:gid0 >= gid1")
            << gid0 << gid1;
    }
    AutoLock lk(mu_);
    MetaDiffVec v;
    fastSearchNolock(gid0, gid1, v, [](const MetaDiff &){ return true; });
    return v;
}


bool MetaDiffManager::exists(const MetaDiff& diff) const
{
    AutoLock lk(mu_);
    const MetaDiffVec v = getFirstDiffsNolock(diff.snapB.gidB);
    for (const MetaDiff& d : v) {
        if (d == diff) {
            return true;
        }
    }
    return false;
}


std::pair<uint64_t, uint64_t> MetaDiffManager::getMinMaxGid() const
{
    AutoLock lk(mu_);
    if (mmap_.empty()) return {0, 0};
    uint64_t min = UINT64_MAX, max = 0;
    for (const auto &p : mmap_) {
        const MetaDiff &d = p.second;
        min = std::min(min, d.snapB.gidB);
        max = std::max(max, d.snapE.gidB);
    }
    return {min, max};
}


void MetaDiffManager::addNolock(const MetaDiff &diff)
{
    if (searchNolock(diff) != mmap_.end()) {
        throw cybozu::Exception("MetaDiffManager::add:already exists") << diff;
    }
    auto it = mmap_.emplace(diff.snapB.gidB, diff);
    rangeMgr_.add(it);
}


void MetaDiffManager::eraseNolock(const MetaDiff &diff, bool doesThrowError)
{
    auto it = searchNolock(diff);
    if (it == mmap_.end()) {
        if (doesThrowError) {
            throw cybozu::Exception("MetaDiffManager::erase:not found") << diff;
        }
        return;
    }
    rangeMgr_.remove(it);
    mmap_.erase(it);
}


MetaDiffManager::Mmap::iterator MetaDiffManager::searchNolock(const MetaDiff &diff)
{
    Mmap::iterator it, end;
    std::tie(it, end) = mmap_.equal_range(diff.snapB.gidB);
    while (it != end) {
        const MetaDiff &d = it->second;
        if (diff == d) return it;
        ++it;
    }
    return mmap_.end();
}


MetaDiffVec MetaDiffManager::getFirstDiffsNolock(uint64_t gid) const
{
    Mmap::const_iterator it0 = mmap_.lower_bound(gid);
    if (it0 == mmap_.cend()) return {};
    const MetaDiff &d = it0->second;

    MetaDiffVec v;
    Mmap::const_iterator it, it1;
    std::tie(it, it1) = mmap_.equal_range(d.snapB.gidB);
    while (it != it1) {
        v.push_back(it->second);
        ++it;
    }
    return v;
}


MetaDiffVec MetaDiffManager::getMergeableCandidatesNolock(const MetaDiff &diff) const
{
    MetaDiffVec v;
    for (auto it : rangeMgr_.search(diff.snapE.gidB)) {
        const MetaDiff &d = it->second;
        if (diff != d && canMerge(diff, d)) v.push_back(d);
    }
    return v;
}


MetaDiffVec MetaDiffManager::getApplicableCandidatesNolock(const MetaSnap &snap) const
{
    MetaDiffVec v;
    for (auto it : rangeMgr_.search(snap.gidB)) {
        const MetaDiff &d = it->second;
        if (canApply(snap, d)) v.push_back(d);
    }
    return v;
}


namespace meta_local {


size_t findNonInt(const std::string &s, size_t i)
{
    assert(i < s.size());
    while ('0' <= s[i] && s[i] <= '9') i++;
    return i;
}


/**
 * parse '|gid|' or '|gid,gid|' string.
 * RETURN:
 *   next position.
 */
size_t parseMetaSnap(const std::string &s, size_t i, MetaSnap &snap)
{
    const char *const FUNC = __func__;
    const char *msg = "bad input string";
    if (s[i] != '|') throw cybozu::Exception(FUNC) << msg << s << i;
    i++;
    size_t j = findNonInt(s, i);
    const uint64_t gidB = cybozu::atoi(s.substr(i, j - i));
    if (s[j] == '|') {
        snap.set(gidB);
        return j + 1;
    }
    if (s[j] != ',') throw cybozu::Exception(FUNC) << msg << s << i;
    i = j + 1;
    j = findNonInt(s, i);
    if (s[j] != '|') throw cybozu::Exception(FUNC) << msg << s << i;
    const uint64_t gidE = cybozu::atoi(s.substr(i, j - i));
    snap.set(gidB, gidE);
    return j + 1;
}


} // namespace meta_local



MetaSnap strToMetaSnap(const std::string &s)
{
    MetaSnap snap;
    meta_local::parseMetaSnap(s, 0, snap);
    return snap;
}


/**
 * <SNAP>-TIMESTAMP or <SNAP-->SNAP>-TIMESTAMP
 * TIMESTAMP format is 'YYYYMMDDhhmmss'.
 * SNAP format is '|gid|' or '|gid,gid|'
 */
MetaState strToMetaState(const std::string &s)
{
    const char *const FUNC = __func__;
    const char *msg = "bad input string";
    if (s[0] != '<') throw cybozu::Exception(FUNC) << msg << s << 0;

    MetaSnap snapB, snapE;
    size_t pos = meta_local::parseMetaSnap(s, 1, snapB);
    const bool isApplying = s[pos] != '>';
    if (isApplying) {
        if (s.substr(pos, 3) != "-->") {
            throw cybozu::Exception(FUNC) << msg << s << pos;
        }
        pos = meta_local::parseMetaSnap(s, pos + 3, snapE);
    }
    if (s[pos] != '>') throw cybozu::Exception(FUNC) << msg << s << pos;
    pos++;
    time_t ts;
    if (s.size() == pos) {
        ts = ::time(0);
    } else {
        if (s[pos] != '-') throw cybozu::Exception(FUNC) << msg << s << pos;
        pos++;
        size_t pos2 = meta_local::findNonInt(s, pos);
        if (pos2 != s.size()) {
            throw cybozu::Exception(FUNC) << msg << s << pos;
        }
        ts = cybozu::strToUnixTime(s.substr(pos, pos2));
    }
    if (isApplying) {
        return MetaState(snapB, snapE, ts);
    } else {
        return MetaState(snapB, ts);
    }
}


} // namespace walb
