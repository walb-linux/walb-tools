#pragma once

#include <type_traits>

namespace cybozu {

template <typename S1, typename S2>
inline bool isOverlapped(S1 addr0, S2 size0, S1 addr1, S2 size1)
{
    static_assert(std::is_integral<S1>::value, "S1 must be integral type.");
    static_assert(std::is_integral<S2>::value, "S2 must be integral type.");
    return addr0 < addr1 + size1 && addr1 < addr0 + size0;
}

template <typename S1, typename S2>
inline bool isOverwritten(S1 addr0, S2 size0, S1 addr1, S2 size1)
{
    static_assert(std::is_integral<S1>::value, "S1 must be integral type.");
    static_assert(std::is_integral<S2>::value, "S2 must be integral type.");
    return addr1 <= addr0 && addr0 + size0 <= addr1 + size1;
}

} // cybozu
