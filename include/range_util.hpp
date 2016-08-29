#pragma once

#include <type_traits>

namespace cybozu {

inline bool isOverlapped(uint64_t addr0, uint32_t size0, uint64_t addr1, uint32_t size1)
{
    return addr0 < addr1 + size1 && addr1 < addr0 + size0;
}

inline bool isOverwritten(uint64_t addr0, uint32_t size0, uint64_t addr1, uint32_t size1)
{
    return addr1 <= addr0 && addr0 + size0 <= addr1 + size1;
}

} // cybozu
