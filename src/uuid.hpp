#pragma once
/**
 * @file
 * @brief Uuid utility.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "hex_byte_array.hpp"
#include "walb/util.h"

namespace cybozu {

class Uuid : public HexByteArrayT<UUID_SIZE, Uuid> {};

} //namespace cybozu
