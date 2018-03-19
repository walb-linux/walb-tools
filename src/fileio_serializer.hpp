#pragma once
/**
 * @file
 * @brief Temporary file serializer.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/stream.hpp"
#include "fileio.hpp"

namespace cybozu {


template <>
inline size_t readSome<util::File>(void *buf, size_t size, util::File& is)
{
    return is.readsome(buf, size);
}


template <>
inline void write<util::File>(util::File& os, const void *buf, size_t size)
{
    os.write(buf, size);
}


} //namespace cybozu
