#pragma once
/**
 * @file
 * @brief Temporary file serializer.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/stream.hpp"
#include "tmp_file.hpp"
#include "fileio.hpp"

namespace cybozu {


template <>
inline size_t readSome<TmpFile>(void *buf, size_t size, TmpFile& is)
{
    util::File file(is.fd());
    return file.readsome(buf, size);
}


template <>
inline void write<TmpFile>(TmpFile& os, const void *buf, size_t size)
{
    util::File file(os.fd());
    file.write(buf, size);
}


} //namespace cybozu
