#pragma once
/**
 * @file
 * @brief Temporary file serializer.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/stream_fwd.hpp"
#include "tmp_file.hpp"
#include "fileio.hpp"

namespace cybozu {

template <>
struct InputStreamTag<TmpFile>
{
    static inline size_t readSome(TmpFile &is, void *buf, size_t size) {
        util::File file(is.fd());
        return file.readsome(buf, size);
    }
};

template <>
struct OutputStreamTag<TmpFile>
{
    static inline void write(TmpFile &os, const void *buf, size_t size) {
        util::File file(os.fd());
        file.write(buf, size);
    }
};

} //namespace cybozu
