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

#ifndef WALB_TOOLS_TMP_FILE_SERIALIZER_HPP
#define WALB_TOOLS_TMP_FILE_SERIALIZER_HPP

namespace cybozu {

template <>
struct InputStreamTag<TmpFile>
{
    static inline size_t read(TmpFile &is, char *buf, size_t size) {
        util::FdReader fdr(is.fd());
        return fdr.readsome(buf, size);
    }
};

template <>
struct OutputStreamTag<TmpFile>
{
    static inline void write(TmpFile &os, const char *buf, size_t size) {
        util::FdWriter fdw(os.fd());
        fdw.write(buf, size);
    }
};

} //namespace cybozu

#endif /* WALB_TOOLS_TMP_FILE_SERIALIZER_HPP */
