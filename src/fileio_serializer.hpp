#pragma once
/**
 * @file
 * @brief Temporary file serializer.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/stream_fwd.hpp"
#include "fileio.hpp"

namespace cybozu {

#define WALB_FILEIO_DEFINE_SERIALIZE_LOADER(type)                       \
    template <>                                                         \
    struct InputStreamTag<type>                                         \
    {                                                                   \
        static inline size_t readSome(type &is, void *buf, size_t size) { \
            return is.readsome(buf, size);                              \
        }                                                               \
    }

WALB_FILEIO_DEFINE_SERIALIZE_LOADER(util::FdOperator);
WALB_FILEIO_DEFINE_SERIALIZE_LOADER(util::FdReader);
WALB_FILEIO_DEFINE_SERIALIZE_LOADER(util::FileOperator);
WALB_FILEIO_DEFINE_SERIALIZE_LOADER(util::FileReader);

#undef WALB_FILEIO_DEFINE_SERIALIZE_LOADER

#define WALB_FILEIO_DEFINE_SERIALIZE_SAVER(type)                        \
    template <>                                                         \
    struct OutputStreamTag<type>                                        \
    {                                                                   \
        static inline void write(type &os, const void *buf, size_t size) { \
            os.write(buf, size);                                        \
        }                                                               \
    }

WALB_FILEIO_DEFINE_SERIALIZE_SAVER(util::FdOperator);
WALB_FILEIO_DEFINE_SERIALIZE_SAVER(util::FdWriter);
WALB_FILEIO_DEFINE_SERIALIZE_SAVER(util::FileOperator);
WALB_FILEIO_DEFINE_SERIALIZE_SAVER(util::FileWriter);

#undef WALB_FILEIO_DEFINE_SERIALIZE_SAVER

} //namespace cybozu
