/**
 * @file
 * @brief Temporary file serializer.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/stream_fwd.hpp"
#include "fileio.hpp"

#ifndef WALB_TOOLS_FILE_IO_SERIALIZER_HPP
#define WALB_TOOLS_FILE_IO_SERIALIZER_HPP

namespace cybozu {

#define DEFINE_SERIALIZE_LOADER(type)                                   \
    template <>                                                         \
    struct InputStreamTag<type>                                         \
    {                                                                   \
        static inline size_t readSome(type &is, void *buf, size_t size) { \
            return is.readsome(buf, size);                              \
        }                                                               \
        static inline void read(type &is, void *buf, size_t size) {     \
            return is.read(buf, size);                                  \
        }                                                               \
    }

DEFINE_SERIALIZE_LOADER(util::FdOperator);
DEFINE_SERIALIZE_LOADER(util::FdReader);
DEFINE_SERIALIZE_LOADER(util::FileOperator);
DEFINE_SERIALIZE_LOADER(util::FileReader);

#define DEFINE_SERIALIZE_SAVER(type)                                    \
    template <>                                                         \
    struct OutputStreamTag<type>                                        \
    {                                                                   \
        static inline void write(type &os, const void *buf, size_t size) { \
            os.write(buf, size);                                        \
        }                                                               \
    }

DEFINE_SERIALIZE_SAVER(util::FdOperator);
DEFINE_SERIALIZE_SAVER(util::FdWriter);
DEFINE_SERIALIZE_SAVER(util::FileOperator);
DEFINE_SERIALIZE_SAVER(util::FileWriter);

} //namespace cybozu

#endif /* WALB_TOOLS_FILE_IO_SERIALIZER_HPP */
