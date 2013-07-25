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

template <>
struct InputStreamTag<util::FdOperator>
{
    static inline size_t read(util::FdOperator &is, char *buf, size_t size) {
        return is.readsome(buf, size);
    }
};

template <>
struct InputStreamTag<util::FdReader>
{
    static inline size_t read(util::FdReader &is, char *buf, size_t size) {
        return is.readsome(buf, size);
    }
};

template <>
struct InputStreamTag<util::FileOperator>
{
    static inline size_t read(util::FileOperator &is, char *buf, size_t size) {
        return is.readsome(buf, size);
    }
};

template <>
struct InputStreamTag<util::FileReader>
{
    static inline size_t read(util::FileReader &is, char *buf, size_t size) {
        return is.readsome(buf, size);
    }
};

template <>
struct OutputStreamTag<util::FdOperator>
{
    static inline void write(util::FdOperator &os, const char *buf, size_t size) {
        os.write(buf, size);
    }
};

template <>
struct OutputStreamTag<util::FdWriter>
{
    static inline void write(util::FdWriter &os, const char *buf, size_t size) {
        os.write(buf, size);
    }
};

template <>
struct OutputStreamTag<util::FileOperator>
{
    static inline void write(util::FileOperator &os, const char *buf, size_t size) {
        os.write(buf, size);
    }
};

template <>
struct OutputStreamTag<util::FileWriter>
{
    static inline void write(util::FileWriter &os, const char *buf, size_t size) {
        os.write(buf, size);
    }
};

} //namespace cybozu

#endif /* WALB_TOOLS_FILE_IO_SERIALIZER_HPP */
