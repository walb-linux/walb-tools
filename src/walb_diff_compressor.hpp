#pragma once
/**
 * @file
 * @brief parallel compressor/uncompressor class
 * @author MITSUNARI Shigeo
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "compressor.hpp"
#include <memory>
#include <vector>
#include "walb_diff.h"
#include "logger.hpp"
#include "checksum.hpp"

namespace walb {

inline Compressor::Mode convertCompressionType(int type)
{
    switch (type) {
    case WALB_DIFF_CMPR_NONE: return walb::Compressor::AsIs;
    case WALB_DIFF_CMPR_GZIP: return walb::Compressor::Zlib;
    case WALB_DIFF_CMPR_SNAPPY: return walb::Compressor::Snappy;
    case WALB_DIFF_CMPR_LZMA: return walb::Compressor::Xz;
    default: throw cybozu::Exception("walb:Compressor:convertCompressionType") << type;
    }
}

class PackCompressor {
    int type_;
    walb::Compressor c_;
public:
    PackCompressor(int type, size_t maxPlainSize, size_t compressionLevel = 0)
        : type_(type), c_(convertCompressionType(type), maxPlainSize, compressionLevel)
    {
    }
    void convertRecord(char *out, walb_diff_record& outRecord, const char *in, const walb_diff_record& inRecord)
    {
        outRecord = inRecord;
        const size_t inSize = inRecord.data_size;
        size_t encSize = inSize;
        try {
            encSize = c_.run(out, in, inSize);
        } catch (std::bad_alloc&) {
            throw;
        } catch (std::exception& e) {
            LOGd("encode error %s\n", e.what());
        }
        if (encSize < inSize) {
            outRecord.compression_type = type_;
        } else {
            // not compress
            encSize = inSize;
            outRecord.compression_type = WALB_DIFF_CMPR_NONE;
            ::memcpy(out, in, inSize);
        }
        outRecord.data_size = encSize;
        outRecord.checksum = cybozu::util::calcChecksum(out, outRecord.data_size, 0);
    }
    /*
     * compress pack data
     * @param inPackTop [in] top address of pack data
     * @return buffer of compressed pack data
     */
    std::unique_ptr<char[]> convert(const char *inPackTop)
    {
        const walb_diff_pack& inPack = *(const walb_diff_pack*)inPackTop;
        std::unique_ptr<char[]> ret(new char [WALB_DIFF_PACK_SIZE + inPack.total_size]);
        walb_diff_pack& outPack = *(walb_diff_pack*)ret.get();
        const char *in = inPackTop + WALB_DIFF_PACK_SIZE;
        char *const out = ret.get() + WALB_DIFF_PACK_SIZE;

        memset(ret.get(), 0, WALB_DIFF_PACK_SIZE);
        uint32_t outOffset = 0;
        for (int i = 0, n = inPack.n_records; i < n; i++) {
            const walb_diff_record& inRecord = inPack.record[i];
            walb_diff_record& outRecord = outPack.record[i];

            convertRecord(out, outRecord, in, inRecord);
            outRecord.data_offset = outOffset;
            outOffset += outRecord.data_size;
        }
        outPack.total_size = outOffset;
        outPack.checksum = 0;
        outPack.checksum = cybozu::util::calcChecksum(&outPack, outOffset, 0);
        return ret;
    }
};

} //namespace walb

