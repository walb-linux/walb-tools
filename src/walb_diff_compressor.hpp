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
#include "checksum.hpp"
#include "stdout_logger.hpp"
#include <cybozu/thread.hpp>
#include <atomic>

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

namespace compressor {

/*
 * convert pack data
 * @param conv [in] PackCompressor or PackUncompressor
 * @param inPackTop [in] top address of pack data
 * @param maxOutSize max output size excluding pack header block.
 * @return buffer of converted pack data
 */
template<class Convertor>
std::unique_ptr<char[]> convert(Convertor& conv, const char *inPackTop, size_t maxOutSize)
{
    const walb_diff_pack& inPack = *(const walb_diff_pack*)inPackTop;
    std::unique_ptr<char[]> ret(new char [WALB_DIFF_PACK_SIZE + maxOutSize]);
    walb_diff_pack& outPack = *(walb_diff_pack*)ret.get();
    const char *in = inPackTop + WALB_DIFF_PACK_SIZE;
    char *out = ret.get() + WALB_DIFF_PACK_SIZE;

    memset(ret.get(), 0, WALB_DIFF_PACK_SIZE);
    uint32_t outOffset = 0;
    for (int i = 0, n = inPack.n_records; i < n; i++) {
        const walb_diff_record& inRecord = inPack.record[i];
        walb_diff_record& outRecord = outPack.record[i];

        assert(inRecord.flags & (1U << ::WALB_DIFF_FLAG_EXIST));
        if (inRecord.flags & ((1U << ::WALB_DIFF_FLAG_ALLZERO) | (1U << ::WALB_DIFF_FLAG_DISCARD))) {
            outRecord = inRecord;
        } else {
            conv.convertRecord(out, maxOutSize - outOffset, outRecord, in, inRecord);
        }
        outRecord.data_offset = outOffset;
        outOffset += outRecord.data_size;
        assert(outOffset <= maxOutSize);
        out += outRecord.data_size;
        in += inRecord.data_size;
    }
    outPack.n_records = inPack.n_records;
    outPack.total_size = outOffset;
    outPack.checksum = 0; // necessary to the following calcChecksum
    outPack.checksum = cybozu::util::calcChecksum(&outPack, WALB_DIFF_PACK_SIZE, 0);
    return ret;
}

inline uint32_t calcTotalBlockNum(const walb_diff_pack& pack)
{
    uint32_t num = 0;
    for (int i = 0; i < pack.n_records; i++) {
        num += pack.record[i].io_blocks;
    }
    return num;
}

struct PackCompressorBase {
	virtual ~PackCompressorBase() {}
    virtual void convertRecord(char *out, size_t maxOutSize, walb_diff_record& outRecord, const char *in, const walb_diff_record& inRecord) = 0;
    virtual std::unique_ptr<char[]> convert(const char *inPackTop) = 0;
};

} // compressor

class PackCompressor : public compressor::PackCompressorBase {
    int type_;
    walb::Compressor c_;
public:
    PackCompressor(int type, size_t compressionLevel = 0)
        : type_(type), c_(convertCompressionType(type), compressionLevel)
    {
    }
    void convertRecord(char *out, size_t maxOutSize, walb_diff_record& outRecord, const char *in, const walb_diff_record& inRecord)
    {
        outRecord = inRecord;
        const size_t inSize = inRecord.data_size;
        size_t encSize = inSize;
        try {
            encSize = c_.run(out, maxOutSize, in, inSize);
        } catch (std::bad_alloc&) {
            throw;
        } catch (std::exception& e) {
            LOGd("encode error %s\n", e.what());
            // through
        }
        if (encSize < inSize) {
            outRecord.compression_type = type_;
            outRecord.data_size = encSize;
        } else {
            // not compress
            outRecord.compression_type = WALB_DIFF_CMPR_NONE;
            ::memcpy(out, in, inSize);
        }
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
        return compressor::convert(*this, inPackTop, inPack.total_size);
    }
};

class PackUncompressor : public compressor::PackCompressorBase {
    int type_;
    walb::Uncompressor d_;
public:
    PackUncompressor(int type, size_t para = 0)
        : type_(type), d_(convertCompressionType(type), para)
    {
    }
    void convertRecord(char *out, size_t maxOutSize, walb_diff_record& outRecord, const char *in, const walb_diff_record& inRecord)
    {
        outRecord = inRecord;
        const size_t inSize = inRecord.data_size;
        if (inRecord.compression_type == WALB_DIFF_CMPR_NONE) {
            if (inSize > maxOutSize) throw cybozu::Exception("PackUncompressor:convertRecord:small maxOutSize") << inSize << maxOutSize;
            ::memcpy(out, in, inSize);
            return;
        }
        size_t decSize = d_.run(out, maxOutSize, in, inSize);
        outRecord.compression_type = WALB_DIFF_CMPR_NONE;
        outRecord.data_size = decSize;
        assert(decSize == outRecord.io_blocks * 512);
        outRecord.checksum = cybozu::util::calcChecksum(out, outRecord.data_size, 0);
    }
    /*
     * uncompress pack data
     * @param inPackTop [in] top address of pack data
     * @return buffer of compressed pack data
     */
    std::unique_ptr<char[]> convert(const char *inPackTop)
    {
        const walb_diff_pack& inPack = *(const walb_diff_pack*)inPackTop;
        const size_t uncompressedSize = compressor::calcTotalBlockNum(inPack) * 512;
        return compressor::convert(*this, inPackTop, uncompressedSize);
    }
};

class ConverterQueue {
	size_t queueNum_;
	size_t threadNum_;
	std::vector<compressor::PackCompressorBase*> converter_;
	std::atomic<bool> quit_;
public:
	ConverterQueue(size_t queueNum, size_t threadNum, bool doCompress, int type, size_t para = 0)
		: queueNum_(queueNum)
		, threadNum_(threadNum)
		, converter_(queueNum)
		, quit_(false)
	{
		for (size_t i = 0; i < queueNum; i++) {
			if (doCompress) {
				converter_[i] = new PackCompressor(type, para);
			} else {
				converter_[i] = new PackUncompressor(type, para);
			}
		}
	}
	~ConverterQueue()
	{
		for (size_t i = 0; i < queueNum_; i++) {
			delete converter_[i];
		}
	}
};

} //namespace walb

