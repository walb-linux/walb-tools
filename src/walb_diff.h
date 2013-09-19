#pragma once
/**
 * @file
 * @brief walb diff utiltities for files.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdint>
#include "walb/util.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Wdiff file format.
 *
 * [4KiB: walb_diff_file_header]
 * [[4KiB: walb_diff_pack, [walb_diff_record, ...]]
 *  [compressed IO data, ...], ...]
 * [4KiB: walb_diff_pack: end flag on]
 *
 * All IOs are sorted by address.
 * There is no overlap of IO range.
 */

/**
 * Walb diff flag bit indicators.
 *
 * ALLZERO and DISCARD is exclusive.
 */
enum {
    WALB_DIFF_FLAG_EXIST_SHIFT = 0,
    WALB_DIFF_FLAG_ALLZERO_SHIFT,
    WALB_DIFF_FLAG_DISCARD_SHIFT,
    WALB_DIFF_FLAGS_SHIFT_MAX,
};

#define WALB_DIFF_FLAG(name) (1U << WALB_DIFF_FLAG_ ## name ## _SHIFT)

/**
 * Walb diff compression type.
 */
enum {
    WALB_DIFF_CMPR_NONE = 0,
    WALB_DIFF_CMPR_GZIP,
    WALB_DIFF_CMPR_SNAPPY,
    WALB_DIFF_CMPR_LZMA,
    WALB_DIFF_CMPR_MAX
};

/**
 * Walb diff metadata record for an IO.
 *
 * If the flags is 0, the record is invalid.
 */
struct walb_diff_record
{
    uint64_t io_address; /* [logical block] */
    uint16_t io_blocks; /* [logical block] */
    uint8_t flags; /* see WALB_DIFF_FLAG_XXX. */
    uint8_t compression_type; /* see WALB_DIFF_CMPR_XXX. */
    uint32_t data_offset; /* [byte] */
    uint32_t data_size; /* [byte] */
    uint32_t checksum; /* compressed data checksum with salt 0. */
} __attribute__((packed));

/**
 * Walb diff file header.
 */
struct walb_diff_file_header
{
    uint32_t checksum;       /* header block checksum. salt is 0. */
    uint16_t max_io_blocks;  /* Max io_blocks inside the diff.
                                This is used for overlapped check. */
    uint16_t reserved1;
    uint8_t uuid[UUID_SIZE]; /* Identifier of the target block device. */

    /* Remaining area of 4KiB may be used by application. */

} __attribute__((packed));

/**
 * Flag bits of walb_diff_pack.flags.
 */
enum
{
    WALB_DIFF_PACK_END = 0,
};

/**
 * Walb record pack.
 * 4KB data.
 */
struct walb_diff_pack
{
    uint32_t checksum; /* pack block (4KiB) checksum. salt is 0. */
    uint16_t n_records;
    uint8_t flags;
    uint8_t reserved0;
    uint32_t total_size; /* [byte]. whole pack size is
                            WALB_DIFF_PACK_SIZE + total_size. */
    uint32_t reserved1;
    struct walb_diff_record record[0];
} __attribute__((packed));

const unsigned int WALB_DIFF_PACK_SIZE = 4096; /* 4KiB */
const unsigned int MAX_N_RECORDS_IN_WALB_DIFF_PACK =
    (WALB_DIFF_PACK_SIZE - sizeof(struct walb_diff_pack)) / sizeof(struct walb_diff_record);
const unsigned int WALB_DIFF_PACK_MAX_SIZE = 32 * 1024 * 1024; /* 32MiB */

#ifdef __cplusplus
}
#endif
