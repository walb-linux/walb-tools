#pragma once

namespace walb {

const uint64_t KIBI = 1024ULL;
const uint64_t MEBI = 1024ULL * 1024ULL;
const uint64_t GIBI = 1024ULL * 1024ULL * 1024ULL;

const uint64_t LBS = 512;

const uint64_t DEFAULT_BULK_LB = 64 * KIBI / LBS;
const uint64_t MAX_BULK_SIZE = 4 * MEBI;
const size_t DEFAULT_TIMEOUT_SEC = 60;

const size_t DEFAULT_MAX_FOREGROUND_TASKS = 2;
const size_t DEFAULT_MAX_BACKGROUND_TASKS = 1;
const size_t DEFAULT_MAX_WDIFF_SEND_MB = 128;
const size_t DEFAULT_MAX_WDIFF_SEND_NR = 1000;
const size_t DEFAULT_MAX_WDIFF_MERGE_MB = 1024;
const size_t DEFAULT_MAX_WLOG_SEND_MB = 128;
const size_t DEFAULT_MAX_CONVERSION_MB = 1024;
const size_t DEFAULT_DELAY_SEC_FOR_RETRY = 20;
const size_t DEFAULT_RETRY_TIMEOUT_SEC = 1800;

const size_t PROXY_HEARTBEAT_INTERVAL_SEC = 10;
const size_t PROXY_HEARTBEAT_SOCKET_TIMEOUT_SEC = 3; // seconds.

const size_t DEFAULT_MAX_IO_LB = MEBI / LBS; // used as max diff IO size.

const size_t DEFAULT_SOCKET_TIMEOUT_SEC = 10;

const uint64_t DEFAULT_FSYNC_INTERVAL_SIZE = 128 * MEBI;
const size_t DEFAULT_MERGE_BUFFER_LB = 4 * MEBI / LBS;

const char DEFAULT_DISCARD_TYPE_STR[] = "ignore";

const uint64_t DIRTY_HASH_SYNC_READ_AHEAD_LB = 256 * MEBI / LBS;
const uint64_t DIRTY_HASH_SYNC_MAX_PACK_AREA_LB = 256 * MEBI / LBS;

} // walb
