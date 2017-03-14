#pragma once

namespace walb {

const uint64_t KIBI = 1ULL << 10;
const uint64_t MEBI = 1ULL << 20;
const uint64_t GIBI = 1ULL << 30;

const uint64_t KILO = 1000ULL;
const uint64_t MEGA = KILO * 1000ULL;
const uint64_t GIGA = MEGA * 1000ULL;

const uint64_t LBS = 512;

const uint64_t DEFAULT_BULK_LB = 64 * KIBI / LBS;
const uint64_t MAX_BULK_SIZE = 4 * MEBI;
const size_t DEFAULT_TIMEOUT_SEC = 60;

const size_t DEFAULT_MAX_CONNECTIONS = 10;
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

const uint64_t DEFAULT_FULL_SCAN_BYTES_PER_SEC = 0; // unlimited.

const uint64_t DEFAULT_FSYNC_INTERVAL_SIZE = 128 * MEBI;
const size_t DEFAULT_MERGE_BUFFER_LB = 4 * MEBI / LBS;

const char DEFAULT_DISCARD_TYPE_STR[] = "ignore";

const uint64_t DIRTY_HASH_SYNC_READ_AHEAD_LB = 256 * MEBI / LBS;
const uint64_t DIRTY_HASH_SYNC_MAX_PACK_AREA_LB = 256 * MEBI / LBS;

const int DEFAULT_TCP_KEEPIDLE = 60 * 30;
const int DEFAULT_TCP_KEEPINTVL = 60;
const int DEFAULT_TCP_KEEPCNT = 10;
const int MAX_TCP_KEEPIDLE = 60 * 60 * 24;
const int MAX_TCP_KEEPINTVL = 60 * 60;
const int MAX_TCP_KEEPCNT = 100;

const size_t PROGRESS_INTERVAL_SEC = 60;

const size_t DEFAULT_TS_DELTA_INTERVAL_SEC = 60;

const uint32_t DEFAULT_MAX_WDIFF_IO_BLOCKS = 64 * 1024;

const size_t DEFAULT_IMPLICIT_SNAPSHOT_INTERVAL_SEC = 10;

} // walb
