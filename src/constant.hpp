#pragma once

namespace walb {

const uint64_t KIBI = 1024ULL;
const uint64_t MEBI = 1024ULL * 1024ULL;
const uint64_t GIBI = 1024ULL * 1024ULL * 1024ULL;

const uint64_t LBS = 512;

const uint64_t DEFAULT_BULK_LB = MEBI / LBS;
const size_t DEFAULT_TIMEOUT = 60; // seconds.

const size_t DEFAULT_MAX_CONNECTIONS = 2;
const size_t DEFAULT_MAX_BACKGROUND_TASKS = 1;
const size_t DEFAULT_MAX_WDIFF_SEND_MB = 128;
const size_t DEFAULT_MAX_WLOG_SEND_MB = 128;
const size_t DEFAULT_DELAY_SEC_FOR_RETRY = 60; // seconds.
const size_t DEFAULT_RETRY_TIMEOUT = 1800; // seconds.

} // walb
