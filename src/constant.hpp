#pragma once

namespace walb {

const uint64_t DEFAULT_BULK_LB = 1024 * 1024 / 512;
const size_t DEFAULT_TIMEOUT = 60; // seconds.

const size_t DEFAULT_MAX_CONNECTIONS = 2;
const size_t DEFAULT_MAX_BACKGROUND_TASKS = 1;
const size_t DEFAULT_MAX_WDIFF_SEND_MB = 128;
const size_t DEFAULT_WAIT_FOR_RETRY = 60; // seconds.
const size_t DEFAULT_RETRY_TIMEOUT = 1800; // seconds.

} // walb
