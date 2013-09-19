#pragma once
/**
 * @file
 * @brief wrapper of cybozu logger (for syslog).
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/log.hpp"

#ifndef LOGd
#ifdef DEBUG
#define LOGd(fmt, args...)                                              \
    cybozu::PutLog(cybozu::LogDebug, "(%s:%d)" fmt, __func__, __LINE__, ##args)
#else
#define LOGd(fmt, args...)
#endif
#endif

#ifndef LOGi
#define LOGi(fmt, args...) cybozu::PutLog(cybozu::LogInfo, fmt, ##args)
#endif
#ifndef LOGw
#define LOGw(fmt, args...) cybozu::PutLog(cybozu::LogWarning, fmt, ##args)
#endif
#ifndef LOGe
#define LOGe(fmt, args...) cybozu::PutLog(cybozu::LogError, fmt, ##args)
#endif

#ifndef LOGd_
#define LOGd_(fmt, args...)
#endif
#ifndef LOGi_
#define LOGi_(fmt, args...)
#endif
#ifndef LOGw_
#define LOGw_(fmt, args...)
#endif
#ifndef LOGe_
#define LOGe_(fmt, args...)
#endif
