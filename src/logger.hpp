/**
 * @file
 * @brief logger wrapper.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/log.hpp"

#ifndef WALB_TOOLS_LOGGER_HPP
#define WALB_TOOLS_LOGGER_HPP

#ifdef LOGd
#undef LOGd
#endif
#ifdef LOGi
#undef LOGi
#endif
#ifdef LOGw
#undef LOGw
#endif
#ifdef LOGe
#undef LOGe
#endif
#ifdef LOGd_
#undef LOGd_
#endif
#ifdef LOGi_
#undef LOGi_
#endif
#ifdef LOGw_
#undef LOGw_
#endif
#ifdef LOGe_
#undef LOGe_
#endif

#ifdef DEBUG
#define LOGd(fmt, args...)                                              \
    cybozu::PutLog(cybozu::LogDebug, "(%s:%d)" fmt, __func__, __LINE__, ##args)
#else
#define LOGd(fmt, args...)
#endif

#define LOGi(fmt, args...) cybozu::PutLog(cybozu::LogInfo, fmt, ##args)
#define LOGw(fmt, args...) cybozu::PutLog(cybozu::LogWarning, fmt, ##args)
#define LOGe(fmt, args...) cybozu::PutLog(cybozu::LogError, fmt, ##args)

#define LOGd_(fmt, args...)
#define LOGi_(fmt, args...)
#define LOGw_(fmt, args...)
#define LOGe_(fmt, args...)

#endif /* WALB_TOOLS_LOGGER_HPP */
