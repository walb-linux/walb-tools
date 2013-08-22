/**
 * @file
 * @brief wrapper of stdout logger.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/log.hpp"

#ifndef WALB_TOOLS_STDOUT_LOGGER_HPP
#define WALB_TOOLS_STDOUT_LOGGER_HPP

#ifndef LOG_INTERNAL
#define LOG_INTERNAL(level, fmt, args...)                               \
    ::fprintf(::stderr, level "(%s:%d)" fmt, __func__, __LINE__, ##args)
#endif

#ifndef LOGd
#ifdef DEBUG
#define LOGd(fmt, args...) LOG_INTERNAL("D", fmt, ##args)
#else
#define LOGd(fmt, args...)
#endif
#endif

#ifndef LOGi
#define LOGi(fmt, args...) LOG_INTERNAL("I", fmt, ##args)
#endif
#ifndef LOGw
#define LOGw(fmt, args...) LOG_INTERNAL("W", fmt, ##args)
#endif
#ifndef LOGe
#define LOGe(fmt, args...) LOG_INTERNAL("E", fmt, ##args)
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

#endif /* WALB_TOOLS_STDOUT_LOGGER_HPP */
