#pragma once
/**
 * @file
 * @brief Serializer to/from strings.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/serializer.hpp"
#include <string>

namespace cybozu {

template <class T>
void saveToStr(std::string &s, const T &t)
{
    cybozu::StringOutputStream os(s);
    cybozu::save(os, t);
}

template <class T>
void loadFromStr(T &t, const std::string &s)
{
    cybozu::StringInputStream is(s);
    cybozu::load(t, is);
}

} //namespace cybozu
