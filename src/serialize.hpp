/**
 * @file
 * @brief Serializer of primitive types, string, and containers of them.
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2009 Cybozu Labs, Inc.
 */
#ifndef SERIALIZE_HPP
#define SERIALIZE_HPP

#include <map>
#include <vector>
#include <iomanip>

#include "exception_stack.hpp"

namespace cybozu {
namespace serialize {

/****************************************
 * Put functions for debug.
 ****************************************/

template <typename T1, typename T2>
static void put(const std::map<T1, T2> &map, std::ostream &os = std::cout)
{
    for (typename std::map<T1, T2>::const_iterator i = map.begin(); i != map.end(); ++i) {
        os << i->first << " -> " << i->second << "\n";
    }
}

template <typename T>
static void put(const std::vector<T> &v, std::ostream &os = std::cout)
{
    for (typename std::vector<T>::const_iterator i = v.begin(); i != v.end(); ++i) {
        os << *i << std::endl;
    }
}

template <>
void put(const std::vector<char> &v, std::ostream &os)
{
    os << std::hex;
    for (typename std::vector<char>::const_iterator i = v.begin(); i != v.end(); ++i) {
        os << std::setw(2) << std::setfill('0')
           << static_cast<int>(static_cast<unsigned char>(*i));
    }
    os << std::dec
       << "\n";
}

/**
 * ostream operators for primitive integers and std::string.
 */
template <typename T>
static std::ostream& putAsString(std::ostream& os, const T& t, char delim = '\0')
{
    std::ostream& ret = os << t << delim;
    if (os.fail()) {
        THROW_ERROR_STACK("putAsString failed.");
    }
	return ret;
}

/**
 * istream operators for primitive integers.
 */
template <typename T>
static void getAsString(T &out, std::istream &is, char delim = '\0')
{
	std::string line;
	std::getline(is, line, delim);
	std::istringstream(line) >> out;
	if (is.fail()) {
        THROW_ERROR_STACK("getAsString failed.");
    }
}

/**
 * istream operators for std::string.
 */
template <>
void getAsString(std::string& out, std::istream& is, char delim)
{
	std::getline(is, out, delim);
	if (is.fail()) {
        THROW_ERROR_STACK("getAsString failed.");
    }
}

template <typename T1, typename T2>
static void serialize(
    std::ostream &os, const std::map<T1, T2> &map,
    char delimItem = '\n', char delimPair = ' ')
{
    try {
        cybozu::serialize::putAsString(os, map.size(), delimItem);
        for (typename std::map<T1, T2>::const_iterator i = map.begin(); i != map.end(); ++i) {
            putAsString(os, i->first, delimPair);
            putAsString(os, i->second, delimItem);
        }
    } catch (cybozu::except::ExceptionStack& e) {
        RETHROW_ERROR_STACK(e, "serialize map.");
    }
}

template <typename T1, typename T2>
static void deserialize(
    std::istream &is, std::map<T1, T2> &map,
    char delimItem = '\n', char delimPair = ' ')
{
    map.clear();
	size_t size;
    try {
        cybozu::serialize::getAsString(size, is, delimItem);
        for (size_t i = 0; i < size; i++) {
            T1 key;
            T2 value;
            cybozu::serialize::getAsString(key, is, delimPair);
            cybozu::serialize::getAsString(value, is, delimItem);
            map[key] = value;
        }
    } catch (cybozu::except::ExceptionStack& e) {
        RETHROW_ERROR_STACK(e, "deserialize map");
    }
}

template <typename T>
static void serialize(std::ostream &os, const std::vector<T> &v)
{
    try {
        cybozu::serialize::putAsString(os, v.size());
        os.write(reinterpret_cast<const char *>(&v[0]), v.size() * sizeof(T));
        if (os.fail()) {
            THROW_ERROR_STACK("write failed.");
        }
    } catch (cybozu::except::ExceptionStack& e) {
        RETHROW_ERROR_STACK(e, "serialize vector.");
    }
}

template <typename T>
static void deserialize(std::istream &is, std::vector<T> &v)
{
    try {
        size_t size;
        cybozu::serialize::getAsString(size, is);
        v.resize(size);
        is.read(reinterpret_cast<char *>(&v[0]), size * sizeof(T));
        if (is.fail()) {
            THROW_ERROR_STACK("read failed.");
        }
    } catch (cybozu::except::ExceptionStack& e) {
        RETHROW_ERROR_STACK(e, "deserialize vector.");
    }
}

}} //namespace cybozu::serialize

namespace {

/****************************************
 * stream operators for a map.
 ****************************************/

template<typename T1, typename T2>
static std::ostream &operator<<(std::ostream &os, const std::map<T1, T2> &map)
{
    cybozu::serialize::serialize(os, map);
	return os;
}

template<typename T1, typename T2>
static std::istream &operator>>(std::istream &is, std::map<T1, T2> &map)
{
    cybozu::serialize::deserialize(is, map);
    return is;
}

/****************************************
 * stream operators for a vector.
 ****************************************/

template <typename T>
static std::ostream &operator<<(std::ostream &os, const std::vector<T> &v)
{
    cybozu::serialize::serialize(os, v);
    return os;
}

template <typename T>
static std::istream &operator>>(std::istream &is, std::vector<T> &v)
{
    cybozu::serialize::deserialize(is, v);
	return is;
}

} //empty namespace

#endif /* SERIALIZE_HPP */
