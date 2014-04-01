#pragma once
#include "queue_file.hpp"
#include "serializer.hpp"

class QueueFilePlus /*final*/ : public cybozu::util::QueueFile
{
public:
    using QueueFile::QueueFile;

    template <typename T>
    void saveFront(const T& t) {
        std::string s;
        cybozu::saveToStr(s, t);
        cybozu::util::QueueFile::pushFront(s);
    }
    template <typename T>
    void saveBack(const T& t) {
        std::string s;
        cybozu::saveToStr(s, t);
        cybozu::util::QueueFile::pushBack(s);
    }
    template <typename T>
    void loadFront(T& t) {
        std::string s;
        cybozu::util::QueueFile::front(s);
        cybozu::loadFromStr(t, s);
    }
    template <typename T>
    void loadBack(T& t) {
        std::string s;
        cybozu::util::QueueFile::back(s);
        cybozu::loadFromStr(t, s);
    }

    // remove confusiong member functions.
    template <typename T>
    void pushFront(const T&) = delete;
    template <typename T>
    void pushBack(const T&) = delete;

    template <typename T>
    void front(T&) const = delete;
    template <typename T>
    void back(T&) const = delete;

    using ConstIteratorBase = cybozu::util::QueueFile::ConstIterator;
    struct ConstIterator : public ConstIteratorBase
    {
        using ConstIteratorBase::ConstIteratorBase;

        template <typename T>
        void load(T& t) const {
            std::string s;
            ConstIteratorBase::get(s);
            cybozu::loadFromStr(t, s);
        }
        template <typename T>
        void get(T& t) const = delete;
    };
    ConstIterator cbegin() const { return ConstIterator(this, beginOffset()); }
    ConstIterator cend() const { return ConstIterator(this, endOffset()); }
    ConstIterator begin() const { return cbegin(); }
    ConstIterator end() const { return cend(); }
};
