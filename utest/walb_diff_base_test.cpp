#include "cybozu/test.hpp"
#include "walb_diff_base.hpp"
#include "random.hpp"

using namespace walb;

CYBOZU_TEST_AUTO(IndexedDiffRecord)
{
    IndexedDiffRecord rec;
    rec.init();
    rec.io_address = 0;
    rec.io_blocks = 15;
    rec.setDiscard();
    rec.updateRecChecksum();

    std::vector<IndexedDiffRecord> recV = rec.split();

    CYBOZU_TEST_EQUAL(recV.size(), 4);
    CYBOZU_TEST_EQUAL(recV[0].io_address, 0U);
    CYBOZU_TEST_EQUAL(recV[1].io_address, 8U);
    CYBOZU_TEST_EQUAL(recV[2].io_address, 12U);
    CYBOZU_TEST_EQUAL(recV[3].io_address, 14U);
    CYBOZU_TEST_EQUAL(recV[0].io_blocks, 8U);
    CYBOZU_TEST_EQUAL(recV[1].io_blocks, 4U);
    CYBOZU_TEST_EQUAL(recV[2].io_blocks, 2U);
    CYBOZU_TEST_EQUAL(recV[3].io_blocks, 1U);
}
