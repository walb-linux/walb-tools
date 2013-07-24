#include "cybozu/test.hpp"
#include "tmp_file.hpp"
#include "file_path.hpp"

CYBOZU_TEST_AUTO(test)
{
    /* Create a tmp file. */
    cybozu::TmpFile tmpFile0(".");
    CYBOZU_TEST_EQUAL(tmpFile0.path().substr(0, 3), "tmp");

    /* Write an integer. */
    cybozu::util::FdWriter fdw(tmpFile0.fd());
    uint64_t a = 5;
    fdw.write(&a, sizeof(a));

    /* Save as a name. */
    cybozu::FilePath fp("test0.bin");
    tmpFile0.save(fp.str());
    CYBOZU_TEST_ASSERT(fp.stat().isFile());
    CYBOZU_TEST_EQUAL(fp.stat().size(), sizeof(a));

    /* Check the written data. */
    cybozu::util::FileOpener fo(fp.str(), O_RDONLY);
    cybozu::util::FdReader fdr(fo.fd());
    uint64_t b;
    fdr.read(&b, sizeof(b));
    CYBOZU_TEST_EQUAL(a, b);
    fo.close();

    fp.unlink();
}
