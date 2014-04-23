#include "cybozu/test.hpp"
#include "cybozu/serializer.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "file_path.hpp"
#include "fileio.hpp"
#include "fileio_serializer.hpp"

CYBOZU_TEST_AUTO(tmpfile)
{
    /* Create a tmp file. */
    cybozu::TmpFile tmpFile0(".");
    CYBOZU_TEST_EQUAL(tmpFile0.path().substr(0, 3), "tmp");

    /* Write an integer. */
    cybozu::util::File fw(tmpFile0.fd());
    uint64_t a = 5;
    fw.write(&a, sizeof(a));

    /* Save as a name. */
    cybozu::FilePath fp("test0.bin");
    tmpFile0.save(fp.str());
    CYBOZU_TEST_ASSERT(fp.stat().isFile());
    CYBOZU_TEST_EQUAL(fp.stat().size(), sizeof(a));

    /* Check the written data. */
    cybozu::util::File fr(fp.str(), O_RDONLY);
    uint64_t b;
    fr.read(&b, sizeof(b));
    CYBOZU_TEST_EQUAL(a, b);
    fr.close();

    fp.unlink();
}

CYBOZU_TEST_AUTO(serialize)
{
    cybozu::TmpFile tmpFile0(".");
    int a0 = 0;
    uint64_t b0 = 310298708ULL;
    bool c0 = true;
    cybozu::save(tmpFile0, a0);
    cybozu::save(tmpFile0, b0);
    cybozu::save(tmpFile0, c0);
    cybozu::FilePath fp("test0.bin");
    tmpFile0.save(fp.str());

    cybozu::util::File reader(fp.str(), O_RDONLY);
    int a1;
    uint64_t b1;
    bool c1;
    cybozu::load(a1, reader);
    cybozu::load(b1, reader);
    cybozu::load(c1, reader);

    CYBOZU_TEST_EQUAL(a0, a1);
    CYBOZU_TEST_EQUAL(b0, b1);
    CYBOZU_TEST_EQUAL(c0, c1);

    fp.unlink();
}
