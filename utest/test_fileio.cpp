#include "cybozu/test.hpp"
#include "cybozu/serializer.hpp"
#include "file_path.hpp"
#include "fileio.hpp"
#include "fileio_serializer.hpp"

CYBOZU_TEST_AUTO(serialize)
{
    cybozu::FilePath fp("test0.bin");

    cybozu::util::FileWriter writer(fp.str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    int a0 = 5;
    cybozu::save(writer, a0);
    writer.close();

    cybozu::util::FileReader reader(fp.str(), O_RDONLY);
    int a1;
    cybozu::load(a1, reader);

    CYBOZU_TEST_EQUAL(a0, a1);

    fp.unlink();
}
