/**
 * @file
 * @brief Show walb diff file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "util.hpp"
#include "walb_diff_file.hpp"

int doMain(int, char *[])
{
    /* Read a wdiff file and show the contents. */
    walb::diff::Reader wdiffR(0);
    walb::DiffFileHeader wdiffH;
    wdiffR.readHeader(wdiffH);
    wdiffH.print();

    /* now editing */
    walb::DiffRecord rec;
    walb::DiffIo io;
    while (wdiffR.readDiff(rec, io)) {
        if (!rec.isValid()) {
            ::printf("Invalid record: ");
        }
        rec.printOneline();
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-show")
