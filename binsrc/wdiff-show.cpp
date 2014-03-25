/**
 * @file
 * @brief Show walb diff file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <cstdio>

#include "util.hpp"
#include "walb_diff_file.hpp"

int main()
try {
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
} catch (std::exception &e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
    return 1;
} catch (...) {
    ::fprintf(::stderr, "caught other error.\n");
    return 1;
}

