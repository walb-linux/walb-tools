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

int main(UNUSED int argc, UNUSED char *argv[])
{
    using namespace walb::diff;
    try{
        /* Read a wdiff file and show the contents. */
        walb::diff::Reader wdiffR(0);
        std::shared_ptr<FileHeaderWrap> wdiffH = wdiffR.readHeader();
        wdiffH->print();

        /* now editing */
        walb_diff_record rec;
        walb::diff::IoData io;
        while (wdiffR.readDiff(rec, io)) {
            if (!isValidRec(rec)) {
                ::printf("Invalid record: ");
            }
            printOnelineRec(rec);
        }
        return 0;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "exception: %s\n", e.what());
    } catch (...) {
        ::fprintf(::stderr, "caught other error.\n");
    }
    return 1;
}

/* end of file. */
