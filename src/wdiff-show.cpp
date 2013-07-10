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
#include "walb_diff.hpp"

int main(UNUSED int argc, UNUSED char *argv[])
{
    try{
        /* Read a wdiff file and show the contents. */
        using DiffRec = walb::diff::WalbDiffRecord;
        using DiffIo = walb::diff::BlockDiffIo;
        using DiffHeaderPtr = std::shared_ptr<walb::diff::WalbDiffFileHeader>;

        walb::diff::WalbDiffReader wdiffR(0);
        DiffHeaderPtr wdiffH = wdiffR.readHeader();
        wdiffH->print();

        /* now editing */
        DiffRec rec;
        DiffIo io;
        while (wdiffR.readDiff(rec, io)) {
            if (!rec.isValid()) {
                ::printf("Invalid record: ");
            }
            rec.printOneline();
        }
        return 0;
    } catch (std::runtime_error &e) {
        ::printf("%s\n", e.what());
        return 1;
    } catch (std::exception &e) {
        ::printf("%s\n", e.what());
        return 1;
    } catch (...) {
        ::printf("caught other error.\n");
        return 1;
    }
}

/* end of file. */
