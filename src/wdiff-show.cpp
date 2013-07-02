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
        using DiffRecPtr = std::shared_ptr<walb::diff::WalbDiffRecord>;
        using DiffIoPtr = std::shared_ptr<walb::diff::BlockDiffIo>;
        using DiffHeaderPtr = std::shared_ptr<walb::diff::WalbDiffFileHeader>;

        walb::diff::WalbDiffReader wdiffR(0);
        DiffHeaderPtr wdiffH = wdiffR.readHeader();
        wdiffH->print();
        std::pair<DiffRecPtr, DiffIoPtr> p = wdiffR.readDiff();
        DiffRecPtr recP = p.first;
        DiffIoPtr ioP = p.second;
        while (recP) {
            if (!recP->isValid()) {
                ::printf("Invalid record: ");
            }
            recP->printOneline();
            p = wdiffR.readDiff();
            recP = p.first; ioP = p.second;
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
