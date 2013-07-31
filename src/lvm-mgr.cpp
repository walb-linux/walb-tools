/**
 * @file
 * @brief lvm wrapper manager.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "lvm.hpp"
#include "cybozu/option.hpp"

struct Option : public cybozu::Option
{
    std::string vgName;
    std::string lvName;
    std::string command;
    std::vector<std::string> args;
    Option() {
        appendMust(&vgName, "vg", "volume group name");
        appendOpt(&lvName, "lv", "logical volume name");
        appendParam(&command, "command", "command name");
        appendParamVec(&args, "args", "command-specified arguments");
        appendHelp("h");
    }

    /* now editing */
};

int main(int argc, char *argv[])
{
    try {



        /* now editing */



        return 0;
    } catch (...) {
        ::fprintf(::stderr, "caugut another error.\n");
        return 1;
    }
}
