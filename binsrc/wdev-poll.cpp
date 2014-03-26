/**
 * @file
 * @brief polling walb log generation.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <thread>
#include <atomic>

#include "cybozu/option.hpp"
#include "util.hpp"
#include "log_dev_monitor.hpp"
#include "walb_util.hpp"

struct Option : public cybozu::Option
{
    std::vector<std::string> wdevNameV;
    Option() {
        setUsage("Usage: wdev-poll [wdev name list]");
        appendVec(&wdevNameV, "i", "wdev name list");
        appendHelp("h");
    }
    bool parse(int argc, char *argv[]) {
        return cybozu::Option::parse(argc, argv);
    }
};

void pollWorker(std::atomic<bool> &flag, walb::LogDevMonitor &monitor)
{
    try {
        while (!flag.load()) {
            auto v = monitor.poll(1000);
            ::printf("got %zu\n", v.size());
            for (std::string &s : v) {
                ::printf("wdev %s\n", s.c_str());
            }
        }
    } catch (...) {
    }
}

size_t nonSpacePos(const std::string &s, size_t pos)
{
    size_t i = pos;
    while (i < s.size()) {
        if (s[i] != ' ') break;
        ++i;
    }
    return i;
}

std::vector<std::string> commandReader(FILE *fp)
{
    char buf[1024];
    if (::fgets(buf, 1024, fp) == nullptr) {
        ::printf("fgets failed.\n");
        return {"quit"};
    }
    int l = ::strlen(buf);
    if (0 < l && buf[l - 1] == '\n') {
        buf[l - 1] = '\0';
    }
    std::string s(buf);
    std::vector<std::string> v;

    size_t n = s.find(" ");
    while (n != std::string::npos) {
        v.push_back(s.substr(0, n));
        s = s.substr(nonSpacePos(s, n));
        n = s.find(" ");
    }
    if (!s.empty()) {
        v.push_back(s);
    }
    return v;
}

bool commandRunner(const std::vector<std::string> &cmds, walb::LogDevMonitor &monitor)
{
    if (cmds.empty() || cmds[0] == "quit") return false;

    const std::string &cmdType = cmds[0];
    std::string wdevName;
    if (1 <= cmds.size()) {
        wdevName = cmds[1];
    }
    if (cmdType == "addForce") {
        ::printf("addForce '%s'(%zu)\n", wdevName.c_str(), wdevName.size());
        if (!monitor.addForce(wdevName)) {
            ::printf("addForce failed.\n");
        }
    } else if (cmdType == "add") {
        ::printf("add '%s'(%zu)\n", wdevName.c_str(), wdevName.size());
        if (!monitor.add(wdevName)) {
            ::printf("add failed.\n");
        }
    } else if (cmdType == "del") {
        ::printf("del '%s'(%zu)\n", wdevName.c_str(), wdevName.size());
        monitor.del(wdevName);
    } else if (cmdType == "list") {
        for (auto &p : monitor.list()) {
            ::printf("%s %d\n", p.first.c_str(), p.second);
        }
    } else {
        ::printf("bad command: '%s' %zu\n", cmdType.c_str(), cmdType.size());
        return false;
    }
    return true;
}

int main(int argc, char *argv[])
{
    try {
        Option opt;
        if (!opt.parse(argc, argv)) {
            return 1;
        }
        walb::util::setLogSetting("-", false);

        walb::LogDevMonitor monitor;
        std::atomic<bool> flag(false);
        std::thread th(pollWorker, std::ref(flag), std::ref(monitor));
        while (true) {
            std::vector<std::string> cmds = commandReader(::stdin);
            if (!commandRunner(cmds, monitor)) {
                break;
            }
        }
        flag.store(true);
        th.join();
        return 0;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "exception: %s\n", e.what());
    } catch (...) {
        ::fprintf(::stderr, "caught an error.");
    }
    return 1;
}
/* end of file. */
