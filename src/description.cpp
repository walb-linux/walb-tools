#include "description.hpp"

namespace walb {


std::string getDescription(const char *prefix)
{
    return cybozu::util::formatString(
        "%s version %s build at %s (wlog version %d)\n"
#ifndef DISABLE_COMMIT_ID
        "commit %s\n"
#endif
        , prefix
        , getWalbToolsVersion()
        , getWalbToolsBuildDate()
        , WALB_LOG_VERSION
#ifndef DISABLE_COMMIT_ID
        , getWalbToolsCommitId()
#endif
        );
}


std::string getDescriptionLtsv()
{
    std::stringstream ss;
    ss << "version:" << getWalbToolsVersion();
    ss << "\t" << "build_date:" << getWalbToolsBuildDate();
    ss << "\t" << "log_version:" << WALB_LOG_VERSION;
#ifndef DISABLE_COMMIT_ID
    ss << "\t" << "commit:" << getWalbToolsCommitId();
#endif
    return ss.str();
}


} // namespace walb
