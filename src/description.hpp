#pragma once
#include <string>
#include <sstream>
#include "version.hpp"
#include "linux/walb/walb.h"
#include "util.hpp"

namespace walb {

std::string getDescription(const char *prefix);
std::string getDescriptionLtsv();

} // namespace walb
