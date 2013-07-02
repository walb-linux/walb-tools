#include "simple_logger.hpp"

int main()
{
    cybozu::logger::SimpleLogger::setPath("log");
    LOGn("log test1.");
    LOGe("log test2.");

    return 0;
}
