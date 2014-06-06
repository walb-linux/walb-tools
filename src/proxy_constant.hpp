#pragma once
#include "state_machine.hpp"

namespace walb {

// states.
const char *const pClear = "Clear";
const char *const pStopped = "Stopped";
const char *const pStarted = "Started";

// temporary states.
const char *const ptStart = "Start";
const char *const ptStop = "Stop";
const char *const ptClearVol = "ClearVol";
const char *const ptAddArchiveInfo = "AddArchiveInfo";
const char *const ptDeleteArchiveInfo = "DeleteArchiveInfo";
const char *const ptWlogRecv = "WlogRecv";
const char *const ptWaitForEmpty = "WaitForEmpty";

const struct StateMachine::Pair statePairTbl[] = {
    { pClear, ptAddArchiveInfo },
    { ptAddArchiveInfo, pStopped },

    { pStopped, ptClearVol },
    { ptClearVol, pClear },

    { pStopped, ptAddArchiveInfo },
    { ptAddArchiveInfo, pStopped },

    { pStopped, ptDeleteArchiveInfo },
    { ptDeleteArchiveInfo, pStopped },

    { pStopped, ptDeleteArchiveInfo },
    { ptDeleteArchiveInfo, pClear },

    { pStopped, ptStart },
    { ptStart, pStarted },

    { pStarted, ptStop },
    { ptStop, pStopped },

    { pStarted, ptWlogRecv },
    { ptWlogRecv, pStarted },

    { pStarted, ptWaitForEmpty },
    { ptWaitForEmpty, pStopped },
};

const char *const ArchiveSuffix = ".archive";
const char *const ArchiveExtension = "archive";

} // namespace walb
