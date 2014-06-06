#pragma once
#include "walb_types.hpp"
#include "state_machine.hpp"

namespace walb {

// states.
const char *const sClear = "Clear";
const char *const sSyncReady = "SyncReady";
const char *const sStopped = "Stopped";
const char *const sMaster = "Master";
const char *const sSlave = "Slave";

// temporary states.
const char *const stInitVol = "InitVol";
const char *const stClearVol = "ClearVol";
const char *const stStartSlave = "StartSlave";
const char *const stStopSlave = "StopSlave";
const char *const stFullSync = "FullSync";
const char *const stHashSync = "HashSync";
const char *const stStartMaster = "StartMaster";
const char *const stStopMaster = "StopMaster";
const char *const stReset = "Reset";

const struct StateMachine::Pair statePairTbl[] = {
    { sClear, stInitVol },
    { stInitVol, sSyncReady },
    { sSyncReady, stClearVol },
    { stClearVol, sClear },

    { sSyncReady, stStartSlave },
    { stStartSlave, sSlave },
    { sSlave, stStopSlave },
    { stStopSlave, sSyncReady },

    { sSyncReady, stFullSync },
    { stFullSync, sStopped },
    { sSyncReady, stHashSync },
    { stHashSync, sStopped },
    { sStopped, stReset },
    { stReset, sSyncReady },

    { sStopped, stStartMaster },
    { stStartMaster, sMaster },
    { sMaster, stStopMaster },
    { stStopMaster, sStopped },
};

// action
const char *const sWlogSend = "WlogSend";
const char *const sWlogRemove = "WlogRemove";

const StrVec allActionVec = {sWlogSend, sWlogRemove};

} // namespace walb
