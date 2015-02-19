#pragma once
#include "walb_types.hpp"
#include "state_machine.hpp"

namespace walb {

// states.
// All the these states will be stored in files except for 'Clear'.
const char *const sClear = "Clear";
const char *const sSyncReady = "SyncReady";
const char *const sStopped = "Stopped";
const char *const sTarget = "Target";
const char *const sStandby = "Standby";
const StrVec sSteadyStates = { sClear, sSyncReady, sStopped, sTarget, sStandby };

// temporary states.
const char *const stInitVol = "InitVol";
const char *const stClearVol = "ClearVol";
const char *const stStartStandby = "StartStandby";
const char *const stStopStandby = "StopStandby";
const char *const stFullSync = "FullSync";
const char *const stHashSync = "HashSync";
const char *const stStartTarget = "StartTarget";
const char *const stStopTarget = "StopTarget";
const char *const stReset = "Reset";

const struct StateMachine::Pair statePairTbl[] = {
    { sClear, stInitVol },
    { stInitVol, sSyncReady },
    { sSyncReady, stClearVol },
    { stClearVol, sClear },

    { sSyncReady, stStartStandby },
    { stStartStandby, sStandby },
    { sStandby, stStopStandby },
    { stStopStandby, sSyncReady },

    { sSyncReady, stFullSync },
    { stFullSync, sStopped },
    { sSyncReady, stHashSync },
    { stHashSync, sStopped },
    { sStopped, stReset },
    { stReset, sSyncReady },

    { sStopped, stStartTarget },
    { stStartTarget, sTarget },
    { sTarget, stStopTarget },
    { stStopTarget, sStopped },
};

// action
const char *const saWlogSend = "WlogSend";
const char *const saWlogRemove = "WlogRemove";

const StrVec allActionVec = {saWlogSend, saWlogRemove};
const StrVec sAcceptForStop = { sTarget, sStandby };
const StrVec sAcceptForSnapshot = { sTarget, sStopped };
// action = WlogSend + WlogRemove
const StrVec sAcceptForWlogAction = {sTarget, stFullSync, stHashSync, sStandby};
const StrVec sAcceptForResize = {sSyncReady, sStopped, sTarget, sStandby};

} // namespace walb
