#pragma once
#include "walb_types.hpp"
#include "state_machine.hpp"

namespace walb {

/**
 * States.
 * All the these states will be stored in files except for 'Clear'.
 */
const char *const aClear = "Clear";
const char *const aSyncReady = "SyncReady";
const char *const aArchived = "Archived";
const char *const aStopped = "Stopped";
const StrVec aSteadyStates = { aClear, aSyncReady, aArchived, aStopped };

/**
 * Temporary states.
 */
const char *const atInitVol = "InitVol";
const char *const atClearVol = "ClearVol";
const char *const atResetVol = "ResetVol";
const char *const atFullSync = "FullSync";
const char *const atHashSync = "HashSync";
const char *const atWdiffRecv = "WdiffRecv";
const char *const atReplSync = "ReplSyncAsServer";
const char *const atStop = "Stop";
const char *const atStart = "Start";

const struct StateMachine::Pair statePairTbl[] = {
    { aClear, atInitVol },
    { atInitVol, aSyncReady },
    { aSyncReady, atClearVol },
    { atClearVol, aClear },

    { aSyncReady, atFullSync },
    { atFullSync, aArchived },

    { aArchived, atHashSync },
    { atHashSync, aArchived },
    { aArchived, atWdiffRecv },
    { atWdiffRecv, aArchived },
    { aArchived, atReplSync },
    { atReplSync, aArchived },

    { aArchived, atStop },
    { atStop, aStopped },

    { aStopped, atClearVol },
    { atClearVol, aClear },
    { aStopped, atStart },
    { atStart, aArchived },

    { aStopped, atResetVol },
    { aSyncReady, atResetVol },
    { atResetVol, aSyncReady },
};

/**
 * Actions. prefix : aa
 */
const char *const aaMerge = "Merge";
const char *const aaApply = "Apply";
const char *const aaRestore = "Restore";
const char *const aaReplSync = "ReplSyncAsClient";
const char *const aaResize = "Resize";

const StrVec allActionVec = {aaMerge, aaApply, aaRestore, aaReplSync, aaResize};

const StrVec aDenyForRestore = {aaRestore, aaResize};
const StrVec aDenyForReplSyncClient = {aaRestore, aaReplSync, aaApply, aaMerge, aaResize};
const StrVec aDenyForApply = {aaRestore, aaReplSync, aaApply, aaMerge, aaResize};
const StrVec aDenyForMerge = {aaRestore, aaReplSync, aaApply, aaMerge, aaResize};
const StrVec aDenyForResize = {aaRestore, aaReplSync, aaApply, aaResize};
const StrVec aDenyForChangeSnapshot = {aaApply, aaMerge};

const StrVec aActionOnLvm = {aaRestore, aaResize};

const std::string VOLUME_PREFIX = "i_";
const std::string RESTORE_PREFIX = "r_";
const std::string RESTORE_TMP_SUFFIX = "_tmp";

const StrVec aAcceptForReplicateServer = {aSyncReady, aArchived};
const StrVec aActive = {aArchived, atHashSync, atWdiffRecv, atReplSync};
const StrVec aActiveOrStopped = {aArchived, atHashSync, atWdiffRecv, atReplSync, aStopped};
const StrVec& aAcceptForResize = aActiveOrStopped;

} // namespace walb
