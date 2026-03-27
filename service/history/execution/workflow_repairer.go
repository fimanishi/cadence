//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination workflow_repairer_mock.go -self_package github.com/uber/cadence/service/history/execution

package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/uber/cadence/common/checksum"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/shard"
)

var (
	// ErrChecksumMismatchAfterRebuild indicates the rebuilt state has a different checksum than the original
	ErrChecksumMismatchAfterRebuild = errors.New("rebuilt mutable state checksum does not match original - StateRebuilder may be buggy or history tampered")

	// ErrWorkflowRepairedRetryOperation indicates repair succeeded and caller must retry.
	// This error is ALWAYS returned after successful repair, regardless of enableChecksumFailureRetry config.
	// The repaired state has been successfully persisted to the database.
	//
	// The caller MUST NOT proceed with the current in-memory mutableState because:
	// 1. It was loaded from corrupted data - any workflow logic based on it may be incorrect
	// 2. Writing it back could race with other operations that loaded the repaired state
	// 3. It lacks the fully-rebuilt internal maps and indexes from StateRebuilder
	//
	// The caller should retry the entire workflow operation, which will reload fresh state from DB.
	ErrWorkflowRepairedRetryOperation = errors.New("workflow corruption was repaired and persisted - retry operation to load fresh state")
)

type (
	// WorkflowRepairer attempts to detect and repair corrupted workflow executions
	WorkflowRepairer interface {
		// DetectAndRepairIfNeeded verifies checksum and attempts repair if corruption detected
		DetectAndRepairIfNeeded(
			ctx context.Context,
			mutableState MutableState,
			persistedChecksum checksum.Checksum,
			checksumCorrupted bool,
		) error

		// RepairWorkflow attempts to repair a corrupted workflow execution
		// If successful, the passed-in mutableState will be updated with repaired state
		RepairWorkflow(
			ctx context.Context,
			mutableState MutableState,
			corruptionType CorruptionType,
			persistedChecksum checksum.Checksum,
		) error
	}

	// CorruptionType represents the type of corruption detected
	CorruptionType int

	workflowRepairerImpl struct {
		shard          shard.Context
		stateRebuilder StateRebuilder
		logger         log.Logger
		metricsClient  metrics.Client
		scope          metrics.Scope
	}
)

const (
	CorruptionTypeNone CorruptionType = iota
	CorruptionTypeChecksumMismatch
)

var _ WorkflowRepairer = (*workflowRepairerImpl)(nil)

// NewWorkflowRepairer creates a new workflow repairer
func NewWorkflowRepairer(
	shard shard.Context,
	logger log.Logger,
	metricsClient metrics.Client,
) WorkflowRepairer {
	return &workflowRepairerImpl{
		shard:          shard,
		stateRebuilder: NewStateRebuilder(shard, logger),
		logger:         logger,
		metricsClient:  metricsClient,
		scope:          metricsClient.Scope(metrics.WorkflowCorruptionRepairScope),
	}
}

func (c CorruptionType) String() string {
	switch c {
	case CorruptionTypeNone:
		return "None"
	case CorruptionTypeChecksumMismatch:
		return "ChecksumMismatch"
	default:
		return "Unknown"
	}
}

// DetectAndRepairIfNeeded verifies checksum and attempts repair if corruption detected
func (r *workflowRepairerImpl) DetectAndRepairIfNeeded(
	ctx context.Context,
	mutableState MutableState,
	persistedChecksum checksum.Checksum,
	checksumCorrupted bool,
) error {
	var corruptionType CorruptionType
	var checksumValue checksum.Checksum
	var err error

	if checksumCorrupted {
		// Caller already verified checksum - skip redundant verification
		// Assume checksum mismatch corruption and log/metric it
		corruptionType = CorruptionTypeChecksumMismatch
		checksumValue = persistedChecksum
		err = checksum.ErrMismatch
		r.logChecksumMismatchDetected(mutableState, err)
	} else {
		// Verify checksum and analyze corruption type
		corruptionType, checksumValue, err = r.verifyChecksumAndAnalyze(mutableState, persistedChecksum)
	}

	if corruptionType == CorruptionTypeNone {
		return nil
	}

	// Corruption detected - attempt auto-repair if enabled
	domainName := mutableState.GetDomainEntry().GetInfo().Name
	if r.shard.GetConfig().EnableCorruptionAutoRepair(domainName) {
		return r.RepairWorkflow(ctx, mutableState, corruptionType, checksumValue)
	}

	// Auto-repair disabled - return the error associated with this corruption type
	return err
}

// RepairWorkflow attempts to repair a corrupted workflow execution
func (r *workflowRepairerImpl) RepairWorkflow(
	callerCtx context.Context,
	mutableState MutableState,
	corruptionType CorruptionType,
	persistedChecksum checksum.Checksum,
) (retErr error) {
	// Guard against programmer error - should never be called with CorruptionTypeNone
	if corruptionType == CorruptionTypeNone {
		return &types.InternalServiceError{
			Message: "RepairWorkflow called with CorruptionTypeNone - this is a bug",
		}
	}

	// Create repair context with its own independent timeout.
	// We use context.Background() so the timeout is not bound by the caller's deadline,
	// but we still propagate cancellation (not timeout) from the caller.
	// This ensures repair gets full repairTimeout duration even if caller times out sooner,
	// while still stopping on shard shutdown (cancellation).
	domainName := mutableState.GetDomainEntry().GetInfo().Name
	repairTimeout := r.shard.GetConfig().CorruptionRepairTimeout(domainName)
	repairCtx, cancel := context.WithTimeout(context.Background(), repairTimeout)
	defer cancel()

	// Propagate cancellation (but not timeout) from caller to repair context
	go func() {
		select {
		case <-callerCtx.Done():
			// Only propagate if caller was cancelled (not timed out)
			// We want repair to have its full timeout regardless of caller's timeout
			if callerCtx.Err() == context.Canceled {
				cancel()
			}
		case <-repairCtx.Done():
			// Repair finished or timed out
		}
	}()

	executionInfo := mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	startTime := time.Now()
	taggedScope := r.scope.Tagged(metrics.CorruptionTypeTag(corruptionType.String()))

	// Common log tags for both success and failure paths
	workflowTags := []tag.Tag{
		tag.WorkflowDomainID(domainID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
		tag.Dynamic("corruptionType", corruptionType.String()),
	}

	defer func() {
		taggedScope.RecordHistogramDuration(metrics.WorkflowRepairDuration, time.Since(startTime))

		// ErrWorkflowRepairedRetryOperation means repair succeeded
		isSuccess := errors.Is(retErr, ErrWorkflowRepairedRetryOperation)

		if !isSuccess {
			isTimeout := errors.Is(retErr, context.DeadlineExceeded) || errors.Is(retErr, context.Canceled)

			if isTimeout {
				taggedScope.IncCounter(metrics.WorkflowRepairTimeout)
			}

			taggedScope.IncCounter(metrics.WorkflowRepairFailure)

			r.logger.Error("Workflow repair failed", append(workflowTags, tag.Error(retErr))...)
		} else {
			taggedScope.IncCounter(metrics.WorkflowRepairSuccess)

			r.logger.Info("Workflow repair succeeded", workflowTags...)
		}
	}()

	clusterName := r.shard.GetClusterMetadata().GetCurrentClusterName()
	taggedScope.Tagged(metrics.SourceClusterTag(clusterName)).
		IncCounter(metrics.WorkflowRepairAttempted)

	r.logger.Info("Attempting to repair corrupted workflow",
		tag.WorkflowDomainID(domainID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
		tag.ClusterName(clusterName),
		tag.Dynamic("corruptionType", corruptionType.String()),
	)

	if corruptionType == CorruptionTypeChecksumMismatch {
		// Checksum mismatch - try to rebuild from local history
		return r.repairViaRebuild(repairCtx, mutableState, persistedChecksum)
	}

	// Unknown corruption type - should not happen
	return &types.InternalServiceError{
		Message: fmt.Sprintf("unknown corruption type: %v", corruptionType),
	}
}

// repairViaRebuild attempts to repair by rebuilding mutable state from history
func (r *workflowRepairerImpl) repairViaRebuild(
	ctx context.Context,
	mutableState MutableState,
	persistedChecksum checksum.Checksum,
) error {
	executionInfo := mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID
	domainName := mutableState.GetDomainEntry().GetInfo().Name

	branchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	versionHistories := mutableState.GetVersionHistories()
	if versionHistories == nil {
		return ErrMissingVersionHistories
	}

	currentVersionHistory, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return err
	}

	lastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return err
	}

	// Use StateRebuilder to rebuild mutable state from history
	// For local repair, source and target workflow are the same (we're rebuilding the same workflow from its own history)
	workflowIdentifier := definition.NewWorkflowIdentifier(domainID, workflowID, runID)

	rebuiltMutableState, _, err := r.stateRebuilder.Rebuild(
		ctx,
		time.Now(),
		workflowIdentifier, // source workflow
		branchToken,
		lastItem.EventID,
		lastItem.Version,
		workflowIdentifier, // target workflow (same as source for local repair)
		func() ([]byte, error) { return branchToken, nil },
		"", // requestID - empty for corruption repair
	)
	if err != nil {
		return err
	}

	// Try preserving original sticky tasklist before generating checksum
	// Sticky tasklist is a performance hint (not correctness) and isn't stored in history,
	// so rebuilt state won't have it. Try preserving it to see if checksum matches.
	//
	// NOTE: We directly mutate rebuiltInfo here:
	// - StickyTaskList: included in checksum, must be set before comparison
	// - Other fields: not in checksum, but preserved for metadata continuity
	rebuiltInfo := rebuiltMutableState.GetExecutionInfo()
	rebuiltInfo.StickyTaskList = executionInfo.StickyTaskList
	rebuiltInfo.StickyScheduleToStartTimeout = executionInfo.StickyScheduleToStartTimeout
	rebuiltInfo.ClientLibraryVersion = executionInfo.ClientLibraryVersion
	rebuiltInfo.ClientFeatureVersion = executionInfo.ClientFeatureVersion
	rebuiltInfo.ClientImpl = executionInfo.ClientImpl

	rebuiltChecksum, err := generateMutableStateChecksum(rebuiltMutableState)
	if err != nil {
		return err
	}

	if checksumMatches(rebuiltChecksum, persistedChecksum) {
		r.scope.IncCounter(metrics.MutableStateRebuildChecksumMatch)
	} else {
		r.scope.IncCounter(metrics.MutableStateRebuildChecksumMismatch)

		// If strict validation enabled, fail repair on checksum mismatch
		if r.shard.GetConfig().RequireChecksumMatchAfterRebuildRepair(domainName) {
			return ErrChecksumMismatchAfterRebuild
		}

		// Checksum didn't match - can't trust original sticky state, clear sticky fields.
		// Client version fields are preserved as they are metadata not included in checksum.
		rebuiltInfo.StickyTaskList = ""
		rebuiltInfo.StickyScheduleToStartTimeout = 0
	}

	// CRITICAL: Set the update condition (nextEventIDInDB) for conditional write to succeed
	//
	// When we persist the repaired state, we use optimistic concurrency control:
	// - PreviousNextEventIDCondition = what we expect DB to currently have (for the WHERE clause)
	// - ExecutionInfo.NextEventID = the correct value we want to write (for the SET clause)
	//
	// rebuiltMutableState already has the correct NextEventID from StateRebuilder.
	// We just need to set the update condition to the original (possibly corrupted) value
	// that's currently in the database, so the conditional write passes.
	originalNextEventIDInDB := executionInfo.NextEventID
	rebuiltNextEventID := rebuiltMutableState.GetExecutionInfo().NextEventID

	if originalNextEventIDInDB != rebuiltNextEventID {
		r.logger.Warn("NextEventID corruption detected - original DB value differs from rebuilt value",
			tag.WorkflowDomainID(domainID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.WorkflowNextEventID(originalNextEventIDInDB),
			tag.Dynamic("rebuiltNextEventID", rebuiltNextEventID),
		)
	}

	rebuiltMutableState.SetUpdateCondition(originalNextEventIDInDB)

	// Persist the repaired state immediately to DB
	// We use rebuiltMutableState directly (not the original mutableState) because:
	// - rebuiltMutableState is fully populated by replaying history (all maps, indexes, etc.)
	// - We force retry via ErrWorkflowRepairedRetryOperation, so the original mutableState is discarded
	// - The retry will load fresh, repaired state from DB
	if err := r.persistRepairedState(ctx, rebuiltMutableState); err != nil {
		return err
	}

	// Return special error to signal caller that repair succeeded and they should retry.
	// This prevents the caller from proceeding with potentially stale operation context:
	// - The operation may have been initiated based on corrupted state
	// - Concurrent repairs could race if caller proceeds to update
	// - Forcing retry ensures caller loads fresh, repaired state from DB
	return ErrWorkflowRepairedRetryOperation
}

func (r *workflowRepairerImpl) persistRepairedState(
	ctx context.Context,
	mutableState MutableState,
) error {
	executionInfo := mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID

	domainName, err := r.shard.GetDomainCache().GetDomainName(domainID)
	if err != nil {
		return err
	}

	// Close the transaction to generate the workflow mutation
	// Use TransactionPolicyPassive since we're just persisting repaired state, not generating new events
	workflowMutation, workflowEventsSeq, err := mutableState.CloseTransactionAsMutation(
		time.Now(),
		TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}

	// CloseTransactionAsMutation doesn't populate ExecutionStats - set it manually from mutableState
	workflowMutation.ExecutionStats = &persistence.ExecutionStats{
		HistorySize: mutableState.GetHistorySize(),
	}

	// Should not have any new events when just persisting repaired state
	if len(workflowEventsSeq) != 0 {
		return &types.InternalServiceError{
			Message: "unexpected history events during corruption repair persistence",
		}
	}

	// Persist to database using UpdateWorkflowModeIgnoreCurrent
	// We just want to fix the corrupted row, not update current execution pointers
	// Use shard.UpdateWorkflowExecution() because it automatically sets RangeID and Encoding
	_, err = r.shard.UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		Mode:                   persistence.UpdateWorkflowModeIgnoreCurrent,
		UpdateWorkflowMutation: *workflowMutation,
		DomainName:             domainName,
	})
	return err
}

// verifyChecksumAndAnalyze verifies the checksum and analyzes any mismatch
// Returns (corruptionType, persistedChecksum, error). If no corruption, returns (CorruptionTypeNone, checksum, nil)
func (r *workflowRepairerImpl) verifyChecksumAndAnalyze(
	mutableState MutableState,
	persistedChecksum checksum.Checksum,
) (CorruptionType, checksum.Checksum, error) {
	err := verifyMutableStateChecksum(mutableState, persistedChecksum)
	if err == nil {
		return CorruptionTypeNone, persistedChecksum, nil
	}

	r.logChecksumMismatchDetected(mutableState, err)
	return CorruptionTypeChecksumMismatch, persistedChecksum, checksum.ErrMismatch
}

// logChecksumMismatchDetected logs and metrics checksum mismatch detection
func (r *workflowRepairerImpl) logChecksumMismatchDetected(
	mutableState MutableState,
	err error,
) {
	clusterName := r.shard.GetClusterMetadata().GetCurrentClusterName()
	executionInfo := mutableState.GetExecutionInfo()

	// Increment the legacy checksum-mismatch metric on WorkflowContextScope for backwards compatibility
	r.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.MutableStateChecksumMismatch)

	// Also increment the new corruption-detection metric with tags
	r.scope.Tagged(metrics.CorruptionTypeTag(CorruptionTypeChecksumMismatch.String())).
		Tagged(metrics.SourceClusterTag(clusterName)).
		IncCounter(metrics.MutableStateCorruptionDetected)

	// Build detailed diagnostic tags for logging
	logTags := []tag.Tag{
		tag.WorkflowDomainID(executionInfo.DomainID),
		tag.WorkflowID(executionInfo.WorkflowID),
		tag.WorkflowRunID(executionInfo.RunID),
		tag.WorkflowNextEventID(executionInfo.NextEventID),
		tag.WorkflowScheduleID(executionInfo.DecisionScheduleID),
		tag.WorkflowStartedID(executionInfo.DecisionStartedID),
		tag.ClusterName(clusterName),
		tag.Dynamic("corruptionType", CorruptionTypeChecksumMismatch.String()),
		tag.Error(err),
	}

	// Add pending item counts for additional diagnostics
	logTags = append(logTags,
		tag.Dynamic("timerIDs", maps.Keys(mutableState.GetPendingTimerInfos())),
		tag.Dynamic("activityIDs", maps.Keys(mutableState.GetPendingActivityInfos())),
		tag.Dynamic("childIDs", maps.Keys(mutableState.GetPendingChildExecutionInfos())),
		tag.Dynamic("signalIDs", maps.Keys(mutableState.GetPendingSignalExternalInfos())),
		tag.Dynamic("cancelIDs", maps.Keys(mutableState.GetPendingRequestCancelExternalInfos())),
	)

	r.logger.Warn("Mutable state corruption detected: checksum mismatch", logTags...)
}

// checksumMatches returns true if both checksums are non-empty and equal
func checksumMatches(a, b checksum.Checksum) bool {
	return len(a.Value) > 0 && len(b.Value) > 0 && bytes.Equal(a.Value, b.Value)
}
