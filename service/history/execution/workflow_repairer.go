//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination workflow_repairer_mock.go -self_package github.com/uber/cadence/service/history/execution

package execution

import (
	"context"
	"errors"
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
	// ErrHistoryCorruption indicates history corruption that cannot be repaired via rebuild
	ErrHistoryCorruption = errors.New("history corruption detected - cannot rebuild from local history")

	// ErrChecksumMismatchAfterRebuild indicates the rebuilt state has a different checksum than the original
	ErrChecksumMismatchAfterRebuild = errors.New("rebuilt mutable state checksum does not match original - StateRebuilder may be buggy or history tampered")
)

type (
	// WorkflowRepairer attempts to detect and repair corrupted workflow executions
	WorkflowRepairer interface {
		// DetectAndRepairIfNeeded verifies checksum and attempts repair if corruption detected
		DetectAndRepairIfNeeded(
			ctx context.Context,
			mutableState MutableState,
			persistedChecksum checksum.Checksum,
		) error

		// RepairWorkflow attempts to repair a corrupted workflow execution
		// If successful, the passed-in mutableState will be updated with repaired state
		RepairWorkflow(
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

// String returns the string representation of CorruptionType
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
) error {
	// Verify checksum
	corruptionType, checksumValue, err := r.verifyChecksumAndAnalyze(mutableState, persistedChecksum)

	// If no corruption detected, return
	if corruptionType == CorruptionTypeNone {
		return nil
	}

	// Corruption detected - attempt auto-repair if enabled
	if r.shard.GetConfig().EnableCorruptionAutoRepair() {
		repairErr := r.RepairWorkflow(mutableState, corruptionType, checksumValue)
		if repairErr == nil {
			// Repair succeeded - mutableState has been updated in place
			return nil
		}
		// Repair failed - return the repair error
		return repairErr
	}

	// Auto-repair disabled - return the error associated with this corruption type
	return err
}

// RepairWorkflow attempts to repair a corrupted workflow execution
//
//goland:noinspection DuplicatedCode
func (r *workflowRepairerImpl) RepairWorkflow(
	mutableState MutableState,
	corruptionType CorruptionType,
	persistedChecksum checksum.Checksum,
) (retErr error) {
	// Use dedicated timeout independent of caller's context to give repair sufficient time.
	repairTimeout := r.shard.GetConfig().CorruptionRepairTimeout()
	repairCtx, cancel := context.WithTimeout(context.Background(), repairTimeout)
	defer cancel()

	executionInfo := mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	startTime := time.Now()
	defer func() {
		r.scope.Tagged(metrics.CorruptionTypeTag(corruptionType.String())).
			RecordHistogramDuration(metrics.WorkflowRepairDuration, time.Since(startTime))

		if retErr != nil {
			r.logger.Error("Workflow repair failed",
				tag.Error(retErr),
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Dynamic("corruptionType", corruptionType.String()),
			)
		} else {
			r.logger.Info("Workflow repair succeeded",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Dynamic("corruptionType", corruptionType.String()),
			)
		}
	}()

	clusterName := r.shard.GetClusterMetadata().GetCurrentClusterName()
	r.scope.Tagged(metrics.CorruptionTypeTag(corruptionType.String())).
		Tagged(metrics.SourceClusterTag(clusterName)).
		IncCounter(metrics.WorkflowRepairAttempted)

	r.logger.Info("Attempting to repair corrupted workflow",
		tag.WorkflowDomainID(domainID),
		tag.WorkflowID(workflowID),
		tag.WorkflowRunID(runID),
		tag.ClusterName(clusterName),
		tag.Dynamic("corruptionType", corruptionType.String()),
	)

	// Determine repair strategy based on corruption type
	if corruptionType == CorruptionTypeChecksumMismatch {
		// Checksum mismatch - try to rebuild from local history
		return r.repairViaRebuild(repairCtx, mutableState, persistedChecksum)
	}

	// History corruption detected - would need cross-cluster recovery
	// For now, return failure (cross-cluster not implemented yet)
	r.scope.Tagged(metrics.CorruptionTypeTag(corruptionType.String())).
		IncCounter(metrics.WorkflowRepairFailure)

	return ErrHistoryCorruption
}

// repairViaRebuild attempts to repair by rebuilding mutable state from history
// and loading it into the passed-in mutableState
func (r *workflowRepairerImpl) repairViaRebuild(
	ctx context.Context,
	mutableState MutableState,
	persistedChecksum checksum.Checksum,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	// Get branch token
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
	workflowIdentifier := definition.NewWorkflowIdentifier(domainID, workflowID, runID)

	rebuiltMutableState, _, err := r.stateRebuilder.Rebuild(
		ctx,
		time.Now(),
		workflowIdentifier,
		branchToken,
		lastItem.EventID,
		lastItem.Version,
		workflowIdentifier,
		func() ([]byte, error) { return branchToken, nil },
		"", // requestID - empty for corruption repair
	)
	if err != nil {
		// Check if the error is due to context timeout
		isTimeout := errors.Is(ctx.Err(), context.DeadlineExceeded)

		if isTimeout {
			r.scope.Tagged(metrics.CorruptionTypeTag(CorruptionTypeChecksumMismatch.String())).
				IncCounter(metrics.WorkflowRepairTimeout)
		}

		r.scope.Tagged(metrics.CorruptionTypeTag(CorruptionTypeChecksumMismatch.String())).
			IncCounter(metrics.WorkflowRepairFailure)

		return err
	}

	// Generate checksum from rebuilt state
	rebuiltChecksum, err := generateMutableStateChecksum(rebuiltMutableState)
	if err != nil {
		return err
	}

	checksumMatch := len(rebuiltChecksum.Value) > 0 && len(persistedChecksum.Value) > 0 &&
		string(rebuiltChecksum.Value) == string(persistedChecksum.Value)

	if checksumMatch {
		r.scope.IncCounter(metrics.MutableStateRebuildChecksumMatch)
	} else {
		r.scope.IncCounter(metrics.MutableStateRebuildChecksumMismatch)

		// If strict validation enabled, fail repair on checksum mismatch
		if r.shard.GetConfig().RequireChecksumMatchAfterRebuildRepair() {
			return ErrChecksumMismatchAfterRebuild
		}
	}

	// Load the rebuilt state into the passed-in mutableState
	rebuiltPersistence := rebuiltMutableState.CopyToPersistence()

	// CRITICAL: Preserve values for conditional write to succeed
	//
	// When we persist the repaired state, we use optimistic concurrency control:
	// - PreviousNextEventIDCondition = what we expect DB to currently have (for the WHERE clause)
	// - ExecutionInfo.NextEventID = the correct value we want to write (for the SET clause)
	//
	// The condition is derived from mutableState.nextEventIDInDB, which gets set by Load() from
	// ExecutionInfo.NextEventID. To make the conditional write pass, we need:
	// - nextEventIDInDB = original DB value we loaded (possibly corrupted)
	// - ExecutionInfo.NextEventID = rebuilt correct value
	//
	// Strategy:
	// 1. Temporarily set ExecutionInfo.NextEventID to original value before Load()
	// 2. Load() will copy this to nextEventIDInDB (correct for condition check)
	// 3. Restore ExecutionInfo.NextEventID to rebuilt value (correct for DB write)
	originalNextEventIDInDB := executionInfo.NextEventID
	rebuiltNextEventID := rebuiltPersistence.ExecutionInfo.NextEventID

	// Temporarily use original value so Load() sets nextEventIDInDB correctly
	rebuiltPersistence.ExecutionInfo.NextEventID = originalNextEventIDInDB

	if err := mutableState.Load(ctx, rebuiltPersistence); err != nil {
		r.scope.Tagged(metrics.CorruptionTypeTag(CorruptionTypeChecksumMismatch.String())).
			IncCounter(metrics.WorkflowRepairFailure)
		return err
	}

	// Restore the rebuilt NextEventID - this is the correct value to write to DB
	mutableState.GetExecutionInfo().NextEventID = rebuiltNextEventID

	// Persist the repaired state immediately to DB
	// Without this, if the caller's context times out or fails before persisting,
	// we'll detect the same corruption on the next Load() without actually fixing the issue.
	if err := r.persistRepairedState(ctx, mutableState); err != nil {
		r.scope.Tagged(metrics.CorruptionTypeTag(CorruptionTypeChecksumMismatch.String())).
			IncCounter(metrics.WorkflowRepairFailure)
		return err
	}

	// Repair succeeded
	r.scope.IncCounter(metrics.MutableStateCorruptionAutoRecovered)
	r.scope.Tagged(metrics.CorruptionTypeTag(CorruptionTypeChecksumMismatch.String())).
		IncCounter(metrics.WorkflowRepairSuccess)

	return nil
}

// persistRepairedState persists the repaired mutable state to the database
func (r *workflowRepairerImpl) persistRepairedState(
	ctx context.Context,
	mutableState MutableState,
) error {
	executionInfo := mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID

	// Get domain name for persistence request
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

	// Should not have any new events when just persisting repaired state
	if len(workflowEventsSeq) != 0 {
		return &types.InternalServiceError{
			Message: "unexpected history events during corruption repair persistence",
		}
	}

	// Persist to database using UpdateWorkflowModeIgnoreCurrent
	// We just want to fix the corrupted row, not update current execution pointers
	_, err = r.shard.GetExecutionManager().UpdateWorkflowExecution(ctx, &persistence.UpdateWorkflowExecutionRequest{
		Mode:                   persistence.UpdateWorkflowModeIgnoreCurrent,
		UpdateWorkflowMutation: *workflowMutation,
		DomainName:             domainName,
	})
	if err != nil {
		return err
	}

	return nil
}

// verifyChecksumAndAnalyze verifies the checksum and analyzes any mismatch
// Returns (corruptionType, persistedChecksum, error). If no corruption, returns (CorruptionTypeNone, checksum, nil)
func (r *workflowRepairerImpl) verifyChecksumAndAnalyze(
	mutableState MutableState,
	persistedChecksum checksum.Checksum,
) (CorruptionType, checksum.Checksum, error) {

	err := verifyMutableStateChecksum(mutableState, persistedChecksum)
	if err == nil {
		// Checksum is valid - no corruption
		return CorruptionTypeNone, persistedChecksum, nil
	}

	clusterName := r.shard.GetClusterMetadata().GetCurrentClusterName()
	executionInfo := mutableState.GetExecutionInfo()

	r.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.MutableStateChecksumMismatch)
	r.scope.IncCounter(metrics.MutableStateCorruptionDetected)
	r.scope.Tagged(metrics.CorruptionTypeTag(CorruptionTypeChecksumMismatch.String())).
		Tagged(metrics.SourceClusterTag(clusterName)).
		IncCounter(metrics.MutableStateCorruptionDetected)

	r.logger.Warn("Mutable state corruption detected: checksum mismatch",
		tag.WorkflowDomainID(executionInfo.DomainID),
		tag.WorkflowID(executionInfo.WorkflowID),
		tag.WorkflowRunID(executionInfo.RunID),
		tag.ClusterName(clusterName),
		tag.Dynamic("corruptionType", CorruptionTypeChecksumMismatch.String()),
	)

	return CorruptionTypeChecksumMismatch, persistedChecksum, checksum.ErrMismatch
}
