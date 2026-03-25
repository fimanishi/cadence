//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination workflow_repairer_mock.go -self_package github.com/uber/cadence/service/history/execution

package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	corruptionType, checksumValue, err := r.verifyChecksumAndAnalyze(mutableState, persistedChecksum)

	if corruptionType == CorruptionTypeNone {
		return nil
	}

	// Corruption detected - attempt auto-repair if enabled
	if r.shard.GetConfig().EnableCorruptionAutoRepair() {
		return r.RepairWorkflow(mutableState, corruptionType, checksumValue)
	}

	// Auto-repair disabled - return the error associated with this corruption type
	return err
}

// RepairWorkflow attempts to repair a corrupted workflow execution
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
	taggedScope := r.scope.Tagged(metrics.CorruptionTypeTag(corruptionType.String()))
	defer func() {
		taggedScope.RecordHistogramDuration(metrics.WorkflowRepairDuration, time.Since(startTime))

		if retErr != nil {
			isTimeout := errors.Is(retErr, context.DeadlineExceeded) || errors.Is(retErr, context.Canceled)

			if isTimeout {
				taggedScope.IncCounter(metrics.WorkflowRepairTimeout)
			}

			taggedScope.IncCounter(metrics.WorkflowRepairFailure)

			r.logger.Error("Workflow repair failed",
				tag.Error(retErr),
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Dynamic("corruptionType", corruptionType.String()),
			)
		} else {
			taggedScope.IncCounter(metrics.WorkflowRepairSuccess)

			r.logger.Info("Workflow repair succeeded",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Dynamic("corruptionType", corruptionType.String()),
			)
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
	rebuiltInfo := rebuiltMutableState.GetExecutionInfo()
	rebuiltInfo.StickyTaskList = executionInfo.StickyTaskList

	rebuiltChecksum, err := generateMutableStateChecksum(rebuiltMutableState)
	if err != nil {
		return err
	}

	if checksumMatches(rebuiltChecksum, persistedChecksum) {
		r.scope.IncCounter(metrics.MutableStateRebuildChecksumMatch)
	} else {
		r.scope.IncCounter(metrics.MutableStateRebuildChecksumMismatch)

		// If strict validation enabled, fail repair on checksum mismatch
		if r.shard.GetConfig().RequireChecksumMatchAfterRebuildRepair() {
			return ErrChecksumMismatchAfterRebuild
		}

		// Checksum didn't match - can't trust original sticky tasklist, clear it
		rebuiltInfo.StickyTaskList = ""
	}

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

	// CRITICAL: Clear checksum to prevent recursive repair during Load()
	//
	// Load() verifies the checksum (line 366-372 in mutable_state_builder.go) and would call
	// DetectAndRepairIfNeeded() again, creating infinite recursion. By clearing the checksum,
	// we skip verification during Load(). A fresh checksum will be automatically generated
	// when we call CloseTransactionAsMutation() in persistRepairedState().
	rebuiltPersistence.Checksum = checksum.Checksum{}

	if err := mutableState.Load(ctx, rebuiltPersistence); err != nil {
		return err
	}

	// Restore the rebuilt NextEventID - this is the correct value to write to DB
	mutableState.GetExecutionInfo().NextEventID = rebuiltNextEventID

	// Persist the repaired state immediately to DB
	// Without this, if the caller's context times out or fails before persisting,
	// we'll detect the same corruption on the next Load() without actually fixing the issue.
	return r.persistRepairedState(ctx, mutableState)
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

	clusterName := r.shard.GetClusterMetadata().GetCurrentClusterName()
	executionInfo := mutableState.GetExecutionInfo()

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

// checksumMatches returns true if both checksums are non-empty and equal
func checksumMatches(a, b checksum.Checksum) bool {
	return len(a.Value) > 0 && len(b.Value) > 0 && bytes.Equal(a.Value, b.Value)
}
