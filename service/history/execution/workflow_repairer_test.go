// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/checksum"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
)

const (
	testDomainID    = "test-domain-id"
	testWorkflowID  = "test-workflow-id"
	testRunID       = "test-run-id"
	testDomainName  = "test-domain"
	testTimeout     = time.Second * 10
	testBranchToken = "branch-token"
)

var (
	matchingChecksum = checksum.Checksum{
		Version: 1,
		Flavor:  checksum.FlavorIEEECRC32OverThriftBinary,
		Value:   []byte{223, 81, 9, 232},
	}
	mismatchedChecksum = checksum.Checksum{
		Version: 1,
		Flavor:  checksum.FlavorIEEECRC32OverThriftBinary,
		Value:   []byte{0xff, 0xff, 0xff, 0xff},
	}
)

func setupChecksumVerificationMocks(ms *MockMutableState, domainID, workflowID, runID string, match bool) checksum.Checksum {
	ms.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}).AnyTimes()
	ms.EXPECT().GetVersionHistories().Return(nil).AnyTimes()
	ms.EXPECT().GetPendingTimerInfos().Return(map[string]*persistence.TimerInfo{}).AnyTimes()
	ms.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistence.ActivityInfo{}).AnyTimes()
	ms.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistence.ChildExecutionInfo{}).AnyTimes()
	ms.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistence.RequestCancelInfo{}).AnyTimes()
	ms.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistence.SignalInfo{}).AnyTimes()

	if match {
		return matchingChecksum
	}
	return mismatchedChecksum
}

func setupBasicExecutionInfo(ms *MockMutableState) {
	ms.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
	}).AnyTimes()
}

func setupVersionHistories(ms *MockMutableState) {
	ms.EXPECT().GetVersionHistories().Return(&persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			{
				BranchToken: []byte(testBranchToken),
				Items: []*persistence.VersionHistoryItem{
					{EventID: 9, Version: 1},
				},
			},
		},
	}).AnyTimes()
	ms.EXPECT().GetCurrentBranchToken().Return([]byte(testBranchToken), nil).Times(1)
}

func setupDomainCacheMocks(testShard *shard.TestContext) {
	testShard.Resource.DomainCache.EXPECT().GetDomainName(testDomainID).Return(testDomainName, nil).Times(1)
	testShard.Resource.DomainCache.EXPECT().GetDomainByID(testDomainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: testDomainID, Name: testDomainName},
		&persistence.DomainConfig{},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: "active",
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: "active"},
			},
		},
		1234,
	), nil).Times(1)
}

func setupRebuiltMutableState(ctrl *gomock.Controller, checksumMatch bool) *MockMutableState {
	mockRebuiltMS := NewMockMutableState(ctrl)
	setupChecksumVerificationMocks(mockRebuiltMS, testDomainID, testWorkflowID, testRunID, checksumMatch)

	return mockRebuiltMS
}

func setupSuccessfulRebuild(
	ctrl *gomock.Controller,
	mockMutableState *MockMutableState,
	mockStateRebuilder *MockStateRebuilder,
	testShard *shard.TestContext,
	checksumMatch bool,
) {
	mockRebuiltMS := setupRebuiltMutableState(ctrl, checksumMatch)

	mockStateRebuilder.EXPECT().Rebuild(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		[]byte(testBranchToken),
		int64(9),
		int64(1),
		gomock.Any(),
		gomock.Any(),
		"",
	).Return(mockRebuiltMS, int64(0), nil).Times(1)

	// New flow: use rebuiltMutableState directly without Load()
	mockRebuiltMS.EXPECT().SetUpdateCondition(int64(0)).Times(1)
	mockRebuiltMS.EXPECT().GetHistorySize().Return(int64(1024)).Times(1)
	mockRebuiltMS.EXPECT().CloseTransactionAsMutation(gomock.Any(), TransactionPolicyPassive).Return(
		&persistence.WorkflowMutation{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				DomainID:   testDomainID,
				WorkflowID: testWorkflowID,
				RunID:      testRunID,
			},
		},
		nil,
		nil,
	).Times(1)

	setupDomainCacheMocks(testShard)
	testShard.Resource.ExecutionMgr.On("UpdateWorkflowExecution", mock.Anything, mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{}, nil).Once()
}

func TestNewWorkflowRepairer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testShard := shard.NewTestContext(
		t,
		ctrl,
		&persistence.ShardInfo{
			ShardID: 0,
			RangeID: 1,
		},
		config.NewForTest(),
	)

	mockLogger := log.NewNoop()
	mockMetricsClient := metrics.NewClient(tally.NoopScope, metrics.History, metrics.HistogramMigration{})

	repairer := NewWorkflowRepairer(testShard, mockLogger, mockMetricsClient)

	require.NotNil(t, repairer)
	impl, ok := repairer.(*workflowRepairerImpl)
	require.True(t, ok)
	require.Equal(t, testShard, impl.shard)
	require.Equal(t, mockLogger, impl.logger)
	require.Equal(t, mockMetricsClient, impl.metricsClient)
	require.NotNil(t, impl.stateRebuilder)
	require.NotNil(t, impl.scope)
}

func TestCorruptionType_String(t *testing.T) {
	require.Equal(t, "None", CorruptionTypeNone.String())
	require.Equal(t, "ChecksumMismatch", CorruptionTypeChecksumMismatch.String())
	require.Equal(t, "Unknown", CorruptionType(999).String())
}

func TestWorkflowRepairer_RepairWorkflow(t *testing.T) {
	testError := errors.New("test-error")
	testPersistenceError := &persistence.ConditionFailedError{Msg: "conflict"}

	tests := []struct {
		name              string
		corruptionType    CorruptionType
		persistedChecksum checksum.Checksum
		setupFunc         func(*gomock.Controller, *shard.TestContext, *config.Config, *MockMutableState, *MockStateRebuilder)
		wantErr           bool
		wantErrIs         error
	}{
		{
			name:              "repair succeeds with checksum match",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(false)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)
				setupSuccessfulRebuild(ctrl, mockMutableState, mockStateRebuilder, testShard, true)
			},
			wantErr:   true,
			wantErrIs: ErrWorkflowRepairedRetryOperation,
		},
		{
			name:              "Checksum mismatch after rebuild with RequireChecksumMatchAfterRebuildRepair disabled - repair succeeds",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(false)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)
				setupSuccessfulRebuild(ctrl, mockMutableState, mockStateRebuilder, testShard, false)
			},
			wantErr:   true,
			wantErrIs: ErrWorkflowRepairedRetryOperation,
		},
		{
			name:              "rebuild fails",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)

				setupBasicExecutionInfo(mockMutableState)
				mockMutableState.EXPECT().GetVersionHistories().Return(nil).AnyTimes()
				mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{}, testError).Times(1)
			},
			wantErr:   true,
			wantErrIs: testError,
		},
		{
			name:              "versionHistories is nil",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)

				setupBasicExecutionInfo(mockMutableState)
				mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte(testBranchToken), nil).Times(1)
				mockMutableState.EXPECT().GetVersionHistories().Return(nil).Times(1)
			},
			wantErr:   true,
			wantErrIs: ErrMissingVersionHistories,
		},
		{
			name:              "GetCurrentVersionHistory fails",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)

				setupBasicExecutionInfo(mockMutableState)
				mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte(testBranchToken), nil).Times(1)
				mockMutableState.EXPECT().GetVersionHistories().Return(&persistence.VersionHistories{
					CurrentVersionHistoryIndex: 99,
					Histories:                  []*persistence.VersionHistory{},
				}).Times(1)
			},
			wantErr: true,
		},
		{
			name:              "GetLastItem fails",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)

				setupBasicExecutionInfo(mockMutableState)
				mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte(testBranchToken), nil).Times(1)
				mockMutableState.EXPECT().GetVersionHistories().Return(&persistence.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*persistence.VersionHistory{
						{
							BranchToken: []byte(testBranchToken),
							Items:       []*persistence.VersionHistoryItem{},
						},
					},
				}).Times(1)
			},
			wantErr: true,
		},
		{
			name:              "Rebuild timeout",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)

				mockStateRebuilder.EXPECT().Rebuild(
					gomock.Any(), gomock.Any(), gomock.Any(),
					[]byte(testBranchToken), int64(9), int64(1),
					gomock.Any(), gomock.Any(), "",
				).Return(nil, int64(0), context.DeadlineExceeded).Times(1)
			},
			wantErr:   true,
			wantErrIs: context.DeadlineExceeded,
		},
		{
			name:              "Checksum mismatch after rebuild with RequireChecksumMatchAfterRebuildRepair enabled",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(true)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)

				mockRebuiltMS := NewMockMutableState(ctrl)
				mockRebuiltMS.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
					DomainID:    testDomainID,
					WorkflowID:  testWorkflowID,
					RunID:       testRunID,
					NextEventID: 999,
				}).AnyTimes()
				mockRebuiltMS.EXPECT().GetVersionHistories().Return(nil).AnyTimes()
				mockRebuiltMS.EXPECT().GetPendingTimerInfos().Return(map[string]*persistence.TimerInfo{}).AnyTimes()
				mockRebuiltMS.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistence.ActivityInfo{}).AnyTimes()
				mockRebuiltMS.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistence.ChildExecutionInfo{}).AnyTimes()
				mockRebuiltMS.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistence.RequestCancelInfo{}).AnyTimes()
				mockRebuiltMS.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistence.SignalInfo{}).AnyTimes()

				mockStateRebuilder.EXPECT().Rebuild(
					gomock.Any(), gomock.Any(), gomock.Any(),
					[]byte(testBranchToken), int64(9), int64(1),
					gomock.Any(), gomock.Any(), "",
				).Return(mockRebuiltMS, int64(0), nil).Times(1)
			},
			wantErr:   true,
			wantErrIs: ErrChecksumMismatchAfterRebuild,
		},
		{
			name:              "GetDomainName fails",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(false)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)

				mockRebuiltMS := setupRebuiltMutableState(ctrl, true)
				mockStateRebuilder.EXPECT().Rebuild(
					gomock.Any(), gomock.Any(), gomock.Any(),
					[]byte(testBranchToken), int64(9), int64(1),
					gomock.Any(), gomock.Any(), "",
				).Return(mockRebuiltMS, int64(0), nil).Times(1)

				mockRebuiltMS.EXPECT().SetUpdateCondition(int64(0)).Times(1)
				testShard.Resource.DomainCache.EXPECT().GetDomainName(testDomainID).Return("", testError).Times(1)
			},
			wantErr:   true,
			wantErrIs: testError,
		},
		{
			name:              "CloseTransactionAsMutation fails",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(false)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)

				mockRebuiltMS := setupRebuiltMutableState(ctrl, true)
				mockStateRebuilder.EXPECT().Rebuild(
					gomock.Any(), gomock.Any(), gomock.Any(),
					[]byte(testBranchToken), int64(9), int64(1),
					gomock.Any(), gomock.Any(), "",
				).Return(mockRebuiltMS, int64(0), nil).Times(1)

				mockRebuiltMS.EXPECT().SetUpdateCondition(int64(0)).Times(1)
				mockRebuiltMS.EXPECT().CloseTransactionAsMutation(gomock.Any(), TransactionPolicyPassive).Return(nil, nil, testError).Times(1)

				testShard.Resource.DomainCache.EXPECT().GetDomainName(testDomainID).Return(testDomainName, nil).Times(1)
			},
			wantErr:   true,
			wantErrIs: testError,
		},
		{
			name:              "CloseTransactionAsMutation returns unexpected events",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(false)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)

				mockRebuiltMS := setupRebuiltMutableState(ctrl, true)
				mockStateRebuilder.EXPECT().Rebuild(
					gomock.Any(), gomock.Any(), gomock.Any(),
					[]byte(testBranchToken), int64(9), int64(1),
					gomock.Any(), gomock.Any(), "",
				).Return(mockRebuiltMS, int64(0), nil).Times(1)

				mockRebuiltMS.EXPECT().SetUpdateCondition(int64(0)).Times(1)
				mockRebuiltMS.EXPECT().GetHistorySize().Return(int64(1024)).Times(1)
				mockRebuiltMS.EXPECT().CloseTransactionAsMutation(gomock.Any(), TransactionPolicyPassive).Return(
					&persistence.WorkflowMutation{
						ExecutionInfo: &persistence.WorkflowExecutionInfo{
							DomainID:   testDomainID,
							WorkflowID: testWorkflowID,
							RunID:      testRunID,
						},
					},
					[]*persistence.WorkflowEvents{{DomainID: testDomainID}},
					nil,
				).Times(1)

				testShard.Resource.DomainCache.EXPECT().GetDomainName(testDomainID).Return(testDomainName, nil).Times(1)
			},
			wantErr: true,
		},
		{
			name:              "UpdateWorkflowExecution fails",
			corruptionType:    CorruptionTypeChecksumMismatch,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(false)

				setupBasicExecutionInfo(mockMutableState)
				setupVersionHistories(mockMutableState)

				mockRebuiltMS := setupRebuiltMutableState(ctrl, true)
				mockStateRebuilder.EXPECT().Rebuild(
					gomock.Any(), gomock.Any(), gomock.Any(),
					[]byte(testBranchToken), int64(9), int64(1),
					gomock.Any(), gomock.Any(), "",
				).Return(mockRebuiltMS, int64(0), nil).Times(1)

				mockRebuiltMS.EXPECT().SetUpdateCondition(int64(0)).Times(1)
				mockRebuiltMS.EXPECT().GetHistorySize().Return(int64(1024)).Times(1)
				mockRebuiltMS.EXPECT().CloseTransactionAsMutation(gomock.Any(), TransactionPolicyPassive).Return(
					&persistence.WorkflowMutation{
						ExecutionInfo: &persistence.WorkflowExecutionInfo{
							DomainID:   testDomainID,
							WorkflowID: testWorkflowID,
							RunID:      testRunID,
						},
					},
					nil,
					nil,
				).Times(1)

				testShard.Resource.DomainCache.EXPECT().GetDomainName(testDomainID).Return(testDomainName, nil).Times(1)
				testShard.Resource.DomainCache.EXPECT().GetDomainByID(testDomainID).Return(cache.NewGlobalDomainCacheEntryForTest(
					&persistence.DomainInfo{ID: testDomainID, Name: testDomainName},
					&persistence.DomainConfig{},
					&persistence.DomainReplicationConfig{
						ActiveClusterName: "active",
						Clusters: []*persistence.ClusterReplicationConfig{
							{ClusterName: "active"},
						},
					},
					1234,
				), nil).Times(1)
				testShard.Resource.ExecutionMgr.On("UpdateWorkflowExecution", mock.Anything, mock.Anything).Return(nil, testPersistenceError).Once()
			},
			wantErr:   true,
			wantErrIs: testPersistenceError,
		},
		{
			name:              "Unknown corruption type - returns InternalServiceError",
			corruptionType:    CorruptionTypeNone,
			persistedChecksum: matchingChecksum,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(time.Second * 10)

				mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
					DomainID:   testDomainID,
					WorkflowID: testWorkflowID,
					RunID:      testRunID,
				}).AnyTimes()
			},
			wantErr: true,
			// Don't check specific error, just verify error is returned
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMutableState := NewMockMutableState(ctrl)
			mockLogger := log.NewNoop()
			mockMetricsClient := metrics.NewClient(tally.NoopScope, metrics.History, metrics.HistogramMigration{})
			mockConfig := config.NewForTest()
			mockStateRebuilder := NewMockStateRebuilder(ctrl)

			testShard := shard.NewTestContext(
				t,
				ctrl,
				&persistence.ShardInfo{
					ShardID: 0,
					RangeID: 1,
				},
				mockConfig,
			)

			tt.setupFunc(ctrl, testShard, mockConfig, mockMutableState, mockStateRebuilder)

			repairer := &workflowRepairerImpl{
				shard:          testShard,
				stateRebuilder: mockStateRebuilder,
				logger:         mockLogger,
				metricsClient:  mockMetricsClient,
				scope:          mockMetricsClient.Scope(metrics.WorkflowCorruptionRepairScope),
			}

			err := repairer.RepairWorkflow(context.Background(), mockMutableState, tt.corruptionType, tt.persistedChecksum)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWorkflowRepairer_DetectAndRepairIfNeeded(t *testing.T) {
	testDomainID := "test-domain-id"
	testWorkflowID := "test-workflow-id"
	testRunID := "test-run-id"
	testError := errors.New("test-error")

	tests := []struct {
		name      string
		match     bool
		setupFunc func(*gomock.Controller, *shard.TestContext, *config.Config, *MockMutableState, *MockStateRebuilder)
		wantErr   bool
		wantErrIs error
	}{
		{
			name:  "No corruption - checksum matches",
			match: true,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockMutableState.EXPECT().GetVersionHistories().Return(nil).AnyTimes()
			},
			wantErr: false,
		},
		{
			name:  "Mutable state corruption - checksum mismatch - repair disabled",
			match: false,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.EnableCorruptionAutoRepair = dynamicproperties.GetBoolPropertyFn(false)
				mockMutableState.EXPECT().GetVersionHistories().Return(nil).AnyTimes()
			},
			wantErr:   true,
			wantErrIs: checksum.ErrMismatch,
		},
		{
			name:  "Mutable state corruption - checksum mismatch - repair enabled and error",
			match: false,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.EnableCorruptionAutoRepair = dynamicproperties.GetBoolPropertyFn(true)
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(time.Second * 10)

				mockMutableState.EXPECT().GetVersionHistories().Return(nil).AnyTimes()
				mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{}, testError).Times(1)
			},
			wantErr:   true,
			wantErrIs: testError,
		},
		{
			name:  "Mutable state corruption - checksum mismatch - repair enabled and success",
			match: false,
			setupFunc: func(ctrl *gomock.Controller, testShard *shard.TestContext, mockConfig *config.Config, mockMutableState *MockMutableState, mockStateRebuilder *MockStateRebuilder) {
				mockConfig.EnableCorruptionAutoRepair = dynamicproperties.GetBoolPropertyFn(true)
				mockConfig.CorruptionRepairTimeout = dynamicproperties.GetDurationPropertyFn(testTimeout)
				mockConfig.RequireChecksumMatchAfterRebuildRepair = dynamicproperties.GetBoolPropertyFn(false)

				mockMutableState.EXPECT().GetVersionHistories().Return(&persistence.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*persistence.VersionHistory{
						{
							BranchToken: []byte(testBranchToken),
							Items: []*persistence.VersionHistoryItem{
								{EventID: 9, Version: 1},
							},
						},
					},
				}).AnyTimes()
				mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte(testBranchToken), nil).Times(1)

				mockRebuiltMS := setupRebuiltMutableState(ctrl, true)
				mockStateRebuilder.EXPECT().Rebuild(
					gomock.Any(), gomock.Any(), gomock.Any(),
					[]byte(testBranchToken), int64(9), int64(1),
					gomock.Any(), gomock.Any(), "",
				).Return(mockRebuiltMS, int64(0), nil).Times(1)

				mockRebuiltMS.EXPECT().SetUpdateCondition(int64(0)).Times(1)
				mockRebuiltMS.EXPECT().GetHistorySize().Return(int64(1024)).Times(1)
				mockRebuiltMS.EXPECT().CloseTransactionAsMutation(gomock.Any(), TransactionPolicyPassive).Return(
					&persistence.WorkflowMutation{
						ExecutionInfo: &persistence.WorkflowExecutionInfo{
							DomainID:   testDomainID,
							WorkflowID: testWorkflowID,
							RunID:      testRunID,
						},
					},
					nil,
					nil,
				).Times(1)

				setupDomainCacheMocks(testShard)
				testShard.Resource.ExecutionMgr.On("UpdateWorkflowExecution", mock.Anything, mock.Anything).Return(&persistence.UpdateWorkflowExecutionResponse{}, nil).Once()
			},
			wantErr:   true,
			wantErrIs: ErrWorkflowRepairedRetryOperation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockMutableState := NewMockMutableState(ctrl)
			mockLogger := log.NewNoop()
			mockMetricsClient := metrics.NewClient(tally.NoopScope, metrics.History, metrics.HistogramMigration{})
			mockConfig := config.NewForTest()
			mockStateRebuilder := NewMockStateRebuilder(ctrl)

			testShard := shard.NewTestContext(
				t,
				ctrl,
				&persistence.ShardInfo{
					ShardID: 0,
					RangeID: 1,
				},
				mockConfig,
			)

			tt.setupFunc(ctrl, testShard, mockConfig, mockMutableState, mockStateRebuilder)

			checksumToUse := setupChecksumVerificationMocks(mockMutableState, testDomainID, testWorkflowID, testRunID, tt.match)

			repairer := &workflowRepairerImpl{
				shard:          testShard,
				stateRebuilder: mockStateRebuilder,
				logger:         mockLogger,
				metricsClient:  mockMetricsClient,
				scope:          mockMetricsClient.Scope(metrics.WorkflowCorruptionRepairScope),
			}

			err := repairer.DetectAndRepairIfNeeded(context.Background(), mockMutableState, checksumToUse)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
