package engineimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/constants"
	"github.com/uber/cadence/service/history/engine/testdata"
)

func TestGetMutableState(t *testing.T) {
	execution := types.WorkflowExecution{
		WorkflowID: constants.TestWorkflowID,
		RunID:      constants.TestRunID,
	}
	parentExecution := &types.WorkflowExecution{
		WorkflowID: "test-parent-workflow-id",
		RunID:      "test-parent-run-id",
	}

	tests := []struct {
		name                        string
		executionInfo               *persistence.WorkflowExecutionInfo
		expectedParentExecutionInfo *types.ParentExecutionInfo
		expectedFirstExecutionRunID string
	}{
		{
			name: "child after continue as new",
			executionInfo: &persistence.WorkflowExecutionInfo{
				DomainID:            constants.TestDomainID,
				WorkflowID:          execution.WorkflowID,
				RunID:               execution.RunID,
				FirstExecutionRunID: "first-execution-run-id",
				ParentDomainID:      constants.TestDomainID,
				ParentWorkflowID:    parentExecution.WorkflowID,
				ParentRunID:         parentExecution.RunID,
				InitiatedID:         5,
				NextEventID:         4,
			},
			// the domain name of the parent is not set
			expectedParentExecutionInfo: &types.ParentExecutionInfo{
				DomainUUID:  constants.TestDomainID,
				Execution:   parentExecution,
				InitiatedID: 5,
			},
			expectedFirstExecutionRunID: "first-execution-run-id",
		},
		{
			name: "no parent",
			executionInfo: &persistence.WorkflowExecutionInfo{
				DomainID:            constants.TestDomainID,
				WorkflowID:          execution.WorkflowID,
				RunID:               execution.RunID,
				FirstExecutionRunID: execution.RunID,
				NextEventID:         4,
			},
			expectedParentExecutionInfo: nil,
			expectedFirstExecutionRunID: execution.RunID,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eft := testdata.NewEngineForTest(t, NewEngineWithShardContext)
			eft.Engine.Start()
			defer eft.Engine.Stop()

			eft.ShardCtx.Resource.ExecutionMgr.
				On("GetWorkflowExecution", mock.Anything, mock.MatchedBy(func(req *persistence.GetWorkflowExecutionRequest) bool {
					return req.Execution == execution
				})).
				Return(&persistence.GetWorkflowExecutionResponse{
					State: &persistence.WorkflowMutableState{
						ExecutionInfo:  tc.executionInfo,
						ExecutionStats: &persistence.ExecutionStats{},
						VersionHistories: persistence.NewVersionHistories(&persistence.VersionHistory{
							BranchToken: []byte("token"),
							Items: []*persistence.VersionHistoryItem{
								{EventID: 3, Version: 1},
							},
						}),
					},
					MutableStateStats: &persistence.MutableStateStats{},
				}, nil).
				Once()
			eft.ShardCtx.Resource.ActiveClusterMgr.
				EXPECT().GetActiveClusterInfoByWorkflow(gomock.Any(), constants.TestDomainID, execution.WorkflowID, execution.RunID).
				Return(&types.ActiveClusterInfo{ActiveClusterName: "test-active-cluster"}, nil).
				AnyTimes()

			response, err := eft.Engine.GetMutableState(context.Background(), &types.GetMutableStateRequest{
				DomainUUID: constants.TestDomainID,
				Execution:  &execution,
			})

			require.NoError(t, err)
			assert.Equal(t, tc.expectedParentExecutionInfo, response.GetParentExecutionInfo())
			assert.Equal(t, tc.expectedFirstExecutionRunID, response.GetFirstExecutionRunID())
		})
	}
}
