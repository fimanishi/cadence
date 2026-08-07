// Copyright (c) 2024 Uber Technologies, Inc.
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
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package domaindeprecation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/worker/scheduler"
)

func TestDisableArchivalActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := frontend.NewMockClient(ctrl)
	mockClientBean := client.NewMockBean(ctrl)
	mockClientBean.EXPECT().GetFrontendClient().Return(mockClient).AnyTimes()

	deprecator := &domainDeprecator{
		cfg: Config{
			AdminOperationToken: dynamicproperties.GetStringPropertyFn(""),
		},
		clientBean: mockClientBean,
		logger:     testlogger.New(t),
	}

	testDomain := "test-domain"
	securityToken := "token"
	disabled := types.ArchivalStatusDisabled
	enabled := types.ArchivalStatusEnabled

	tests := []struct {
		name          string
		setupMocks    func()
		expectedError error
	}{
		{
			name: "Success - Disable archival",
			setupMocks: func() {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						Configuration: &types.DomainConfiguration{
							VisibilityArchivalStatus: &enabled,
							HistoryArchivalStatus:    &enabled,
						},
					}, nil)
				mockClient.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).Return(
					&types.UpdateDomainResponse{
						Configuration: &types.DomainConfiguration{
							VisibilityArchivalStatus: &disabled,
							HistoryArchivalStatus:    &disabled,
						},
					}, nil)
			},
			expectedError: nil,
		},
		{
			name: "Success - Archival already disabled",
			setupMocks: func() {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						Configuration: &types.DomainConfiguration{
							VisibilityArchivalStatus: &disabled,
							HistoryArchivalStatus:    &disabled,
						},
					}, nil)
			},
			expectedError: nil,
		},
		{
			name: "Error - Describe domain fails",
			setupMocks: func() {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					nil, assert.AnError)
			},
			expectedError: assert.AnError,
		},
		{
			name: "Error - Update domain fails",
			setupMocks: func() {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						Configuration: &types.DomainConfiguration{
							VisibilityArchivalStatus: &enabled,
							HistoryArchivalStatus:    &enabled,
						},
					}, nil)
				mockClient.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).Return(
					nil, assert.AnError)
			},
			expectedError: assert.AnError,
		},
		{
			name: "Error - Domain does not exist",
			setupMocks: func() {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					nil, &types.EntityNotExistsError{},
				)
			},
			expectedError: assert.AnError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMocks()
			err := deprecator.DisableArchivalActivity(context.Background(), DomainDeprecationParams{
				DomainName:    testDomain,
				SecurityToken: securityToken,
			})
			if tt.expectedError != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeprecateDomainActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := frontend.NewMockClient(ctrl)
	mockClientBean := client.NewMockBean(ctrl)
	mockClientBean.EXPECT().GetFrontendClient().Return(mockClient).AnyTimes()

	deprecator := &domainDeprecator{
		cfg: Config{
			AdminOperationToken: dynamicproperties.GetStringPropertyFn(""),
		},
		clientBean: mockClientBean,
		logger:     testlogger.New(t),
	}

	testDomain := "test-domain"
	securityToken := "token"

	tests := []struct {
		name          string
		setupMocks    func()
		expectedError error
	}{
		{
			name: "Success",
			setupMocks: func() {
				mockClient.EXPECT().DeprecateDomain(gomock.Any(), gomock.Any()).Return(nil)
			},
			expectedError: nil,
		},
		{
			name: "Error",
			setupMocks: func() {
				mockClient.EXPECT().DeprecateDomain(gomock.Any(), gomock.Any()).Return(assert.AnError)
			},
			expectedError: assert.AnError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMocks()
			err := deprecator.DeprecateDomainActivity(context.Background(), DomainDeprecationParams{
				DomainName:    testDomain,
				SecurityToken: securityToken,
			})
			if tt.expectedError != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCheckOpenWorkflowsActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := frontend.NewMockClient(ctrl)
	mockClientBean := client.NewMockBean(ctrl)
	mockClientBean.EXPECT().GetFrontendClient().Return(mockClient).AnyTimes()

	deprecator := &domainDeprecator{
		cfg: Config{
			AdminOperationToken: dynamicproperties.GetStringPropertyFn(""),
		},
		clientBean: mockClientBean,
		logger:     testlogger.New(t),
	}

	testDomain := "test-domain"

	tests := []struct {
		name           string
		setupMocks     func()
		expectedResult bool
		expectedError  error
	}{
		{
			name: "Success - no open workflows",
			setupMocks: func() {
				mockClient.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&types.CountWorkflowExecutionsResponse{Count: 0}, nil)
			},
			expectedResult: false,
			expectedError:  nil,
		},
		{
			name: "Success - has open workflows",
			setupMocks: func() {
				mockClient.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&types.CountWorkflowExecutionsResponse{Count: 100}, nil)
			},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name: "Error",
			setupMocks: func() {
				mockClient.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&types.CountWorkflowExecutionsResponse{Count: 0}, assert.AnError)
			},
			expectedResult: false,
			expectedError:  assert.AnError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMocks()
			hasOpenWorkflows, err := deprecator.CheckOpenWorkflowsActivity(context.Background(), DomainDeprecationParams{
				DomainName: testDomain,
			})
			if tt.expectedError != nil {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tt.expectedResult, hasOpenWorkflows)
				assert.NoError(t, err)
			}
		})
	}
}

func TestCheckActivePollersActivity(t *testing.T) {
	testDomain := "test-domain"

	singleClusterReplicationConfig := &types.DomainReplicationConfiguration{
		Clusters: []*types.ClusterReplicationConfiguration{
			{ClusterName: "cluster1"},
		},
	}
	multiClusterReplicationConfig := &types.DomainReplicationConfiguration{
		Clusters: []*types.ClusterReplicationConfiguration{
			{ClusterName: "cluster1"},
			{ClusterName: "cluster2"},
		},
	}

	tests := []struct {
		name          string
		setupMocks    func(mockClient *frontend.MockClient, mockClientBean *client.MockBean)
		expectedError bool
	}{
		{
			name: "Success - no pollers in any cluster",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				remoteClient := frontend.NewMockClient(gomock.NewController(t))
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: multiClusterReplicationConfig,
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster1").Return(mockClient, nil)
				mockClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(
					&types.GetTaskListsByDomainResponse{
						DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{},
						ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{},
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster2").Return(remoteClient, nil)
				remoteClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(
					&types.GetTaskListsByDomainResponse{
						DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{},
						ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{},
					}, nil)
			},
			expectedError: false,
		},
		{
			name: "Success - scheduler task list pollers are excluded",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: singleClusterReplicationConfig,
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster1").Return(mockClient, nil)
				mockClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(
					&types.GetTaskListsByDomainResponse{
						DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{
							scheduler.TaskListName: {Pollers: []*types.PollerInfo{{}}},
						},
						ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{
							scheduler.TaskListName: {Pollers: []*types.PollerInfo{{}}},
						},
					}, nil)
			},
			expectedError: false,
		},
		{
			name: "Success - nil replication configuration",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: nil,
					}, nil)
			},
			expectedError: false,
		},
		{
			name: "Error - active decision pollers",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: singleClusterReplicationConfig,
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster1").Return(mockClient, nil)
				mockClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(
					&types.GetTaskListsByDomainResponse{
						DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{
							"tl1": {Pollers: []*types.PollerInfo{{}}},
						},
						ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{},
					}, nil)
			},
			expectedError: true,
		},
		{
			name: "Error - active activity pollers",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: singleClusterReplicationConfig,
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster1").Return(mockClient, nil)
				mockClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(
					&types.GetTaskListsByDomainResponse{
						DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{},
						ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{
							"tl1": {Pollers: []*types.PollerInfo{{}}},
						},
					}, nil)
			},
			expectedError: true,
		},
		{
			name: "Error - DescribeDomain fails",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			expectedError: true,
		},
		{
			name: "Error - domain does not exist",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(nil, &types.EntityNotExistsError{})
			},
			expectedError: true,
		},
		{
			name: "Error - GetTaskListsByDomain fails",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: singleClusterReplicationConfig,
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster1").Return(mockClient, nil)
				mockClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			expectedError: true,
		},
		{
			name: "Error - GetRemoteFrontendClient fails",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: singleClusterReplicationConfig,
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster1").Return(nil, assert.AnError)
			},
			expectedError: true,
		},
		{
			name: "Error - active pollers in remote cluster",
			setupMocks: func(mockClient *frontend.MockClient, mockClientBean *client.MockBean) {
				remoteClient := frontend.NewMockClient(gomock.NewController(t))
				mockClient.EXPECT().DescribeDomain(gomock.Any(), gomock.Any()).Return(
					&types.DescribeDomainResponse{
						ReplicationConfiguration: multiClusterReplicationConfig,
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster1").Return(mockClient, nil)
				mockClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(
					&types.GetTaskListsByDomainResponse{
						DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{},
						ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{},
					}, nil)
				mockClientBean.EXPECT().GetRemoteFrontendClient("cluster2").Return(remoteClient, nil)
				remoteClient.EXPECT().GetTaskListsByDomain(gomock.Any(), gomock.Any()).Return(
					&types.GetTaskListsByDomainResponse{
						DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{
							"tl1": {Pollers: []*types.PollerInfo{{}}},
						},
						ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{},
					}, nil)
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := frontend.NewMockClient(ctrl)
			mockClientBean := client.NewMockBean(ctrl)
			mockClientBean.EXPECT().GetFrontendClient().Return(mockClient).AnyTimes()

			deprecator := &domainDeprecator{
				cfg: Config{
					AdminOperationToken: dynamicproperties.GetStringPropertyFn(""),
				},
				clientBean: mockClientBean,
				logger:     testlogger.New(t),
			}

			tt.setupMocks(mockClient, mockClientBean)
			err := deprecator.CheckActivePollersActivity(context.Background(), DomainDeprecationParams{
				DomainName: testDomain,
			})
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
