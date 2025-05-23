package timeout

// Code generated by gowrap. DO NOT EDIT.
// template: ../../templates/timeout.tmpl
// gowrap: http://github.com/hexdigest/gowrap

import (
	"context"
	"time"

	"go.uber.org/yarpc"

	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/common/types"
)

var _ admin.Client = (*adminClient)(nil)

// adminClient implements the admin.Client interface instrumented with timeouts
type adminClient struct {
	client       admin.Client
	largeTimeout time.Duration
	timeout      time.Duration
}

// NewAdminClient creates a new adminClient instance
func NewAdminClient(
	client admin.Client,
	largeTimeout time.Duration,
	timeout time.Duration,
) admin.Client {
	return &adminClient{
		client:       client,
		largeTimeout: largeTimeout,
		timeout:      timeout,
	}
}

func (c *adminClient) AddSearchAttribute(ctx context.Context, ap1 *types.AddSearchAttributeRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.AddSearchAttribute(ctx, ap1, p1...)
}

func (c *adminClient) CloseShard(ctx context.Context, cp1 *types.CloseShardRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.CloseShard(ctx, cp1, p1...)
}

func (c *adminClient) CountDLQMessages(ctx context.Context, cp1 *types.CountDLQMessagesRequest, p1 ...yarpc.CallOption) (cp2 *types.CountDLQMessagesResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.CountDLQMessages(ctx, cp1, p1...)
}

func (c *adminClient) DeleteWorkflow(ctx context.Context, ap1 *types.AdminDeleteWorkflowRequest, p1 ...yarpc.CallOption) (ap2 *types.AdminDeleteWorkflowResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.DeleteWorkflow(ctx, ap1, p1...)
}

func (c *adminClient) DescribeCluster(ctx context.Context, p1 ...yarpc.CallOption) (dp1 *types.DescribeClusterResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.DescribeCluster(ctx, p1...)
}

func (c *adminClient) DescribeHistoryHost(ctx context.Context, dp1 *types.DescribeHistoryHostRequest, p1 ...yarpc.CallOption) (dp2 *types.DescribeHistoryHostResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.DescribeHistoryHost(ctx, dp1, p1...)
}

func (c *adminClient) DescribeQueue(ctx context.Context, dp1 *types.DescribeQueueRequest, p1 ...yarpc.CallOption) (dp2 *types.DescribeQueueResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.DescribeQueue(ctx, dp1, p1...)
}

func (c *adminClient) DescribeShardDistribution(ctx context.Context, dp1 *types.DescribeShardDistributionRequest, p1 ...yarpc.CallOption) (dp2 *types.DescribeShardDistributionResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.DescribeShardDistribution(ctx, dp1, p1...)
}

func (c *adminClient) DescribeWorkflowExecution(ctx context.Context, ap1 *types.AdminDescribeWorkflowExecutionRequest, p1 ...yarpc.CallOption) (ap2 *types.AdminDescribeWorkflowExecutionResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.DescribeWorkflowExecution(ctx, ap1, p1...)
}

func (c *adminClient) GetDLQReplicationMessages(ctx context.Context, gp1 *types.GetDLQReplicationMessagesRequest, p1 ...yarpc.CallOption) (gp2 *types.GetDLQReplicationMessagesResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.GetDLQReplicationMessages(ctx, gp1, p1...)
}

func (c *adminClient) GetDomainAsyncWorkflowConfiguraton(ctx context.Context, request *types.GetDomainAsyncWorkflowConfiguratonRequest, opts ...yarpc.CallOption) (gp1 *types.GetDomainAsyncWorkflowConfiguratonResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.GetDomainAsyncWorkflowConfiguraton(ctx, request, opts...)
}

func (c *adminClient) GetDomainIsolationGroups(ctx context.Context, request *types.GetDomainIsolationGroupsRequest, opts ...yarpc.CallOption) (gp1 *types.GetDomainIsolationGroupsResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.GetDomainIsolationGroups(ctx, request, opts...)
}

func (c *adminClient) GetDomainReplicationMessages(ctx context.Context, gp1 *types.GetDomainReplicationMessagesRequest, p1 ...yarpc.CallOption) (gp2 *types.GetDomainReplicationMessagesResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.GetDomainReplicationMessages(ctx, gp1, p1...)
}

func (c *adminClient) GetDynamicConfig(ctx context.Context, gp1 *types.GetDynamicConfigRequest, p1 ...yarpc.CallOption) (gp2 *types.GetDynamicConfigResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.GetDynamicConfig(ctx, gp1, p1...)
}

func (c *adminClient) GetGlobalIsolationGroups(ctx context.Context, request *types.GetGlobalIsolationGroupsRequest, opts ...yarpc.CallOption) (gp1 *types.GetGlobalIsolationGroupsResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.GetGlobalIsolationGroups(ctx, request, opts...)
}

func (c *adminClient) GetReplicationMessages(ctx context.Context, gp1 *types.GetReplicationMessagesRequest, p1 ...yarpc.CallOption) (gp2 *types.GetReplicationMessagesResponse, err error) {
	ctx, cancel := createContext(ctx, c.largeTimeout)
	defer cancel()
	return c.client.GetReplicationMessages(ctx, gp1, p1...)
}

func (c *adminClient) GetWorkflowExecutionRawHistoryV2(ctx context.Context, gp1 *types.GetWorkflowExecutionRawHistoryV2Request, p1 ...yarpc.CallOption) (gp2 *types.GetWorkflowExecutionRawHistoryV2Response, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.GetWorkflowExecutionRawHistoryV2(ctx, gp1, p1...)
}

func (c *adminClient) ListDynamicConfig(ctx context.Context, lp1 *types.ListDynamicConfigRequest, p1 ...yarpc.CallOption) (lp2 *types.ListDynamicConfigResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.ListDynamicConfig(ctx, lp1, p1...)
}

func (c *adminClient) MaintainCorruptWorkflow(ctx context.Context, ap1 *types.AdminMaintainWorkflowRequest, p1 ...yarpc.CallOption) (ap2 *types.AdminMaintainWorkflowResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.MaintainCorruptWorkflow(ctx, ap1, p1...)
}

func (c *adminClient) MergeDLQMessages(ctx context.Context, mp1 *types.MergeDLQMessagesRequest, p1 ...yarpc.CallOption) (mp2 *types.MergeDLQMessagesResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.MergeDLQMessages(ctx, mp1, p1...)
}

func (c *adminClient) PurgeDLQMessages(ctx context.Context, pp1 *types.PurgeDLQMessagesRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.PurgeDLQMessages(ctx, pp1, p1...)
}

func (c *adminClient) ReadDLQMessages(ctx context.Context, rp1 *types.ReadDLQMessagesRequest, p1 ...yarpc.CallOption) (rp2 *types.ReadDLQMessagesResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.ReadDLQMessages(ctx, rp1, p1...)
}

func (c *adminClient) ReapplyEvents(ctx context.Context, rp1 *types.ReapplyEventsRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.ReapplyEvents(ctx, rp1, p1...)
}

func (c *adminClient) RefreshWorkflowTasks(ctx context.Context, rp1 *types.RefreshWorkflowTasksRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.RefreshWorkflowTasks(ctx, rp1, p1...)
}

func (c *adminClient) RemoveTask(ctx context.Context, rp1 *types.RemoveTaskRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.RemoveTask(ctx, rp1, p1...)
}

func (c *adminClient) ResendReplicationTasks(ctx context.Context, rp1 *types.ResendReplicationTasksRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.ResendReplicationTasks(ctx, rp1, p1...)
}

func (c *adminClient) ResetQueue(ctx context.Context, rp1 *types.ResetQueueRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.ResetQueue(ctx, rp1, p1...)
}

func (c *adminClient) RestoreDynamicConfig(ctx context.Context, rp1 *types.RestoreDynamicConfigRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.RestoreDynamicConfig(ctx, rp1, p1...)
}

func (c *adminClient) UpdateDomainAsyncWorkflowConfiguraton(ctx context.Context, request *types.UpdateDomainAsyncWorkflowConfiguratonRequest, opts ...yarpc.CallOption) (up1 *types.UpdateDomainAsyncWorkflowConfiguratonResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateDomainAsyncWorkflowConfiguraton(ctx, request, opts...)
}

func (c *adminClient) UpdateDomainIsolationGroups(ctx context.Context, request *types.UpdateDomainIsolationGroupsRequest, opts ...yarpc.CallOption) (up1 *types.UpdateDomainIsolationGroupsResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateDomainIsolationGroups(ctx, request, opts...)
}

func (c *adminClient) UpdateDynamicConfig(ctx context.Context, up1 *types.UpdateDynamicConfigRequest, p1 ...yarpc.CallOption) (err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateDynamicConfig(ctx, up1, p1...)
}

func (c *adminClient) UpdateGlobalIsolationGroups(ctx context.Context, request *types.UpdateGlobalIsolationGroupsRequest, opts ...yarpc.CallOption) (up1 *types.UpdateGlobalIsolationGroupsResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateGlobalIsolationGroups(ctx, request, opts...)
}

func (c *adminClient) UpdateTaskListPartitionConfig(ctx context.Context, request *types.UpdateTaskListPartitionConfigRequest, opts ...yarpc.CallOption) (up1 *types.UpdateTaskListPartitionConfigResponse, err error) {
	ctx, cancel := createContext(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateTaskListPartitionConfig(ctx, request, opts...)
}
