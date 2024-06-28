// Copyright (c) 2020 Uber Technologies, Inc.
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
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
)

// TerminateWorkflow is a helper function to terminate workflow
func TerminateWorkflow(
	mutableState MutableState,
	eventBatchFirstEventID int64,
	terminateReason string,
	terminateDetails []byte,
	terminateIdentity string,
	logger log.Logger,
	metricsClient metrics.Client,
) error {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := FailDecision(
			mutableState,
			decision,
			types.DecisionTaskFailedCauseForceCloseDecision,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		terminateReason,
		terminateDetails,
		terminateIdentity,
	)

	domainName := mutableState.GetDomainEntry().GetInfo().Name

	logger.Info(
		"Workflow execution terminated.",
		tag.WorkflowDomainName(domainName),
		tag.WorkflowID(mutableState.GetExecutionInfo().WorkflowID),
		tag.WorkflowRunID(mutableState.GetExecutionInfo().RunID),
		tag.WorkflowTerminationReason(terminateReason),
	)

	scopeWithDomainTag := metricsClient.Scope(metrics.HistoryTerminateWorkflowExecutionScope).Tagged(metrics.DomainTag(domainName))
	scopeWithDomainTag.IncCounter(metrics.WorkflowTerminateCounterPerDomain)

	return err
}
