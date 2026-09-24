package signalexternalworkflow

import (
	"fmt"

	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/simulation/replication/types"
)

func Workflow(ctx workflow.Context, input types.WorkflowInput) (types.WorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Sugar().Infof("signal-external-workflow started, signaling %d targets with prefix %s", input.TargetWorkflowCount, input.TargetWorkflowID)

	if input.Delay > 0 {
		logger.Sugar().Infof("sleeping %v before sending signals", input.Delay)
		workflow.Sleep(ctx, input.Delay)
	}

	var futures []workflow.Future
	for i := 0; i < input.TargetWorkflowCount; i++ {
		targetID := fmt.Sprintf("%s-%d", input.TargetWorkflowID, i)
		f := workflow.SignalExternalWorkflow(ctx, targetID, "", input.SignalName, input.SignalData)
		futures = append(futures, f)
	}

	for i, f := range futures {
		if err := f.Get(ctx, nil); err != nil {
			targetID := fmt.Sprintf("%s-%d", input.TargetWorkflowID, i)
			logger.Sugar().Errorf("failed to signal workflow %s: %v", targetID, err)
			return types.WorkflowOutput{}, err
		}
	}

	logger.Sugar().Infof("signal-external-workflow completed, all %d signals delivered", input.TargetWorkflowCount)
	return types.WorkflowOutput{Count: input.TargetWorkflowCount}, nil
}
