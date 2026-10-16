package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/gogo/protobuf/proto"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"github.com/urfave/cli/v2"

	"github.com/uber/cadence/common/types"
	protomapper "github.com/uber/cadence/common/types/mapper/proto"
	"github.com/uber/cadence/tools/common/commoncli"
)

const (
	exportFormatProto = "proto"
	exportFormatJSON  = "json"

	// exportMaxRestarts limits the restarts from the first page when another cluster serves a page.
	exportMaxRestarts = 3
)

func ExportWorkflow(c *cli.Context) error {
	domain, err := getRequiredOption(c, FlagDomain)
	if err != nil {
		return commoncli.Problem("Required flag not found: ", err)
	}
	wid, err := getRequiredOption(c, FlagWorkflowID)
	if err != nil {
		return commoncli.Problem("Required flag not found: ", err)
	}
	rid := c.String(FlagRunID)
	outputFile, err := getRequiredOption(c, FlagOutputFilename)
	if err != nil {
		return commoncli.Problem("Required flag not found: ", err)
	}

	exportFormat := c.String(FlagExportFormat)
	if exportFormat == "" {
		exportFormat = exportFormatProto
	}
	if exportFormat != exportFormatProto && exportFormat != exportFormatJSON {
		return commoncli.Problem("Invalid export format, must be 'proto' or 'json': ", fmt.Errorf("%q", exportFormat))
	}

	frontendClient, err := getWorkflowClient(c)
	if err != nil {
		return commoncli.Problem("Failed to create workflow client: ", err)
	}

	pageSize := int32(c.Int(FlagPageSize))

	var lastEventID *int64
	if c.IsSet(FlagLastEventID) {
		v := c.Int64(FlagLastEventID)
		lastEventID = &v
	}

	var allBatches []*types.DataBlob
	var nextPageToken []byte
	var execution *types.WorkflowExecution
	var version int32
	exportRestarts := 0

	for {
		req := &types.ExportWorkflowExecutionRequest{
			Domain: domain,
			Execution: &types.WorkflowExecution{
				WorkflowID: wid,
				RunID:      rid,
			},
			PageSize:      pageSize,
			NextPageToken: nextPageToken,
		}
		if nextPageToken == nil {
			req.LastEventID = lastEventID
		}
		ctx, cancel, err := newContext(c)
		if err != nil {
			return commoncli.Problem("Failed to create context: ", err)
		}
		resp, err := frontendClient.ExportWorkflowExecution(ctx, req)
		cancel()
		if err != nil {
			// The token is local to the cluster that served the first page. When another cluster
			// serves the request, the export starts again from the first page.
			if nextPageToken != nil && isExportTokenFromOtherClusterError(err) && exportRestarts < exportMaxRestarts {
				exportRestarts++
				allBatches = nil
				nextPageToken = nil
				continue
			}
			return commoncli.Problem("Failed to export workflow execution: ", err)
		}

		allBatches = append(allBatches, resp.GetHistoryBatches()...)
		execution = resp.GetExecution()
		version = resp.GetVersion()

		if len(resp.GetNextPageToken()) == 0 {
			break
		}
		nextPageToken = resp.GetNextPageToken()
	}

	exportData := &types.WorkflowExportData{
		Version:        version,
		Execution:      execution,
		HistoryBatches: allBatches,
	}

	data, err := marshalExportData(exportData, exportFormat)
	if err != nil {
		return commoncli.Problem("Failed to serialize export data: ", err)
	}

	if err := os.WriteFile(outputFile, data, 0644); err != nil {
		return commoncli.Problem("Failed to write export file: ", err)
	}

	fmt.Fprintf(getDeps(c).Output(), "Exported Workflow Id: %s, run Id: %s to %s (%d history batches, format: %s)\n",
		execution.GetWorkflowID(), execution.GetRunID(), outputFile, len(allBatches), exportFormat)
	return nil
}

func marshalExportData(exportData *types.WorkflowExportData, format string) ([]byte, error) {
	if format == exportFormatJSON {
		return json.MarshalIndent(exportData, "", "  ")
	}
	return proto.Marshal(protomapper.FromWorkflowExportData(exportData))
}

// unmarshalExportData reads an export file in proto or JSON format.
func unmarshalExportData(data []byte) (*types.WorkflowExportData, error) {
	exportData, err := unmarshalProtoExportData(data)
	if err == nil {
		return exportData, nil
	}

	exportData, jsonErr := unmarshalJSONExportData(data)
	if jsonErr == nil {
		return exportData, nil
	}

	return nil, fmt.Errorf("failed to parse export file as proto (%v) or JSON (%v)", err, jsonErr)
}

func unmarshalProtoExportData(data []byte) (*types.WorkflowExportData, error) {
	protoData := &apiv1.WorkflowExportData{}
	if err := proto.Unmarshal(data, protoData); err != nil {
		return nil, err
	}
	exportData := protomapper.ToWorkflowExportData(protoData)
	if err := validateExportData(exportData); err != nil {
		return nil, err
	}
	return exportData, nil
}

func unmarshalJSONExportData(data []byte) (*types.WorkflowExportData, error) {
	var exportData types.WorkflowExportData
	if err := json.Unmarshal(data, &exportData); err != nil {
		return nil, err
	}
	if err := validateExportData(&exportData); err != nil {
		return nil, err
	}
	return &exportData, nil
}

func validateExportData(exportData *types.WorkflowExportData) error {
	if exportData.Execution == nil || len(exportData.HistoryBatches) == 0 {
		return errors.New("invalid export data: missing execution or history batches")
	}
	return nil
}

func isExportTokenFromOtherClusterError(err error) bool {
	var badRequestError *types.BadRequestError
	return errors.As(err, &badRequestError) && strings.Contains(badRequestError.Message, "NextPageToken is from a different cluster")
}
