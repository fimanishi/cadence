package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/tools/cli/clitest"
)

// exportCall is one call that the scripted server expects, with the response it returns.
type exportCall struct {
	wantToken       []byte
	wantLastEventID *int64
	resp            *types.ExportWorkflowExecutionResponse
	err             error
}

func exportPage(data string, nextPageToken []byte) *types.ExportWorkflowExecutionResponse {
	return &types.ExportWorkflowExecutionResponse{
		Execution:      &types.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"},
		HistoryBatches: []*types.DataBlob{{EncodingType: types.EncodingTypeThriftRW.Ptr(), Data: []byte(data), Checksum: []byte{1, 2, 3, 4}}},
		NextPageToken:  nextPageToken,
		Version:        1,
	}
}

func TestExportWorkflow(t *testing.T) {
	otherClusterErr := &types.BadRequestError{Message: "NextPageToken is from a different cluster. Start the export again without a NextPageToken."}

	tests := map[string]struct {
		format    string
		args      []clitest.CliArgument
		calls     []exportCall
		wantData  []string
		wantError string
	}{
		"single page in JSON format": {
			format:   exportFormatJSON,
			calls:    []exportCall{{resp: exportPage("batch1", nil)}},
			wantData: []string{"batch1"},
		},
		"single page in proto format": {
			format:   exportFormatProto,
			calls:    []exportCall{{resp: exportPage("batch1", nil)}},
			wantData: []string{"batch1"},
		},
		"pages are joined in order": {
			format: exportFormatJSON,
			calls: []exportCall{
				{resp: exportPage("batch1", []byte("token"))},
				{wantToken: []byte("token"), resp: exportPage("batch2", nil)},
			},
			wantData: []string{"batch1", "batch2"},
		},
		"last event ID is sent on the first page only": {
			format: exportFormatJSON,
			args:   []clitest.CliArgument{clitest.IntArgument(FlagLastEventID, 7)},
			calls: []exportCall{
				{wantLastEventID: common.Int64Ptr(7), resp: exportPage("batch1", []byte("token"))},
				{wantToken: []byte("token"), resp: exportPage("batch2", nil)},
			},
			wantData: []string{"batch1", "batch2"},
		},
		"export starts again from the first page when another cluster serves a page": {
			format: exportFormatJSON,
			calls: []exportCall{
				{resp: exportPage("batch1", []byte("token"))},
				{wantToken: []byte("token"), err: otherClusterErr},
				{resp: exportPage("batch1-again", []byte("token"))},
				{wantToken: []byte("token"), resp: exportPage("batch2", nil)},
			},
			wantData: []string{"batch1-again", "batch2"},
		},
		"restarts are limited": {
			format: exportFormatJSON,
			calls: []exportCall{
				{resp: exportPage("batch1", []byte("token"))},
				{wantToken: []byte("token"), err: otherClusterErr},
				{resp: exportPage("batch1", []byte("token"))},
				{wantToken: []byte("token"), err: otherClusterErr},
				{resp: exportPage("batch1", []byte("token"))},
				{wantToken: []byte("token"), err: otherClusterErr},
				{resp: exportPage("batch1", []byte("token"))},
				{wantToken: []byte("token"), err: otherClusterErr},
			},
			wantError: "Failed to export workflow execution",
		},
		"error on the first page is not a restart": {
			format:    exportFormatJSON,
			calls:     []exportCall{{err: otherClusterErr}},
			wantError: "Failed to export workflow execution",
		},
		"server error is returned": {
			format:    exportFormatJSON,
			calls:     []exportCall{{err: &types.InternalServiceError{Message: "export failed"}}},
			wantError: "Failed to export workflow execution",
		},
		"invalid format is refused": {
			format:    "xml",
			wantError: "Invalid export format",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			serverFrontendClient := frontend.NewMockClient(mockCtrl)
			app := NewCliApp(&clientFactoryMock{
				serverFrontendClient: serverFrontendClient,
				serverAdminClient:    admin.NewMockClient(mockCtrl),
			})

			callIndex := 0
			serverFrontendClient.EXPECT().
				ExportWorkflowExecution(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ interface{}, req *types.ExportWorkflowExecutionRequest, _ ...interface{}) (*types.ExportWorkflowExecutionResponse, error) {
					require.Less(t, callIndex, len(tc.calls), "unexpected export call")
					call := tc.calls[callIndex]
					callIndex++
					assert.Equal(t, "test-domain", req.Domain)
					assert.Equal(t, &types.WorkflowExecution{WorkflowID: "wf-1"}, req.Execution)
					assert.Equal(t, int32(50), req.PageSize)
					assert.Equal(t, call.wantToken, req.NextPageToken)
					assert.Equal(t, call.wantLastEventID, req.LastEventID)
					return call.resp, call.err
				}).AnyTimes()

			outputFile := filepath.Join(t.TempDir(), "export")
			args := append([]clitest.CliArgument{
				clitest.StringArgument(FlagDomain, "test-domain"),
				clitest.StringArgument(FlagWorkflowID, "wf-1"),
				clitest.StringArgument(FlagOutputFilename, outputFile),
				clitest.StringArgument(FlagExportFormat, tc.format),
				clitest.IntArgument(FlagPageSize, 50),
			}, tc.args...)
			err := ExportWorkflow(clitest.NewCLIContext(t, app, args...))
			assert.Equal(t, len(tc.calls), callIndex)
			if tc.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantError)
				return
			}
			require.NoError(t, err)

			data, err := os.ReadFile(outputFile)
			require.NoError(t, err)
			if tc.format == exportFormatJSON {
				var jsonData types.WorkflowExportData
				require.NoError(t, json.Unmarshal(data, &jsonData), "a JSON export must be JSON")
			}
			exported, err := unmarshalExportData(data)
			require.NoError(t, err)
			assert.Equal(t, int32(1), exported.Version)
			assert.Equal(t, &types.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"}, exported.Execution)
			var gotData []string
			for _, blob := range exported.HistoryBatches {
				gotData = append(gotData, string(blob.Data))
				assert.Equal(t, []byte{1, 2, 3, 4}, blob.Checksum)
			}
			assert.Equal(t, tc.wantData, gotData)
		})
	}
}

func TestUnmarshalExportData(t *testing.T) {
	exportData := &types.WorkflowExportData{
		Version:   1,
		Execution: &types.WorkflowExecution{WorkflowID: "wf-1", RunID: "run-1"},
		HistoryBatches: []*types.DataBlob{
			{EncodingType: types.EncodingTypeThriftRW.Ptr(), Data: []byte("batch-data-1"), Checksum: []byte{1, 2, 3, 4}},
			{EncodingType: types.EncodingTypeThriftRW.Ptr(), Data: []byte("batch-data-2"), Checksum: []byte{5, 6, 7, 8}},
		},
	}
	marshal := func(format string, data *types.WorkflowExportData) func(t *testing.T) []byte {
		return func(t *testing.T) []byte {
			bytes, err := marshalExportData(data, format)
			require.NoError(t, err)
			return bytes
		}
	}

	tests := map[string]struct {
		data      func(t *testing.T) []byte
		want      *types.WorkflowExportData
		wantError bool
	}{
		"proto file": {
			data: marshal(exportFormatProto, exportData),
			want: exportData,
		},
		"JSON file": {
			data: marshal(exportFormatJSON, exportData),
			want: exportData,
		},
		"proto file without history batches is refused": {
			data:      marshal(exportFormatProto, &types.WorkflowExportData{Version: 1, Execution: exportData.Execution}),
			wantError: true,
		},
		"JSON file without an execution is refused": {
			data:      marshal(exportFormatJSON, &types.WorkflowExportData{Version: 1, HistoryBatches: exportData.HistoryBatches}),
			wantError: true,
		},
		"file that is not proto or JSON is refused": {
			data:      func(*testing.T) []byte { return []byte("this is not valid proto or json") },
			wantError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := unmarshalExportData(tc.data(t))
			if tc.wantError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
