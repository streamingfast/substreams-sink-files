package tests

import (
	"context"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockWriter implements Writer interface for testing
type mockWriter struct {
	startBoundaryCalled bool
	closeBoundaryResult writer.Uploadeable
	writeData           []byte
	fileType            writer.FileType
}

func (m *mockWriter) StartBoundary(*bstream.Range) error {
	m.startBoundaryCalled = true
	return nil
}

func (m *mockWriter) CloseBoundary(ctx context.Context) (writer.Uploadeable, error) {
	return m.closeBoundaryResult, nil
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	m.writeData = append(m.writeData, p...)
	return len(p), nil
}

func (m *mockWriter) Type() writer.FileType {
	return m.fileType
}

// mockUploadeable implements Uploadeable interface for testing
// Mimic the structure of dataFile/localFile from types.go
type mockUploadeable struct {
	outputFilename string
	uploadCalled   bool
}

func (m *mockUploadeable) Upload(ctx context.Context, store dstore.Store) (string, error) {
	m.uploadCalled = true
	// Return the outputFilename directly to test path modification
	return m.outputFilename, nil
}

func TestDatePartitionedWriter_DisabledPassthrough(t *testing.T) {
	logger := zap.NewNop()
	mockUnderlying := &mockWriter{
		closeBoundaryResult: &mockUploadeable{outputFilename: "test.jsonl"},
		fileType:            writer.FileTypeJSONL,
	}

	dateWriter := writer.NewDatePartitioned(mockUnderlying, false, "2006-01-02", logger)

	// Test that methods pass through correctly when disabled
	blockRange := bstream.NewRangeExcludingEnd(0, 100)

	err := dateWriter.StartBoundary(blockRange)
	require.NoError(t, err)
	assert.True(t, mockUnderlying.startBoundaryCalled)

	n, err := dateWriter.Write([]byte("test data"))
	require.NoError(t, err)
	assert.Equal(t, 9, n)
	assert.Equal(t, []byte("test data"), mockUnderlying.writeData)

	assert.Equal(t, writer.FileTypeJSONL, dateWriter.Type())

	uploadeable, err := dateWriter.CloseBoundary(context.Background())
	require.NoError(t, err)

	// When disabled, should return original uploadeable unchanged
	result, err := uploadeable.Upload(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "test.jsonl", result)
}

func TestDatePartitionedWriter_EnabledWithDatePath(t *testing.T) {
	logger := zap.NewNop()

	tests := []struct {
		name         string
		originalPath string
		dateFormat   string
		blockTime    time.Time
		expectedPath string
	}{
		{
			name:         "simple filename with date",
			originalPath: "test.jsonl",
			dateFormat:   "2006-01-02",
			blockTime:    time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			expectedPath: "2024-01-15/test.jsonl",
		},
		{
			name:         "table path with date injection",
			originalPath: "transfers/0000000000-0000000100.jsonl",
			dateFormat:   "2006-01-02",
			blockTime:    time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			expectedPath: "transfers/2024-01-15/0000000000-0000000100.jsonl",
		},
		{
			name:         "monthly partitioning",
			originalPath: "events/0000000000-0000000100.parquet",
			dateFormat:   "2006-01",
			blockTime:    time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			expectedPath: "events/2024-01/0000000000-0000000100.parquet",
		},
		{
			name:         "nested date format",
			originalPath: "logs/0000000000-0000000100.jsonl",
			dateFormat:   "2006/01/02",
			blockTime:    time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			expectedPath: "logs/2024/01/15/0000000000-0000000100.jsonl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockUnderlying := &mockWriter{
				closeBoundaryResult: &mockUploadeable{outputFilename: tt.originalPath},
				fileType:            writer.FileTypeJSONL,
			}

			dateWriter := writer.NewDatePartitioned(mockUnderlying, true, tt.dateFormat, logger)

			// Set the timestamp
			dateWriter.SetCurrentTimestamp(tt.blockTime)

			// Create a boundary and close it
			blockRange := bstream.NewRangeExcludingEnd(0, 100)
			err := dateWriter.StartBoundary(blockRange)
			require.NoError(t, err)

			uploadeable, err := dateWriter.CloseBoundary(context.Background())
			require.NoError(t, err)

			// The uploadeable should have the modified path
			result, err := uploadeable.Upload(context.Background(), nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedPath, result)
		})
	}
}

func TestDatePartitionedWriter_NoTimestamp(t *testing.T) {
	logger := zap.NewNop()
	mockUnderlying := &mockWriter{
		closeBoundaryResult: &mockUploadeable{outputFilename: "test.jsonl"},
		fileType:            writer.FileTypeJSONL,
	}

	dateWriter := writer.NewDatePartitioned(mockUnderlying, true, "2006-01-02", logger)
	// Don't set timestamp - should fall back to original path

	blockRange := bstream.NewRangeExcludingEnd(0, 100)
	err := dateWriter.StartBoundary(blockRange)
	require.NoError(t, err)

	uploadeable, err := dateWriter.CloseBoundary(context.Background())
	require.NoError(t, err)

	result, err := uploadeable.Upload(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "test.jsonl", result) // Should be unchanged
}

func TestDatePartitionedWriter_GetUnderlying(t *testing.T) {
	logger := zap.NewNop()
	mockUnderlying := &mockWriter{fileType: writer.FileTypeParquet}

	dateWriter := writer.NewDatePartitioned(mockUnderlying, true, "2006-01-02", logger)

	underlying := dateWriter.GetUnderlying()
	assert.Equal(t, mockUnderlying, underlying)
}
