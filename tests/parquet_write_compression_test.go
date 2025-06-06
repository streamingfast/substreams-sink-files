package tests

import (
	"testing"

	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pbtesting "github.com/streamingfast/substreams-sink-files/internal/pb/tests"
	"google.golang.org/protobuf/proto"
)

func testParquetWriteCompressionCases(t *testing.T) {
	runCases(t, []parquetWriterCase[GoRow]{
		{
			name:          "default compression working",
			writerOptions: []writer.ParquetWriterOption{writer.ParquetDefaultColumnCompression("snappy")},
			outputModules: []proto.Message{
				testProtobufRow(0),
				testProtobufRow(1),
			},
			expectedRows: map[string][]GoRow{
				"row": {
					testGoRow(0),
					testGoRow(1),
				},
			},
		},
	})

	type GoRowColumnCompressionZstd struct {
		Value string `parquet:"value" db:"value"`
	}

	runCases(t, []parquetWriterCase[GoRowColumnCompressionZstd]{
		{
			name: "parquet column compression zstd",
			outputModules: []proto.Message{
				&pbtesting.RowColumnCompressionZstd{
					Value: "abc-0",
				},
			},
			expectedRows: map[string][]GoRowColumnCompressionZstd{
				"row_column_compression_zstd": {
					GoRowColumnCompressionZstd{Value: "abc-0"},
				},
			},
		},
	})

	type GoRowNestedCompressionInvalid struct {
		Value *GoNested `parquet:"value" db:"value"`
	}

	runCases(t, []parquetWriterCase[GoRowNestedCompressionInvalid]{
		{
			name: "parquet compression invalid on non-leaf message",
			outputModules: []proto.Message{
				&pbtesting.RowNestedCompressionInvalid{
					Value: &pbtesting.Nested{
						Value: "value1",
					},
				},
			},
			expectedNewWriterError: errorIsString(
				`find tables: error while walking message descriptor sf.substreams.sink.files.testing.RowNestedCompressionInvalid: compression can only be applied to leaf nodes, but field sf.substreams.sink.files.testing.RowNestedCompressionInvalid.value is not a leaf`,
			),
		},
	})

	type GoRowNestedMessage struct {
		Nested *GoNested `parquet:"nested" db:"nested"`
	}

	runCases(t, []parquetWriterCase[GoRowNestedMessage]{
		{
			name:          "default compression works in presence of nested messages (becomes no-op of non-leaf nodes)",
			writerOptions: []writer.ParquetWriterOption{writer.ParquetDefaultColumnCompression("snappy")},
			outputModules: []proto.Message{
				&pbtesting.RowColumnNestedMessage{
					Nested: &pbtesting.Nested{
						Value: "abc-0",
					},
				},
			},
			expectedRows: map[string][]GoRowNestedMessage{
				"rows": {
					GoRowNestedMessage{
						Nested: &GoNested{
							Value: "abc-0",
						},
					},
				},
			},
		},
	})
}
