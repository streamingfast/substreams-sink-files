package tests

import (
	"testing"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	pbtesting "github.com/streamingfast/substreams-sink-files/internal/pb/tests"
	"google.golang.org/protobuf/proto"
)

func testParquetWriteNestedCases(t *testing.T) {
	type GoNested struct {
		Value string `parquet:"value" db:"value"`
	}

	type GoRowNestedMessage struct {
		Nested *GoNested `parquet:"nested" db:"nested"`
	}

	runCases(t, []parquetWriterCase[GoRowNestedMessage]{
		{
			name:        "protobuf table with nested message field, one level deep",
			onlyDrivers: []string{"parquet-go"},
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

	type GoRowRepeatedNestedMessage struct {
		Nested []*GoNested `parquet:"nested" db:"nested"`
	}

	runCases(t, []parquetWriterCase[GoRowRepeatedNestedMessage]{
		{
			name:        "protobuf table with repeating nested message field, one level deep, empty",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnRepeatedNestedMessage{
					Nested: []*pbtesting.Nested{},
				},
			},
			expectedRows: map[string][]GoRowRepeatedNestedMessage{
				"rows": {
					GoRowRepeatedNestedMessage{
						Nested: []*GoNested{},
					},
				},
			},
		},
		{
			name:        "protobuf table with repeating nested message field, one level deep, one element",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnRepeatedNestedMessage{
					Nested: []*pbtesting.Nested{{
						Value: "abc-0",
					}},
				},
			},
			expectedRows: map[string][]GoRowRepeatedNestedMessage{
				"rows": {
					GoRowRepeatedNestedMessage{
						Nested: []*GoNested{
							{
								Value: "abc-0",
							},
						},
					},
				},
			},
		},
	})
}
