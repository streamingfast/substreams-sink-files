package tests

import (
	"testing"

	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pbtesting "github.com/streamingfast/substreams-sink-files/internal/pb/tests"
	"google.golang.org/protobuf/proto"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
)

func testParquetWriteFlatCases(t *testing.T) {
	runCases(t, []parquetWriterCase[GoRow]{
		{
			name: "message is a single row, one block equals one row",
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
		{
			name: "message with single repeated field",
			outputModules: []proto.Message{
				&pbtesting.SingleRepeated{
					Elements: []*pbtesting.Row{
						testProtobufRow(0),
					},
				},
				&pbtesting.SingleRepeated{
					Elements: []*pbtesting.Row{
						testProtobufRow(1),
					},
				},
			},
			expectedRows: map[string][]GoRow{
				"elements": {
					testGoRow(0),
					testGoRow(1),
				},
			},
		},
		{
			name: "message with multiple repeated fields",
			outputModules: []proto.Message{
				&pbtesting.MultipleRepeated{
					TableA: []*pbtesting.Row{
						testProtobufRow(1),
						testProtobufRow(2),
					},
					TableB: []*pbtesting.Row{
						testProtobufRow(3),
					},
					TableC: []*pbtesting.Row{
						testProtobufRow(4),
						testProtobufRow(5),
						testProtobufRow(6),
					},
				},
			},
			expectedRows: map[string][]GoRow{
				"table_a": {
					testGoRow(1),
					testGoRow(2),
				},
				"table_b": {
					testGoRow(3),
				},
				"table_c": {
					testGoRow(4),
					testGoRow(5),
					testGoRow(6),
				},
			},
		},
		{
			name: "from parquet tables, single non-repeated field root",
			outputModules: []proto.Message{
				&pbtesting.FromTablesFlat{
					Row: testProtobufRowT(0),
				},
				&pbtesting.FromTablesFlat{
					Row: testProtobufRowT(3),
				},
			},
			expectedRows: map[string][]GoRow{
				"rows": {
					testGoRow(0),
					testGoRow(3),
				},
			},
		},
		{
			name: "from parquet tables, single repeated field root",
			outputModules: []proto.Message{
				&pbtesting.FromTablesRepeated{
					Elements: []*pbtesting.RowT{
						testProtobufRowT(0),
						testProtobufRowT(1),
					},
				},
				&pbtesting.FromTablesRepeated{
					Elements: []*pbtesting.RowT{
						testProtobufRowT(4),
						testProtobufRowT(5),
						testProtobufRowT(6),
					},
				},
			},
			expectedRows: map[string][]GoRow{
				"rows": {
					testGoRow(0),
					testGoRow(1),
					testGoRow(4),
					testGoRow(5),
					testGoRow(6),
				},
			},
		},
		{
			name: "from parquet tables, nested flag field",
			outputModules: []proto.Message{
				&pbtesting.FromTablesNestedFlat{
					Nested: &pbtesting.NestedFlat{
						Row: testProtobufRowT(0),
					},
				},
			},
			expectedRows: map[string][]GoRow{
				"rows": {
					testGoRow(0),
				},
			},
		},
		{
			name: "from parquet tables, nested repeated field",
			outputModules: []proto.Message{
				&pbtesting.FromTablesNestedRepeated{
					Nested: &pbtesting.NestedRepeated{
						Elements: []*pbtesting.RowT{
							testProtobufRowT(0),
							testProtobufRowT(1),
						},
					},
				},
			},
			expectedRows: map[string][]GoRow{
				"rows": {
					testGoRow(0),
					testGoRow(1),
				},
			},
		},
		{
			name: "from parquet tables, repeated field and one repeated ignored",
			outputModules: []proto.Message{
				&pbtesting.FromTablesRepeatedWithIgnore{
					Ignored: []*pbtesting.IgnoredRow{
						{Id: "ignored-0"},
					},
					Elements: []*pbtesting.RowT{
						testProtobufRowT(0),
						testProtobufRowT(1),
					},
				},
			},
			expectedRows: map[string][]GoRow{
				"rows": {
					testGoRow(0),
					testGoRow(1),
				},
			},
		},
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

	runCases(t, []parquetWriterCase[GoRowColumnTypeUint256]{
		{
			skip: "parquet-go panics, ch-db is not able to map 'fixed_size_binary[32]' column erroring with 'not yet implemented populating from columns of type fixed_size_binary[32]'",
			name: "from parquet tables, row with uint256 specialized column type",
			outputModules: []proto.Message{
				&pbtesting.RowColumnTypeUint256{
					Amount: "999925881158281189828",
				},
				&pbtesting.RowColumnTypeUint256{
					Amount: "89038154470531593666498931021702688443885319554480928852458527515161026101248",
				},
			},
			expectedRows: map[string][]GoRowColumnTypeUint256{
				"row_column_type_uint_256": {
					GoRowColumnTypeUint256{Amount: uint256("999925881158281189828")},
					GoRowColumnTypeUint256{Amount: uint256("89038154470531593666498931021702688443885319554480928852458527515161026101248")},
				},
			},
		},
	})

	runCases(t, []parquetWriterCase[GoRowColumnTypeInt256]{
		{
			skip: "ch-db is not able to map 'fixed_size_binary[32]' column erroring with 'not yet implemented populating from columns of type fixed_size_binary[32]'",
			name: "from parquet tables, row with int256 specialized column type",
			outputModules: []proto.Message{
				&pbtesting.RowColumnTypeInt256{
					Positive: "999925881158281189828",
					Negative: "999925881158281189828",
				},
			},
			expectedRows: map[string][]GoRowColumnTypeInt256{
				"row_column_type_int_256": {
					GoRowColumnTypeInt256{
						Positive: int256("999925881158281189828"),
						Negative: int256("999925881158281189828"),
					},
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

	type GoRowColumnRepeatedString struct {
		Values []string `parquet:"values" db:"values"`
	}

	runCases(t, []parquetWriterCase[GoRowColumnRepeatedString]{
		{
			name: "protobuf table with column empty/nil repeated string",
			// Not sure why, but ch-db complains with 'Array does not start with '[' character: while converting 'abc-0' to Array(Nullable(String))'
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnRepeatedString{
					Values: nil,
				},
				&pbtesting.RowColumnRepeatedString{
					Values: []string{},
				},
			},
			expectedRows: map[string][]GoRowColumnRepeatedString{
				"rows": {
					GoRowColumnRepeatedString{
						// parquet-go seems to always return an empty array for `nil` in entry
						// it's possible https://github.com/parquet-go/parquet-go/pull/95 would
						// fix this.
						Values: []string{},
					},
					GoRowColumnRepeatedString{
						Values: []string{},
					},
				},
			},
		},
		{
			name: "protobuf table with column repeated string",
			// Not sure why, but ch-db complains with 'Array does not start with '[' character: while converting 'abc-0' to Array(Nullable(String))'
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnRepeatedString{
					Values: []string{"abc-0", "abc-1"},
				},
			},
			expectedRows: map[string][]GoRowColumnRepeatedString{
				"rows": {
					GoRowColumnRepeatedString{Values: []string{"abc-0", "abc-1"}},
				},
			},
		},
	})

	type GoRowColumnSandwichedRepeatedString struct {
		Prefix string   `parquet:"prefix" db:"prefix"`
		Values []string `parquet:"values" db:"values"`
		Suffix string   `parquet:"suffix" db:"suffix"`
	}

	runCases(t, []parquetWriterCase[GoRowColumnSandwichedRepeatedString]{
		{
			name: "protobuf table with sandwiched column empty/nil repeated string",
			// Not sure why, but ch-db complains with 'Array does not start with '[' character: while converting 'abc-0' to Array(Nullable(String))'
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnSandwichedRepeatedString{
					Prefix: "prefix-0",
					Values: nil,
					Suffix: "suffix-0",
				},
				&pbtesting.RowColumnSandwichedRepeatedString{
					Prefix: "prefix-1",
					Values: []string{},
					Suffix: "suffix-1",
				},
			},
			expectedRows: map[string][]GoRowColumnSandwichedRepeatedString{
				"rows": {
					GoRowColumnSandwichedRepeatedString{
						Prefix: "prefix-0",
						// parquet-go seems to always return an empty array for `nil` in entry
						// it's possible https://github.com/parquet-go/parquet-go/pull/95 would
						// fix this.
						Values: []string{},
						Suffix: "suffix-0",
					},
					GoRowColumnSandwichedRepeatedString{
						Prefix: "prefix-1",
						Values: []string{},
						Suffix: "suffix-1",
					},
				},
			},
		},
	})

	type GoRowColumnSandwichedOptional struct {
		Prefix string  `parquet:"prefix" db:"prefix"`
		Value  *string `parquet:"value,optional" db:"value,optional"`
		Suffix string  `parquet:"suffix" db:"suffix"`
	}

	runCases(t, []parquetWriterCase[GoRowColumnSandwichedOptional]{
		{
			name: "protobuf table with sandwiched optional column string",
			outputModules: []proto.Message{
				&pbtesting.RowColumnSandwichedOptional{
					Prefix: "prefix-0",
					Value:  nil,
					Suffix: "suffix-0",
				},
				&pbtesting.RowColumnSandwichedOptional{
					Prefix: "prefix-1",
					Value:  ptr("abc-1"),
					Suffix: "suffix-1",
				},
			},
			expectedRows: map[string][]GoRowColumnSandwichedOptional{
				"rows": {
					GoRowColumnSandwichedOptional{
						Prefix: "prefix-0",
						Value:  nil,
						Suffix: "suffix-0",
					},
					GoRowColumnSandwichedOptional{
						Prefix: "prefix-1",
						Value:  ptr("abc-1"),
						Suffix: "suffix-1",
					},
				},
			},
		},
	})
}
