package tests

import (
	"testing"

	pbtesting "github.com/streamingfast/substreams-sink-files/v2/internal/pb/tests"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func testParquetWriteEnumCases(t *testing.T) {
	type GoEnum struct {
		Value string `parquet:"value" db:"value"`
	}

	runCases(t, []parquetWriterCase[GoEnum]{
		{
			name:        "protobuf table with enum field",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumEnum{
					Value: pbtesting.EnumValue_FIRST,
				},
			},
			expectedRows: map[string][]GoEnum{
				"rows": {
					GoEnum{
						Value: "FIRST",
					},
				},
			},
		},

		{
			name:        "protobuf table with enum field, enum defined inside message",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumEnumInside{
					Value: pbtesting.RowColumEnumInside_SECOND,
				},
			},
			expectedRows: map[string][]GoEnum{
				"rows": {
					GoEnum{
						Value: "SECOND",
					},
				},
			},
		},

		{
			name:        "protobuf table with enum field but value is outside of enum's range, enum defined inside message",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumEnumInside{
					Value: pbtesting.RowColumEnumInside_Value(5),
				},
			},
			expectedEncodeError: errorIsString(
				`extracting rows from message "sf.substreams.sink.files.testing.RowColumEnumInside": converting message row: root message: message: leaf to value: enum value 5 is not a valid enumeration value for field 'value', known enum values are [UNKNOWN (0), FIRST (1), SECOND (2)]`,
			),
		},

		{
			name:        "protobuf table with enum field but value received is unknown, enum defined inside message",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumEnumWithSkippedValue{
					Value: pbtesting.RowColumEnumWithSkippedValue_Value(1),
				},
			},
			expectedEncodeError: errorIsString(
				`extracting rows from message "sf.substreams.sink.files.testing.RowColumEnumWithSkippedValue": converting message row: root message: message: leaf to value: enum value 1 is not a valid enumeration value for field 'value', known enum values are [UNKNOWN (0), SECOND (2)]`,
			),
		},
	})
}

func errorIsString(expected string) require.ErrorAssertionFunc {
	return func(t require.TestingT, err error, msgAndArgs ...interface{}) {
		require.EqualError(t, err, expected, msgAndArgs...)
	}
}
