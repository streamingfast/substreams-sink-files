package tests

import (
	"testing"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	pbtesting "github.com/streamingfast/substreams-sink-files/internal/pb/tests"
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
					Value: pbtesting.RowColumEnum_SECOND,
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
			name:        "protobuf table with enum field but value is outside of enum's range",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumEnum{
					Value: pbtesting.RowColumEnum_Value(5),
				},
			},
			expectedError: errorIsString(
				`extracting rows from message "sf.substreams.sink.files.testing.RowColumEnum": converting message row: root message: leaf to value: enum value 5 is not a valid enumeration value for field 'value', known enum values are [UNKNOWN (0), FIRST (1), SECOND (2)]`,
			),
		},
	})
}

func errorIsString(expected string) require.ErrorAssertionFunc {
	return func(t require.TestingT, err error, msgAndArgs ...interface{}) {
		require.EqualError(t, err, expected, msgAndArgs...)
	}
}
