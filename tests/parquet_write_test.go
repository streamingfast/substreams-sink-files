package tests

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/parquet-go/parquet-go"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pbtesting "github.com/streamingfast/substreams-sink-files/internal/pb/tests"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
)

func TestParquetWriter(t *testing.T) {
	type parquetWriterCase struct {
		name          string
		outputModules []proto.Message
		expectedRows  map[string][]any
	}

	cases := []parquetWriterCase{
		{
			name: "message is a single row, one block equals one row",
			outputModules: []proto.Message{
				testProtobufRow(0),
				testProtobufRow(1),
			},
			expectedRows: map[string][]any{
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
			expectedRows: map[string][]any{
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
			expectedRows: map[string][]any{
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
			expectedRows: map[string][]any{
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
			expectedRows: map[string][]any{
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
			expectedRows: map[string][]any{
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
			expectedRows: map[string][]any{
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
			expectedRows: map[string][]any{
				"rows": {
					testGoRow(0),
					testGoRow(1),
				},
			},
		},
		{
			name: "from parquet tables, row with uint256 specialized column type",
			outputModules: []proto.Message{
				&pbtesting.RowColumnTypeUint256{
					Amount: "999925881158281189828",
				},
			},
			expectedRows: map[string][]any{
				"row_column_type": {
					&GoRowColumnTypeUint256{Amount: uint256("999925881158281189828")},
				},
			},
		},
		{
			name: "from parquet tables, row with int256 specialized column type",
			outputModules: []proto.Message{
				&pbtesting.RowColumnTypeInt256{
					Positive: "999925881158281189828",
					Negative: "999925881158281189828",
				},
			},
			expectedRows: map[string][]any{
				"row_column_type": {
					&GoRowColumnTypeInt256{
						Positive: int256("999925881158281189828"),
						Negative: int256("999925881158281189828"),
					},
				},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			storeDest := t.TempDir()

			// Note that setting PARQUET_WRITER_TEST_DESTINATION stops performing the assertion
			// by skipping the test.
			userProvidedDestination := os.Getenv("PARQUET_WRITER_TEST_DESTINATION")
			if userProvidedDestination != "" {
				storeDest = userProvidedDestination
			}

			require.True(t, len(testCase.outputModules) > 0, "no output modules provided")
			descriptor := testCase.outputModules[0].ProtoReflect().Descriptor()
			for _, outputModule := range testCase.outputModules {
				require.Equal(t, descriptor, outputModule.ProtoReflect().Descriptor(), "all output modules must have the same descriptor")
			}

			ctx := context.Background()
			writer, err := writer.NewParquetWriter(descriptor)
			require.NoError(t, err)

			err = writer.StartBoundary(bstream.NewRangeExcludingEnd(0, 1000))
			require.NoError(t, err)

			for _, outputModule := range testCase.outputModules {
				message, err := anypb.New(outputModule)
				require.NoError(t, err)

				err = writer.EncodeMapModule(&pbsubstreamsrpc.MapModuleOutput{
					Name:      "test",
					MapOutput: message,
				})
				require.NoError(t, err)
			}

			uploadable, err := writer.CloseBoundary(ctx)
			require.NoError(t, err)

			store, err := dstore.NewStore("file://"+storeDest, "", "", true)
			require.NoError(t, err)

			_, err = uploadable.Upload(ctx, store)
			require.NoError(t, err)

			if os.Getenv("PARQUET_WRITER_TEST_DESTINATION") != "" {
				t.Skip("PARQUET_WRITER_TEST_DESTINATION set, skipping assertion only writing files")
				return
			}

			for tableName, expectedRows := range testCase.expectedRows {
				storeFilename := tableName + "/" + "0000000000-0000001000.parquet"

				exists, err := store.FileExists(ctx, storeFilename)
				require.NoError(t, err)
				require.True(t, exists, "filename %q does not exist, files available: %s", storeFilename, strings.Join(listFiles(t, store), ", "))

				chFileInput := filepath.Join(storeDest, tableName, "*.parquet")

				dbx, err := sqlx.Open("chdb", "")
				require.NoError(t, err)

				// We need to retrieve the value's underlying type to create a slice of concrete type (and not interface{}!)
				// So we find the first row concrete value and extract it's type which will be interface{} then on this
				// reflect.Type we call .Elem() to get the concrete value type of the array.
				require.True(t, len(expectedRows) > 0, "Tests resulting in no rows are not supported yet")
				rowsValue := reflect.ValueOf(expectedRows)
				rowConcreteType := rowsValue.Index(0).Elem().Type()
				rowsConcreteType := reflect.SliceOf(rowConcreteType)

				rowsPtr := reflect.New(rowsConcreteType)
				rowsPtr.Elem().Set(reflect.MakeSlice(rowsConcreteType, 0, 8))

				err = dbx.Select(rowsPtr.Interface(), fmt.Sprintf(`select * from file('%s', Parquet)`, chFileInput))
				require.NoError(t, err)

				actualRows := reflect.Indirect(rowsPtr)
				for i := 0; i < actualRows.Len(); i++ {
					if v, ok := actualRows.Index(i).Addr().Interface().(timestampFixable); ok {
						v.fixTimestamps()
					}
				}

				concreteExpectedRows := reflect.MakeSlice(rowsConcreteType, 0, len(expectedRows))
				for _, expectedRow := range expectedRows {
					concreteExpectedRows = reflect.Append(concreteExpectedRows, reflect.ValueOf(expectedRow))
				}

				assert.Equal(t, concreteExpectedRows.Interface(), actualRows.Interface())
			}
		})
	}
}

func TestParquetFixedByte(t *testing.T) {
	schema := parquet.NewSchema("testRow", parquet.Group{
		"fixedField": parquet.Leaf(parquet.FixedLenByteArrayType(32)),
	})

	file, err := os.Create("/tmp/file.parquet")
	require.NoError(t, err)
	defer file.Close()

	writer := parquet.NewGenericWriter[any](file, schema)
	n, err := writer.WriteRows([]parquet.Row{
		{parquet.FixedLenByteArrayValue(make([]byte, 32))},
	})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())

	type testRow struct {
		FixedField []byte `parquet:"fixedField"`
	}

	rows, err := parquet.ReadFile[testRow]("/tmp/file.parquet", schema)
	require.NoError(t, err)

	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(make([]byte, 32)), hex.EncodeToString(rows[0].FixedField[:]))
}

func listFiles(t *testing.T, store dstore.Store) []string {
	t.Helper()

	ctx := context.Background()
	files, err := store.ListFiles(ctx, "", -1)
	require.NoError(t, err)
	return files
}

func testProtobufRow(i int) *pbtesting.Row {
	return &pbtesting.Row{
		TypeString:    fmt.Sprintf("abc-%d", i),
		TypeInt32:     int32(i),
		TypeInt64:     int64(i),
		TypeUint32:    uint32(i),
		TypeUint64:    uint64(i),
		TypeSint32:    int32(i),
		TypeSint64:    int64(i),
		TypeFixed32:   uint32(i),
		TypeFixed64:   uint64(i),
		TypeSfixed32:  int32(i),
		TypeSfixed64:  int64(i),
		TypeFloat:     float32(i) + 0.1,
		TypeDouble:    float64(i) + 0.1,
		TypeBool:      i%2 == 0,
		TypeBytes:     []byte(fmt.Sprintf("bytes-%d", i)),
		TypeTimestamp: timestamppb.New(time.UnixMilli(1000000000 + int64(i))),
	}
}

func testProtobufRowT(i int) *pbtesting.RowT {
	row := testProtobufRow(i)
	rowT := &pbtesting.RowT{}

	fields := row.ProtoReflect().Descriptor().Fields()
	fieldsT := rowT.ProtoReflect().Descriptor().Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldT := fieldsT.Get(i)

		rowFieldValue := row.ProtoReflect().Get(field)
		rowT.ProtoReflect().Set(fieldT, rowFieldValue)
	}

	return rowT
}

func testGoRow(i int) GoRow {
	return GoRow{
		TypeString:    fmt.Sprintf("abc-%d", i),
		TypeInt32:     int32(i),
		TypeInt64:     int64(i),
		TypeUint32:    uint32(i),
		TypeUint64:    uint64(i),
		TypeSint32:    int32(i),
		TypeSint64:    int64(i),
		TypeFixed32:   uint32(i),
		TypeFixed64:   uint64(i),
		TypeSfixed32:  int32(i),
		TypeSfixed64:  int64(i),
		TypeFloat:     float32(i) + 0.1,
		TypeDouble:    float64(i) + 0.1,
		TypeBool:      i%2 == 0,
		TypeBytes:     []byte(fmt.Sprintf("bytes-%d", i)),
		TypeTimestamp: time.UnixMilli(1000000000 + int64(i)).UTC(),
	}
}

type timestampFixable interface {
	fixTimestamps()
}

type GoRow struct {
	TypeString    string    `parquet:"typeString" db:"typeString"`
	TypeInt32     int32     `parquet:"typeInt32" db:"typeInt32"`
	TypeInt64     int64     `parquet:"typeInt64" db:"typeInt64"`
	TypeUint32    uint32    `parquet:"typeUint32" db:"typeUint32"`
	TypeUint64    uint64    `parquet:"typeUint64" db:"typeUint64"`
	TypeSint32    int32     `parquet:"typeSint32" db:"typeSint32"`
	TypeSint64    int64     `parquet:"typeSint64" db:"typeSint64"`
	TypeFixed32   uint32    `parquet:"typeFixed32" db:"typeFixed32"`
	TypeFixed64   uint64    `parquet:"typeFixed64" db:"typeFixed64"`
	TypeSfixed32  int32     `parquet:"typeSfixed32" db:"typeSfixed32"`
	TypeSfixed64  int64     `parquet:"typeSfixed64" db:"typeSfixed64"`
	TypeFloat     float32   `parquet:"typeFloat" db:"typeFloat"`
	TypeDouble    float64   `parquet:"typeDouble" db:"typeDouble"`
	TypeBool      bool      `parquet:"typeBool" db:"typeBool"`
	TypeBytes     []byte    `parquet:"typeBytes" db:"typeBytes"`
	TypeTimestamp time.Time `parquet:"typeTimestamp" db:"typeTimestamp"`
}

func (r *GoRow) fixTimestamps() {
	r.TypeTimestamp = r.TypeTimestamp.UTC()
}

type GoRowColumnTypeInt256 struct {
	Positive *Int256 `parquet:"positive" db:"positive"`
	Negative *Int256 `parquet:"negative" db:"amount"`
}

type Int256 big.Int

func int256(input string) *Int256 {
	i := new(Int256)
	_, ok := (*big.Int)(i).SetString(input, 10)
	if !ok {
		panic(fmt.Errorf("failed to parse %q as a big.Int", input))
	}

	return i
}

func (b Int256) Value() (driver.Value, error) {
	return (*big.Int)(&b).Text(10), nil
}

func (b *Int256) Scan(value interface{}) error {
	switch v := value.(type) {
	case []byte:
		(*big.Int)(b).SetBytes(v)
		return nil

	case string:
		_, ok := (*big.Int)(b).SetString(v, 10)
		if !ok {
			return fmt.Errorf("failed to parse %q as a big.Int", v)
		}

		return nil

	default:
		return fmt.Errorf("unsupported type %T", value)
	}
}

type GoRowColumnTypeUint256 struct {
	Amount *Uint256 `parquet:"amount" db:"amount"`
}

type Uint256 big.Int

func uint256(input string) *Uint256 {
	i := new(Uint256)
	_, ok := (*big.Int)(i).SetString(input, 10)
	if !ok {
		panic(fmt.Errorf("failed to parse %q as a big.Int", input))
	}

	return i
}

func (b Uint256) Value() (driver.Value, error) {
	return (*big.Int)(&b).Text(10), nil
}

func (b *Uint256) Scan(value interface{}) error {
	switch v := value.(type) {
	case []byte:
		(*big.Int)(b).SetBytes(v)
		return nil

	case string:
		_, ok := (*big.Int)(b).SetString(v, 10)
		if !ok {
			return fmt.Errorf("failed to parse %q as a big.Int", v)
		}

		return nil

	default:
		return fmt.Errorf("unsupported type %T", value)
	}
}
