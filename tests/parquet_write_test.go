package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
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
	testParquetWriter(t, &pbtesting.Literals{}, parquetWriterCase{
		Generator: func(t *testing.T, ctx context.Context, writer *writer.ParquetWriter) {
			t.Helper()

			for i := 0; i < 2; i++ {
				message, err := anypb.New(&pbtesting.Literals{
					Elements: []*pbtesting.Literal{
						testProtobufTestLiteral(i),
					},
				})
				require.NoError(t, err)

				err = writer.EncodeMapModule(&pbsubstreamsrpc.MapModuleOutput{
					Name:      "test",
					MapOutput: message,
				})
				require.NoError(t, err)
			}
		},
		ExpectedRows: []GoRow{
			testGoRow(0),
			testGoRow(1),
		},
	})
}

type parquetWriterCase struct {
	Generator    func(t *testing.T, ctx context.Context, writer *writer.ParquetWriter)
	ExpectedRows []GoRow
}

func testParquetWriter(t *testing.T, root proto.Message, testCase parquetWriterCase) {
	t.Helper()

	storeDest := t.TempDir()

	userProvidedDestination := os.Getenv("PARQUET_WRITER_TEST_DESTINATION")
	if userProvidedDestination != "" {
		storeDest = userProvidedDestination
	}

	descriptor := root.ProtoReflect().Descriptor()

	ctx := context.Background()
	writer, err := writer.NewParquetWriter(descriptor)
	require.NoError(t, err)

	err = writer.StartBoundary(bstream.NewRangeExcludingEnd(0, 1000))
	require.NoError(t, err)

	testCase.Generator(t, ctx, writer)

	uploadable, err := writer.CloseBoundary(ctx)
	require.NoError(t, err)

	store, err := dstore.NewStore("file://"+storeDest, "", "", true)
	require.NoError(t, err)

	filename, err := uploadable.Upload(ctx, store)
	require.NoError(t, err)

	exists, err := store.FileExists(ctx, filename)
	require.NoError(t, err)
	require.True(t, exists, "filename %q does not exist", filename)

	chFileInput := filepath.Join(storeDest, "*.parquet")

	dbx, err := sqlx.Open("chdb", "")
	require.NoError(t, err)

	rows := make([]GoRow, 0)
	err = dbx.Select(&rows, fmt.Sprintf(`select * from file('%s', Parquet)`, chFileInput))
	require.NoError(t, err)
	assert.Equal(t, testCase.ExpectedRows, fixRowsTimestamp(rows))
}

func fixRowsTimestamp(rows []GoRow) []GoRow {
	for i := range rows {
		rows[i].TypeTimestamp = rows[i].TypeTimestamp.UTC()
	}
	return rows
}

func testProtobufTestLiteral(i int) *pbtesting.Literal {
	return &pbtesting.Literal{
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
