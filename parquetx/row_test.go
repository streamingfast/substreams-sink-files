package parquetx

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestWriteRepeated(t *testing.T) {
	type Row struct {
		Repeated []int32
	}

	buffer := parquet.NewRowBuffer[Row]()
	buffer.WriteRows([]parquet.Row{
		[]parquet.Value{parquet.Int32Value(0).Level(0, 1, 0), parquet.Int32Value(1).Level(1, 1, 0), parquet.Int32Value(2).Level(1, 1, 0)},
	})

	data := bytes.NewBuffer(nil)
	parquetWriter := parquet.NewWriter(data, &parquet.WriterConfig{
		// FIXME: Add version too?
		CreatedBy: "substreams-sink-files",
		Schema:    parquet.SchemaOf(Row{}),
	})

	_, err := parquetWriter.WriteRowGroup(buffer)
	require.NoError(t, err)

	require.NoError(t, parquetWriter.Close())

	reader := bytes.NewReader(data.Bytes())
	parquetReader := parquet.NewGenericReader[Row](reader)

	rows := make([]Row, 1)
	n, err := parquetReader.Read(rows)
	if !errors.Is(err, io.EOF) {
		require.NoError(t, err)
	}
	require.Equal(t, 1, n)

	require.Equal(t, rows, []Row{
		{Repeated: []int32{0, 1, 2}},
	})
}
