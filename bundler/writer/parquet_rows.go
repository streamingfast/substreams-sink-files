package writer

import (
	"io"

	"github.com/parquet-go/parquet-go"
)

type inMemoryRows struct {
	rows   []parquet.Row
	index  int
	schema *parquet.Schema
}

func (r *inMemoryRows) Close() error {
	r.index = -1
	return nil
}

func (r *inMemoryRows) Schema() *parquet.Schema {
	return r.schema
}

func (r *inMemoryRows) SeekToRow(rowIndex int64) error {
	if rowIndex < 0 {
		return parquet.ErrSeekOutOfRange
	}

	if r.index < 0 {
		return io.ErrClosedPipe
	}

	maxRowIndex := int64(len(r.rows))
	if rowIndex > maxRowIndex {
		rowIndex = maxRowIndex
	}

	r.index = int(rowIndex)
	return nil
}

func (r *inMemoryRows) ReadRows(rows []parquet.Row) (n int, err error) {
	if r.index < 0 {
		return 0, io.EOF
	}

	if n = len(r.rows) - r.index; n > len(rows) {
		n = len(rows)
	}

	for i, row := range r.rows[r.index : r.index+n] {
		rows[i] = append(rows[i][:0], row...)
	}

	if r.index += n; r.index == len(r.rows) {
		err = io.EOF
	}

	return n, err
}

func (r *inMemoryRows) WriteRowsTo(w parquet.RowWriter) (int64, error) {
	n, err := w.WriteRows(r.rows[r.index:])
	r.index += n
	return int64(n), err
}
