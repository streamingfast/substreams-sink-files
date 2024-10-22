package writer

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/substreams-sink-files/protox"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var _ Writer = (*ParquetWriter)(nil)

// ParquetWriter implements our internal interface for writing Parquet data to files
// directly.
//
// FIXME: There is definitely a lot of knowledge to share between [ParquetWriter] here
// and [encoder.ProtoToParquet] struct. Indeed, both needs to determine some kind of information
// about the message structure to be able to work properly. The write for example needs to find the
// correct Message to derive a "table" schema from. The [encoder.ProtoToParquet] needs similar information
// but this time to extract "rows" to pass to the [ParquetWriter] here.
//
// In fact more I think about, Writer and Encoder should co-exists to decouple I/O to actual line format
// used (CSV, JSONL, TSV, etc). But in the case of Parquet, there is no sense to actually have the relationship
// as the writer and the encoder a coupled and can go only hand-in-hand (e.g. there will never be a [ParquetWriter]
// with a different encoder implementation).
type ParquetWriter struct {
	descriptor protoreflect.MessageDescriptor
	schema     *parquet.Schema

	activeRange *bstream.Range
	rowBuffer   *parquet.RowBuffer[any]
}

func NewParquetWriter(descriptor protoreflect.MessageDescriptor) (*ParquetWriter, error) {
	repeatedFieldCount := protox.MessageRepeatedFieldCount(descriptor)
	if repeatedFieldCount == 0 {
		return nil, fmt.Errorf("invalid parquet Protobuf model %s: expected to contain exactly one repeated type, found 0",
			descriptor.FullName(),
		)
	}

	if repeatedFieldCount > 1 {
		return nil, fmt.Errorf("invalid parquet Protobuf model %s: expected to contain exactly one repeated type, found %d (fields %s)",
			descriptor.FullName(),
			repeatedFieldCount,
			strings.Join(protox.MessageRepeatedFieldNames(descriptor), ", "),
		)
	}

	repeatedField := protox.FindMessageFirstRepeatedField(descriptor)
	if repeatedField.Message() == nil {
		// It means we are dealing with a `repeated <primitive>` type like list of string which we don't support
		return nil, fmt.Errorf("invalid parquet Protobuf model %s: repeated field %s seems to be a repeated of primitive type, which we don't support",
			descriptor.FullName(),
			repeatedField.Name(),
		)
	}

	return &ParquetWriter{
		descriptor: descriptor,
		schema:     protox.ParquetSchemaFromMessageDescriptor(repeatedField.Message()),
	}, nil
}

// CloseBoundary implements Writer.
func (p *ParquetWriter) CloseBoundary(ctx context.Context) (Uploadeable, error) {
	defer func() {
		p.activeRange = nil
		p.rowBuffer = nil
	}()

	if p.activeRange == nil {
		return nil, fmt.Errorf("no active range, unable to close boundary")
	}

	// We must keep required data since before upload is actually called, we reset boundary data
	filename := fmt.Sprintf("%010d-%010d.parquet", p.activeRange.StartBlock(), *p.activeRange.EndBlock())
	rowBuffer := p.rowBuffer

	return UploadeableFunc(func(ctx context.Context, store dstore.Store) (string, error) {
		reader, writer := io.Pipe()

		go func() {
			defer writer.Close()

			parquetWriter := parquet.NewWriter(writer, &parquet.WriterConfig{
				// FIXME: Add version too?
				CreatedBy: "substreams-sink-files",
				Schema:    p.schema,
			})

			_, err := parquetWriter.WriteRowGroup(rowBuffer)
			if err != nil {
				writer.CloseWithError(fmt.Errorf("write row group parquet writer: %w", err))
				return
			}

			if err := parquetWriter.Close(); err != nil {
				writer.CloseWithError(fmt.Errorf("close parquet writer: %w", err))
				return
			}
		}()

		if err := store.WriteObject(ctx, filename, reader); err != nil {
			return "", fmt.Errorf("write parquet file: %w", err)
		}

		return filename, nil
	}), nil
}

// StartBoundary implements Writer.
func (p *ParquetWriter) StartBoundary(blockRange *bstream.Range) error {
	if p.activeRange != nil {
		return fmt.Errorf("a range is already in progress")
	}

	if blockRange == nil || blockRange.EndBlock() == nil {
		return fmt.Errorf("invalid block range, must be set and closed")
	}

	p.activeRange = blockRange
	p.rowBuffer = parquet.NewRowBuffer[any](&parquet.RowGroupConfig{
		Schema: p.schema,
	})
	return nil
}

// Type implements Writer.
func (p *ParquetWriter) Type() FileType {
	return FileTypeParquet
}

// Write implements Writer.
func (*ParquetWriter) Write(p []byte) (n int, err error) {
	// FIXME This alone is probably a proof that the Writer interface we have
	// is too much tied with writing low level bytes. As with Parquet, we have
	// an intermediate step.
	panic("shouldn't be called in ParquetWriter")
}
