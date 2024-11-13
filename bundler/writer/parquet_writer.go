package writer

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams-sink-files/parquetx"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

var _ Writer = (*ParquetWriter)(nil)

// ParquetWriter implements our internal interface for writing Parquet data to files
// directly.
type ParquetWriter struct {
	options      *ParquetWriterOptions
	descriptor   protoreflect.MessageDescriptor
	tables       []parquetx.TableResult
	tablesByName map[string]parquetx.TableResult
	rowExtractor parquetx.ProtoRowExtractor

	activeRange           *bstream.Range
	rowsBufferByTableName map[string]*parquet.RowBuffer[any]
}

func NewParquetWriter(descriptor protoreflect.MessageDescriptor, logger *zap.Logger, tracer logging.Tracer, opts ...ParquetWriterOption) (*ParquetWriter, error) {
	options, err := NewParquetWriterOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("invalid parquet writer options: %w", err)
	}

	tables, rowExtractor := parquetx.FindTablesInMessageDescriptor(descriptor, options.DefaultColumnCompression, logger, tracer)
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables found in message descriptor")
	}

	tablesByName := make(map[string]parquetx.TableResult, len(tables))
	for _, table := range tables {
		tablesByName[table.Schema.Name()] = table
	}

	return &ParquetWriter{
		options:      options,
		descriptor:   descriptor,
		tables:       tables,
		tablesByName: tablesByName,
		rowExtractor: rowExtractor,
	}, nil
}

// CloseBoundary implements Writer.
func (p *ParquetWriter) CloseBoundary(ctx context.Context) (Uploadeable, error) {
	defer func() {
		p.activeRange = nil
		p.rowsBufferByTableName = nil
	}()

	if p.activeRange == nil {
		return nil, fmt.Errorf("no active range, unable to close boundary")
	}

	uploadables := make([]Uploadeable, len(p.tables))
	for i, table := range p.tables {
		rows, found := p.rowsBufferByTableName[table.Schema.Name()]
		if !found {
			panic(fmt.Errorf("no rows found for table %q, should have been created", table.Schema.Name()))
		}

		uploadables[i] = uploadTableFile(table.Schema, rows, p.activeRange)
	}

	return UploadeableFunc(func(ctx context.Context, store dstore.Store) (out string, err error) {
		type uploadResult struct {
			filename  string
			uploadErr error
		}

		work := make(chan Uploadeable)
		results := make(chan uploadResult)

		// create worker 5 goroutines
		wg := sync.WaitGroup{}
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for s := range work {
					filename, uploadErr := s.Upload(ctx, store)
					results <- uploadResult{filename, uploadErr}
				}
			}()
		}

		go func() {
			for _, s := range uploadables {
				work <- s
			}

			close(work)

			wg.Wait()
			close(results)
		}()

		filenames := make([]string, 0, len(uploadables))
		for result := range results {
			filenames = append(filenames, result.filename)
			err = multierr.Append(err, result.uploadErr)
		}

		return strings.Join(filenames, ","), err
	}), nil
}

func uploadTableFile(schema *parquet.Schema, rows *parquet.RowBuffer[any], activeRange *bstream.Range) Uploadeable {
	filename := fmt.Sprintf(path.Join(schema.Name(), "%010d-%010d.parquet"), activeRange.StartBlock(), *activeRange.EndBlock())

	return UploadeableFunc(func(ctx context.Context, store dstore.Store) (string, error) {
		reader, writer := io.Pipe()

		go func() {
			defer writer.Close()

			parquetWriter := parquet.NewWriter(writer, &parquet.WriterConfig{
				// FIXME: Add version too?
				CreatedBy: "substreams-sink-files",
				Schema:    schema,
			})

			_, err := parquetWriter.WriteRowGroup(rows)
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
	})
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
	p.rowsBufferByTableName = make(map[string]*parquet.RowBuffer[any], len(p.tables))
	for _, table := range p.tables {
		p.rowsBufferByTableName[table.Schema.Name()] = parquet.NewRowBuffer[any](&parquet.RowGroupConfig{
			Schema: table.Schema,
		})
	}

	return nil
}

func (p *ParquetWriter) EncodeMapModule(output *pbsubstreamsrpc.MapModuleOutput) error {
	if p.activeRange == nil {
		return fmt.Errorf("active range must be set via StartBoundary before calling EncodeMapModule")
	}

	messageFullName := strings.TrimPrefix(output.MapOutput.TypeUrl, "type.googleapis.com/")
	if messageFullName != string(p.descriptor.FullName()) {
		return fmt.Errorf("received message type URL %q doesn't match expected output type %q", messageFullName, p.descriptor.FullName())
	}

	dynamicMsg := dynamicpb.NewMessage(p.descriptor)
	err := proto.Unmarshal(output.MapOutput.Value, dynamicMsg)
	if err != nil {
		return fmt.Errorf("unmarshal message as proto: %w", err)
	}

	rowsByTable, err := p.rowExtractor.ExtractRows(dynamicMsg)
	if err != nil {
		return fmt.Errorf("extracting rows from message %q: %w", messageFullName, err)
	}

	for tableName, rows := range rowsByTable {
		rowsBuffer, found := p.rowsBufferByTableName[tableName]
		if !found {
			panic(fmt.Errorf("rows buffer must be initialized on StartBoundary, code is wrong"))
		}

		n, err := rowsBuffer.WriteRows(rows)
		if err != nil {
			return fmt.Errorf("writing rows to buffer: %w", err)
		}

		if n != len(rows) {
			return fmt.Errorf("expected to write %d rows, but wrote %d", len(rows), n)
		}
	}

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
