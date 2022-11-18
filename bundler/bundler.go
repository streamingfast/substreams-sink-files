package bundler

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/substreams-sink-files/sink"
	"go.uber.org/zap"
)

type FileType string

const (
	FileTypeJSONL FileType = "jsonl"
)

type Bundler struct {
	size    uint64
	encoder Encoder

	fileStores *DStoreIO
	stateStore *StateStore
	fileType   FileType

	activeBoundary *bstream.Range

	zlogger *zap.Logger
}

func New(
	store dstore.Store,
	stateFilePath string,
	size uint64,
	fileType FileType,
	zlogger *zap.Logger,
) (*Bundler, error) {

	stateStore, err := loadStateStore(stateFilePath)
	if err != nil {
		return nil, fmt.Errorf("load state store: %w", err)
	}

	b := &Bundler{
		fileStores: newDStoreIO(store, 3, zlogger),
		stateStore: stateStore,
		fileType:   fileType,
		size:       size,
		zlogger:    zlogger,
	}

	switch fileType {
	case FileTypeJSONL:
		b.encoder = JSONLEncode
	default:
		return nil, fmt.Errorf("invalid file type %q", fileType)
	}
	return b, nil
}

func (b *Bundler) GetCursor() (*sink.Cursor, error) {
	return b.stateStore.Read()
}

func (b *Bundler) Start(blockNum uint64) error {
	boundaryRange := b.newBoundary(blockNum)
	b.activeBoundary = boundaryRange
	filename := b.filename(boundaryRange)
	b.zlogger.Info("starting new file boundary", zap.Stringer("boundary", boundaryRange), zap.String("fiename", filename))

	if err := b.fileStores.StartFile(filename); err != nil {
		return fmt.Errorf("start file: %w", err)
	}

	b.stateStore.newBoundary(filename, boundaryRange)
	return nil
}

func (b *Bundler) Stop() error {
	b.zlogger.Info("stopping file boundary")

	if err := b.fileStores.CloseFile(); err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	if err := b.stateStore.Save(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	b.activeBoundary = nil
	return nil
}

func (b *Bundler) Roll(ctx context.Context, blockNum uint64) error {
	if b.activeBoundary.Contains(blockNum) {
		return nil
	}

	boundaries := boundariesToSkip(b.activeBoundary, blockNum, b.size)

	b.zlogger.Info("block_num is not in active boundary",
		zap.Stringer("active_boundary", b.activeBoundary),
		zap.Int("boundaries_to_skip", len(boundaries)),
		zap.Uint64("block_num", blockNum),
	)

	if err := b.Stop(); err != nil {
		return fmt.Errorf("stop active boundary: %w", err)
	}

	for _, boundary := range boundaries {
		if err := b.Start(boundary.StartBlock()); err != nil {
			return fmt.Errorf("start skipping boundary: %w", err)
		}
		if err := b.Stop(); err != nil {
			return fmt.Errorf("stop skipping boundary: %w", err)
		}
	}

	if err := b.Start(blockNum); err != nil {
		return fmt.Errorf("start skipping boundary: %w", err)
	}
	return nil
}

func (b *Bundler) newBoundary(containingBlockNum uint64) *bstream.Range {
	startBlock := containingBlockNum - (containingBlockNum % b.size)
	return bstream.NewRangeExcludingEnd(startBlock, startBlock+b.size)
}

func (b *Bundler) filename(blockRange *bstream.Range) string {
	return fmt.Sprintf("%010d-%010d.%s", blockRange.StartBlock(), (*blockRange.EndBlock()), b.fileType)
}

func (b *Bundler) Write(cursor *sink.Cursor, entities []*dynamic.Message) error {
	var buf []byte
	for _, entity := range entities {
		cnt, err := b.encoder(proto.Message(entity))
		if err != nil {
			return fmt.Errorf("failed to encode: %w", err)
		}
		buf = append(buf, cnt...)
	}

	if _, err := b.fileStores.activeWriter.Write(buf); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	b.stateStore.setCursor(cursor)
	return nil
}

func boundariesToSkip(lastBoundary *bstream.Range, blockNum uint64, size uint64) (out []*bstream.Range) {
	iter := *lastBoundary.EndBlock()
	endBlock := computeEndBlock(iter, size)
	for blockNum >= endBlock {
		out = append(out, bstream.NewRangeExcludingEnd(iter, endBlock))
		iter = endBlock
		endBlock = computeEndBlock(iter, size)
	}
	return out
}

func computeEndBlock(startBlockNum, size uint64) uint64 {
	return (startBlockNum + size) - (startBlockNum+size)%size
}
