package bundler

import (
	"context"
	"fmt"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"os"
	"path/filepath"
	"time"

	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

type Bundler struct {
	blockCount uint64
	encoder    Encoder

	stats          *boundaryStats
	boundaryWriter writer.Writer
	stateStore     *StateStore
	fileType       writer.FileType
	activeBoundary *bstream.Range
	zlogger        *zap.Logger
}

func New(
	stateFilePath string,
	size uint64,
	boundaryWriter writer.Writer,
	zlogger *zap.Logger,
) (*Bundler, error) {
	stateFileDirectory := filepath.Dir(stateFilePath)
	if err := os.MkdirAll(stateFileDirectory, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create state file directories: %w", err)
	}

	stateStore, err := loadStateStore(stateFilePath)
	if err != nil {
		return nil, fmt.Errorf("load state store: %w", err)
	}

	b := &Bundler{
		boundaryWriter: boundaryWriter,
		stateStore:     stateStore,
		blockCount:     size,
		stats:          newStats(),
		zlogger:        zlogger,
	}

	switch boundaryWriter.Type() {
	case writer.FileTypeJSONL:
		b.encoder = JSONLEncode
	default:
		return nil, fmt.Errorf("invalid file type %q", boundaryWriter.Type())
	}
	return b, nil
}

func (b *Bundler) GetCursor() (*sink.Cursor, error) {
	return b.stateStore.Read()
}

func (b *Bundler) Roll(ctx context.Context, blockNum uint64) error {
	if b.activeBoundary.Contains(blockNum) {
		return nil
	}

	boundaries := boundariesToSkip(b.activeBoundary, blockNum, b.blockCount)

	b.zlogger.Info("block_num is not in active boundary",
		zap.Stringer("active_boundary", b.activeBoundary),
		zap.Int("boundaries_to_skip", len(boundaries)),
		zap.Uint64("block_num", blockNum),
	)

	if err := b.stop(ctx); err != nil {
		return fmt.Errorf("stop active boundary: %w", err)
	}

	for _, boundary := range boundaries {
		if err := b.Start(boundary.StartBlock()); err != nil {
			return fmt.Errorf("start skipping boundary: %w", err)
		}
		if err := b.stop(ctx); err != nil {
			return fmt.Errorf("stop skipping boundary: %w", err)
		}
	}

	if err := b.Start(blockNum); err != nil {
		return fmt.Errorf("start skipping boundary: %w", err)
	}
	return nil
}

func (b *Bundler) TrackBlockProcessDuration(elapsed time.Duration) {
	b.stats.addProcessingDataDur(elapsed)
}

func (b *Bundler) Write(cursor *sink.Cursor, data []byte) error {
	if err := b.boundaryWriter.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	b.stateStore.setCursor(cursor)
	return nil
}

func (b *Bundler) Start(blockNum uint64) error {
	boundaryRange := b.newBoundary(blockNum)
	b.activeBoundary = boundaryRange

	b.zlogger.Info("starting new file boundary", zap.Stringer("boundary", boundaryRange))
	if err := b.boundaryWriter.StartBoundary(boundaryRange); err != nil {
		return fmt.Errorf("start file: %w", err)
	}

	b.stats.startBoundary(boundaryRange)
	b.zlogger.Info("boundary started", zap.Stringer("boundary", boundaryRange))
	b.stateStore.newBoundary(boundaryRange)
	return nil
}

func (b *Bundler) stop(ctx context.Context) error {
	b.zlogger.Info("stopping file boundary")

	if err := b.boundaryWriter.CloseBoundary(ctx); err != nil {
		return fmt.Errorf("closing file: %w", err)
	}
	t0 := time.Now()
	if err := b.boundaryWriter.Upload(ctx); err != nil {
		return err
	}
	b.stats.addUploadedDuration(time.Since(t0))

	if err := b.stateStore.Save(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}
	b.activeBoundary = nil

	b.stats.endBoundary()
	b.zlogger.Info("bundler stats", b.stats.Log()...)
	return nil
}

func (b *Bundler) newBoundary(containingBlockNum uint64) *bstream.Range {
	startBlock := containingBlockNum - (containingBlockNum % b.blockCount)
	return bstream.NewRangeExcludingEnd(startBlock, startBlock+b.blockCount)
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
