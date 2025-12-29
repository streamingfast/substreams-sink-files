package bundler

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dhammer"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"github.com/streamingfast/substreams-sink-files/state"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

type Bundler struct {
	*shutter.Shutter

	blockCount     uint64
	stats          *boundaryStats
	boundaryWriter writer.Writer
	outputStore    dstore.Store
	stateStore     state.Store
	activeBoundary *bstream.Range
	uploadQueue    *dhammer.Nailer
	zlogger        *zap.Logger

	// Date-aware boundary management
	datePartitioningEnabled bool
	dateFormat              string
	currentBoundaryDate     string
	lastProcessedBlock      uint64
}

func New(
	size uint64,
	boundaryWriter writer.Writer,
	stateStore state.Store,
	outputStore dstore.Store,
	zlogger *zap.Logger,
) (*Bundler, error) {

	b := &Bundler{
		Shutter:        shutter.New(),
		boundaryWriter: boundaryWriter,
		stateStore:     stateStore,
		outputStore:    outputStore,
		blockCount:     size,
		stats:          newStats(),
		zlogger:        zlogger,
	}

	b.uploadQueue = dhammer.NewNailer(5, b.uploadBoundary, dhammer.NailerLogger(zlogger))

	return b, nil
}

func (b *Bundler) Launch(ctx context.Context) {
	b.OnTerminating(func(err error) {
		b.zlogger.Info("shutting down bundler", zap.Error(err))
		b.Close()
	})

	b.uploadQueue.Start(ctx)
	go func() {
		for v := range b.uploadQueue.Out {
			bf := v.(*boundaryFile)

			if err := bf.state.Save(); err != nil {
				b.Shutdown(fmt.Errorf("unable to save state: %w", err))
				return
			}
		}
		if b.uploadQueue.Err() != nil {
			b.Shutdown(fmt.Errorf("upload queue failed: %w", b.uploadQueue.Err()))
		}
	}()
}

func (b *Bundler) Close() {
	b.zlogger.Info("closing upload queue")
	b.uploadQueue.Close()
	b.zlogger.Info("waiting till queue is drained")
	b.uploadQueue.WaitUntilEmpty(context.Background())
	b.zlogger.Info("boundary uploaded completed")
}

func (b *Bundler) GetCursor() (*sink.Cursor, error) {
	return b.stateStore.ReadCursor()
}

func (b *Bundler) SetDatePartitioning(enabled bool, format string) {
	b.datePartitioningEnabled = enabled
	b.dateFormat = format
}

func (b *Bundler) RollWithDateCheck(ctx context.Context, blockNum uint64, blockTime time.Time) error {
	// Initialize boundary date if this is the first block with a timestamp
	if b.datePartitioningEnabled && !blockTime.IsZero() && b.currentBoundaryDate == "" {
		b.currentBoundaryDate = blockTime.UTC().Format(b.dateFormat)
		b.zlogger.Info("initialized boundary date",
			zap.Uint64("block_num", blockNum),
			zap.String("boundary_date", b.currentBoundaryDate),
		)
	}

	// Check if date partitioning requires early boundary closure
	if b.datePartitioningEnabled && b.shouldCloseForDateChange(blockTime) {
		b.zlogger.Info("closing boundary due to date change",
			zap.Uint64("block_num", blockNum),
			zap.String("current_date", b.currentBoundaryDate),
			zap.String("new_date", blockTime.UTC().Format(b.dateFormat)),
		)

		// Close current boundary with actual processed blocks
		if err := b.stopWithActualRange(ctx); err != nil {
			return fmt.Errorf("stop boundary for date change: %w", err)
		}

		// Start new boundary at current block
		if err := b.Start(blockNum); err != nil {
			return fmt.Errorf("start boundary after date change: %w", err)
		}

		// Update the boundary date
		b.currentBoundaryDate = blockTime.UTC().Format(b.dateFormat)
		b.lastProcessedBlock = blockNum
		return nil
	}

	// Regular block count based rolling
	if b.activeBoundary.Contains(blockNum) {
		b.lastProcessedBlock = blockNum
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

	b.lastProcessedBlock = blockNum
	return nil
}

// shouldCloseForDateChange checks if the current block's date is different from the boundary's date
func (b *Bundler) shouldCloseForDateChange(blockTime time.Time) bool {
	if !b.datePartitioningEnabled || b.activeBoundary == nil || blockTime.IsZero() {
		return false
	}

	blockDate := blockTime.UTC().Format(b.dateFormat)
	return b.currentBoundaryDate != "" && b.currentBoundaryDate != blockDate
}

// stopWithActualRange closes the current boundary but adjusts the end block to the last processed block
func (b *Bundler) stopWithActualRange(ctx context.Context) error {
	if b.activeBoundary == nil {
		return nil
	}

	b.zlogger.Info("stopping file boundary with actual range",
		zap.Uint64("original_end", *b.activeBoundary.EndBlock()),
		zap.Uint64("actual_end", b.lastProcessedBlock+1),
	)

	// Create adjusted range with actual processed blocks
	adjustedRange := bstream.NewRangeExcludingEnd(b.activeBoundary.StartBlock(), b.lastProcessedBlock+1)

	// Update the writer's boundary if it supports adjustment
	if adjustable, ok := b.boundaryWriter.(writer.BoundaryAdjustable); ok {
		adjustable.AdjustBoundary(adjustedRange)
	}

	file, err := b.boundaryWriter.CloseBoundary(ctx)
	if err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	state, err := b.stateStore.GetState()
	if err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	b.zlogger.Info("queuing boundary upload",
		zap.Stringer("boundary", adjustedRange),
	)
	b.uploadQueue.In <- &boundaryFile{
		name:  adjustedRange.String(),
		file:  file,
		state: state,
	}

	b.activeBoundary = nil
	b.stats.endBoundary()
	b.zlogger.Info("bundler stats", b.stats.Log()...)
	return nil
}

func (b *Bundler) TrackBlockProcessDuration(elapsed time.Duration) {
	b.stats.addProcessingDataDur(elapsed)
}

func (b *Bundler) Writer() writer.Writer {
	return b.boundaryWriter
}

func (b *Bundler) SetCursor(cursor *sink.Cursor) {
	b.stateStore.SetCursor(cursor)
}

func (b *Bundler) Start(blockNum uint64) error {
	boundaryRange := b.newBoundary(blockNum)
	b.activeBoundary = boundaryRange
	b.lastProcessedBlock = blockNum

	b.zlogger.Info("starting new file boundary", zap.Stringer("boundary", boundaryRange))
	if err := b.boundaryWriter.StartBoundary(boundaryRange); err != nil {
		return fmt.Errorf("start file: %w", err)
	}

	b.stats.startBoundary(boundaryRange)
	b.zlogger.Info("boundary started", zap.Stringer("boundary", boundaryRange))
	b.stateStore.NewBoundary(boundaryRange)

	return nil
}

func (b *Bundler) stop(ctx context.Context) error {
	b.zlogger.Info("stopping file boundary")

	file, err := b.boundaryWriter.CloseBoundary(ctx)
	if err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	state, err := b.stateStore.GetState()
	if err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	b.zlogger.Info("queuing boundary upload",
		zap.Stringer("boundary", b.activeBoundary),
	)
	b.uploadQueue.In <- &boundaryFile{
		name:  b.activeBoundary.String(),
		file:  file,
		state: state,
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

type boundaryFile struct {
	name  string
	file  writer.Uploadeable
	state state.Saveable
}

func (b *Bundler) uploadBoundary(ctx context.Context, v interface{}) (interface{}, error) {
	bf := v.(*boundaryFile)

	outputPath, err := bf.file.Upload(ctx, b.outputStore)
	if err != nil {
		return nil, fmt.Errorf("unable to upload: %w", err)
	}
	b.zlogger.Info("boundary uploaded",
		zap.String("boundary", bf.name),
		zap.String("output_path", outputPath),
	)

	return bf, nil
}

// Roll is kept for backward compatibility but should use RollWithDateCheck when possible
func (b *Bundler) Roll(ctx context.Context, blockNum uint64) error {
	return b.RollWithDateCheck(ctx, blockNum, time.Time{})
}
