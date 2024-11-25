package substreams_file_sink

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-files/bundler"
	"github.com/streamingfast/substreams-sink-files/encoder"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
)

type FileSinker struct {
	*shutter.Shutter
	*sink.Sinker

	bundler *bundler.Bundler
	encoder encoder.Encoder
	logger  *zap.Logger
	tracer  logging.Tracer
}

func NewFileSinker(sinker *sink.Sinker, bundler *bundler.Bundler, encoder encoder.Encoder, logger *zap.Logger, tracer logging.Tracer) *FileSinker {
	return &FileSinker{
		Shutter: shutter.New(),
		Sinker:  sinker,

		bundler: bundler,
		encoder: encoder,
		logger:  logger,
		tracer:  tracer,
	}
}

func (fs *FileSinker) Run(ctx context.Context) error {
	cursor, err := fs.bundler.GetCursor()
	if err != nil {
		return fmt.Errorf("failed to read cursor: %w", err)
	}

	fs.Sinker.OnTerminating(fs.Shutdown)
	fs.OnTerminating(func(err error) {
		fs.logger.Info("file sinker terminating")
		fs.Sinker.Shutdown(err)
	})

	fs.bundler.OnTerminating(fs.Shutdown)
	fs.OnTerminating(func(_ error) {
		fs.logger.Info("file sinker terminating, closing bundler")
		fs.bundler.Shutdown(nil)
	})

	fs.bundler.Launch(ctx)

	expectedStartBlock := uint64(0)
	if !cursor.IsBlank() {
		expectedStartBlock = cursor.Block().Num() + 1
	} else if blockRange := fs.BlockRange(); blockRange != nil {
		expectedStartBlock = blockRange.StartBlock()
	}

	if err := fs.bundler.Start(expectedStartBlock); err != nil {
		return fmt.Errorf("unable to start bundler: %w", err)
	}

	fs.logger.Info("starting file sink", zap.Stringer("restarting_at", cursor.Block()))
	fs.Sinker.Run(ctx, cursor, fs)

	return nil
}

func (fs *FileSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	if err := fs.bundler.Roll(ctx, cursor.Block().Num()); err != nil {
		return fmt.Errorf("failed to roll: %w", err)
	}

	startTime := time.Now()
	if err := fs.encoder.EncodeTo(data.Output, fs.bundler.Writer()); err != nil {
		return fmt.Errorf("encode block scoped data: %w", err)
	}

	fs.bundler.TrackBlockProcessDuration(time.Since(startTime))
	fs.bundler.SetCursor(cursor)

	return nil
}

func (fs *FileSinker) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal but there is no handling of undo, this is because you used `--undo-buffer-size=0` which is invalid right now")
}
