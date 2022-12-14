package substreams_file_sink

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/substreams-sink-files/encoder"

	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-files/bundler"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

func RegisterMetrics() {
	sink.RegisterMetrics()
}

type FileSinker struct {
	*shutter.Shutter
	config *Config

	bundler *bundler.Bundler
	sink    *sink.Sinker

	logger  *zap.Logger
	tracer  logging.Tracer
	encoder encoder.Encoder
}

func NewFileSinker(config *Config, logger *zap.Logger, tracer logging.Tracer) *FileSinker {
	return &FileSinker{
		Shutter: shutter.New(),
		config:  config,
		logger:  logger,
		tracer:  tracer,
	}
}

func (fs *FileSinker) Run(ctx context.Context) error {

	outputModule, err := fs.config.validateOutputModule()
	if err != nil {
		return fmt.Errorf("invalid output module: %w", err)
	}

	blockRange, err := resolveBlockRange(fs.config.BlockRange, outputModule.module)
	if err != nil {
		return fmt.Errorf("resolve block range: %w", err)
	}

	fs.bundler, err = bundler.New(
		fs.config.BlockPerFile,
		fs.config.getBoundaryWriter(fs.logger),
		fs.config.StateStore,
		fs.config.FileOutputStore,
		fs.logger,
	)
	if err != nil {
		return fmt.Errorf("new bunlder: %w", err)
	}
	fs.bundler.OnTerminating(fs.Shutdown)
	fs.OnTerminating(fs.bundler.Shutdown)

	fs.bundler.Launch(ctx)

	encoder, err := fs.config.getEncoder(outputModule)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	fs.encoder = encoder

	cursor, err := fs.bundler.GetCursor()
	if err != nil {
		return fmt.Errorf("faile to read curosor: %w", err)
	}

	fs.logger.Info("setting up sink", zap.Object("block_range", blockRange), zap.Reflect("cursor", cursor))

	fs.sink, err = sink.New(
		sink.SubstreamsModeProduction,
		fs.config.Pkg.Modules,
		outputModule.module,
		outputModule.hash,
		fs.handleBlockScopeData,
		fs.config.ClientConfig,
		[]pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_IRREVERSIBLE},
		fs.logger,
		fs.tracer,
	)
	if err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}

	fs.sink.OnTerminating(fs.Shutdown)
	fs.OnTerminating(func(err error) {
		fs.logger.Info(" file sinker terminating shutting down sink")
		fs.sink.Shutdown(err)
	})

	expectedStartBlock := blockRange.StartBlock()
	if !cursor.IsBlank() {
		expectedStartBlock = cursor.Block.Num()
	}

	if err := fs.bundler.Start(expectedStartBlock); err != nil {
		return fmt.Errorf("unable to start bunlder: %w", err)
	}

	if err := fs.sink.Start(ctx, blockRange, cursor); err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}

	//if err := fs.bundler.stop(ctx); err != nil {
	//	return fmt.Errorf("force stop: %w", err)
	//}
	return nil
}

func (fs *FileSinker) handleBlockScopeData(ctx context.Context, cursor *sink.Cursor, data *pbsubstreams.BlockScopedData) error {
	if err := fs.bundler.Roll(ctx, cursor.Block.Num()); err != nil {
		return fmt.Errorf("failed to roll: %w", err)
	}

	for _, output := range data.Outputs {
		if output.Name != fs.config.OutputModuleName {
			continue
		}

		t0 := time.Now()
		if err := fs.encoder.EncodeTo(output, fs.bundler.Writer()); err != nil {
			return fmt.Errorf("encode block scoped data: %w", err)
		}
		fs.bundler.TrackBlockProcessDuration(time.Since(t0))

		fs.bundler.SetCursor(cursor)
	}

	return nil
}
