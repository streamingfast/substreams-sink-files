package substreams_file_sink

import (
	"context"
	"fmt"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-sink-files/bundler"
	"github.com/streamingfast/substreams-sink-files/sink"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

func RegisterMetrics() {
	sink.RegisterMetrics()
}

type FileSinker struct {
	*shutter.Shutter
	config       *Config
	outputModule *OutputModule

	bundler *bundler.Bundler

	//stateStore   *bundler.StateStore
	sink *sink.Syncer

	logger *zap.Logger
	tracer logging.Tracer
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

	fs.outputModule = outputModule

	blockRange, err := resolveBlockRange(fs.config.BlockRange, outputModule.module)
	if err != nil {
		return fmt.Errorf("resolve block range: %w", err)
	}

	fs.bundler, err = bundler.New(
		fs.config.FileOutputStore,
		fs.config.FileWorkingStore,
		fs.config.SubstreamStateStorePath,
		fs.config.BlockPerFile,
		bundler.FileTypeJSONL,
		fs.logger,
	)
	if err != nil {
		return fmt.Errorf("new bunlder: %w", err)
	}

	cursor, err := fs.bundler.GetCursor()
	if err != nil {
		return fmt.Errorf("faile to read curosor: %w", err)
	}

	fs.logger.Info("setting up sink", zap.Object("block_range", blockRange), zap.Reflect("cursor", cursor))

	sink, err := sink.New(
		fs.config.Pkg.Modules,
		outputModule.module,
		outputModule.hash,
		fs.handleBlockScopeData,
		fs.config.ClientConfig,
		fs.logger,
		fs.tracer,
	)
	if err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}
	fs.sink = sink

	sink.OnTerminating(fs.Shutdown)
	fs.OnTerminating(func(err error) {
		fs.logger.Info(" file sinker terminating shutting down sink")
		sink.Shutdown(err)
	})

	expectedStartBlock := blockRange.StartBlock()
	if !cursor.IsBlank() {
		expectedStartBlock = cursor.Block.Num()
	}

	if err := fs.bundler.Start(expectedStartBlock); err != nil {
		return fmt.Errorf("unable to start bunlder: %w", err)
	}

	if err := sink.Start(ctx, blockRange, cursor); err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}

	//if err := fs.bundler.Stop(ctx); err != nil {
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

		resolved, err := fs.config.EntitiesQuery.Resolve(output.GetMapOutput().GetValue(), fs.outputModule.descriptor)
		if err != nil {
			return fmt.Errorf("failed to resolve entities query: %w", err)
		}

		if err := fs.bundler.Write(cursor, resolved); err != nil {
			return fmt.Errorf("failed to write entities: %w", err)
		}
	}

	return nil
}
