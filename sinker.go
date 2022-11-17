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
	bdler        *bundler.Bundler
	stateStore   *StateStore
	sink         *sink.Syncer

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

	fs.stateStore = NewStateStore(fs.config.SubstreamStateStorePath)

	cursor, err := fs.stateStore.Read()
	if err != nil {
		return fmt.Errorf("faile to read curosor: %w", err)
	}

	fs.logger.Info("resolved block range", zap.Object("block_range", blockRange), zap.Reflect("cursor", cursor))

	fs.bdler, err = bundler.New(
		fs.config.FileStore,
		fs.config.BlockPerFile,
		blockRange.StartBlock(),
		bundler.BundlerTypeJSON,
		fs.logger,
	)
	if err != nil {
		return fmt.Errorf("new bunlder: %w", err)
	}

	sink, err := sink.New(
		fs.config.Pkg.Modules,
		outputModule.module,
		outputModule.hash,
		fs.handleBlockScopeData,
		fs.config.ClientConfig,
		cursor,
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

	if err := sink.Start(ctx, blockRange); err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}

	if err := fs.bdler.ForceFlush(ctx); err != nil {
		return fmt.Errorf("force flush: %w", err)
	}

	return fs.stateStore.Save(sink.GetState())
}

func (fs *FileSinker) handleBlockScopeData(ctx context.Context, cursor *sink.Cursor, data *pbsubstreams.BlockScopedData) error {
	flushed, err := fs.bdler.Flush(ctx, cursor.Block.Num())
	if err != nil {
		return fmt.Errorf("failed to roll: %w", err)
	}
	if flushed {
		if err := fs.stateStore.Save(fs.sink.GetState()); err != nil {
			return fmt.Errorf("save state store: %w", err)
		}
	}
	for _, output := range data.Outputs {
		if output.Name != fs.config.OutputModuleName {
			continue
		}

		resolved, err := fs.config.EntitiesQuery.Resolve(output.GetMapOutput().GetValue(), fs.outputModule.descriptor)
		if err != nil {
			return fmt.Errorf("failed to resolve entities query: %w", err)
		}

		fs.bdler.Write(resolved)

	}

	return nil
}
