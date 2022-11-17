package substreams_file_sink

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
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
	bundler      *Bundler

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

	fs.logger.Info("resolved block range", zap.Object("block_range", blockRange))

	fs.bundler = newBundler(fs.config.FileStore, fs.config.BlockPerFile, blockRange.StartBlock(), BundlerTypeJSON)

	sink, err := sink.New(
		fs.config.SubstreamStateStorePath,
		fs.config.Pkg.Modules,
		outputModule.module,
		outputModule.hash,
		fs.config.ClientConfig,
		fs.handleBlockScopeData,
		fs.logger,
		fs.tracer,
	)
	if err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}
	sink.OnTerminating(fs.Shutdown)
	fs.OnTerminating(func(err error) {
		fs.logger.Info(" file sinker terminating shutting down sink")
		sink.Shutdown(err)
	})

	if err := sink.Start(ctx, blockRange); err != nil {
		return fmt.Errorf("sink failed: %w", err)
	}
	return nil
}

func (fs *FileSinker) handleBlockScopeData(cursor *sink.Cursor, data *pbsubstreams.BlockScopedData) error {
	
	for _, output := range data.Outputs {
		if output.Name != fs.config.OutputModuleName {
			continue
		}

		dynMsg := dynamic.NewMessageFactoryWithDefaults().NewDynamicMessage(fs.outputModule.descriptor)
		if err := dynMsg.Unmarshal(output.GetMapOutput().GetValue()); err != nil {
			return fmt.Errorf("failed to unmarshal: %w", err)
		}

		cnt, err := json.Marshal(dynMsg)
		if err != nil {
			return err
		}

		fs.bundler.Write(cursor, dynMsg)
		fmt.Println(string(cnt))
	}

	return nil
}
