package main

import (
	"context"
	"fmt"
	"github.com/streamingfast/dstore"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/shutter"
	substreamsfile "github.com/streamingfast/substreams-sink-files"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
)

// sync run ./localdata api.dev.eth.mainnet.com substrema.spkg map_transfers .transfers[]

var SyncRunCmd = Command(syncRunE,
	"run <output_store> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs  extractor code",
	RangeArgs(4, 5),
	Flags(func(flags *pflag.FlagSet) {
		flags.String("state-store", "./state.yaml", "Output path where to store latest received cursor, if empty, cursor will not be persisted")
		flags.BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
		flags.BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")
		flags.Uint64P("file-block-count", "c", 10000, "Number of blocks per file")
	}),
	AfterAllHook(func(_ *cobra.Command) {
		substreamsfile.RegisterMetrics()
	}),
)

func syncRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	filestorePath := args[0]
	endpoint := args[1]
	manifestPath := args[2]
	outputModuleName := args[3]
	blockRange := ""
	if len(args) > 4 {
		blockRange = args[4]
	}
	stateStorePath := viper.GetString("state-store")
	blocksPerFile := viper.GetUint64("substreams-sink-files-run-file-block-count")
	zlog.Info("sink to files",
		zap.String("file_store_path", filestorePath),
		zap.String("endpoint", endpoint),
		zap.String("manifest_path", manifestPath),
		zap.String("output_module_name", outputModuleName),
		zap.String("block_range", blockRange),
		zap.String("state_store", stateStorePath),
		zap.Uint64("blocks_per_file", blocksPerFile),
	)

	fileOutputStore, err := dstore.NewStore(filestorePath, "", "", false)
	if err != nil {
		return fmt.Errorf("new store %q: %w", filestorePath, err)
	}

	zlog.Info("reading substreams manifest", zap.String("manifest_path", manifestPath))
	pkg, err := manifest.NewReader(manifestPath).Read()
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	apiToken := readAPIToken()

	config := &substreamsfile.Config{
		SubstreamStateStorePath: stateStorePath,
		FileStore:               fileOutputStore,
		BlockRange:              blockRange,
		Pkg:                     pkg,
		OutputModuleName:        outputModuleName,
		BlockPerFile:            blocksPerFile,
		ClientConfig: client.NewSubstreamsClientConfig(
			endpoint,
			apiToken,
			viper.GetBool("substreams-sink-files-run-insecure"),
			viper.GetBool("substreams-sink-files-run-plaintext"),
		),
	}

	fileSinker := substreamsfile.NewFileSinker(config, zlog, tracer)
	fileSinker.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		zlog.Info("application terminating shutting down file sinker")
		fileSinker.Shutdown(err)
	})

	go func() {
		fileSinker.Shutdown(fileSinker.Run(ctx))
	}()

	signalHandler := derr.SetupSignalHandler(0 * time.Second)
	zlog.Info("ready, waiting for signal to quit")
	select {
	case <-signalHandler:
		zlog.Info("received termination signal, quitting application")
		go app.Shutdown(nil)
	case <-app.Terminating():
		NoError(app.Err(), "application shutdown unexpectedly, quitting")
	}

	zlog.Info("waiting for app termination")
	select {
	case <-app.Terminated():
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
		zlog.Error("application did not terminated within 30s, forcing exit")
	}
	return nil
}
