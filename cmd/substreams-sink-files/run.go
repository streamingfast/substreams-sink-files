package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	substreamsfile "github.com/streamingfast/substreams-sink-files"
	"github.com/streamingfast/substreams-sink-files/bundler"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"github.com/streamingfast/substreams-sink-files/encoder"
	"github.com/streamingfast/substreams-sink-files/protox"
	"github.com/streamingfast/substreams-sink-files/state"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var SyncRunCmd = Command(syncRunE,
	"run <endpoint> <manifest> <module> <output_store> [<start>:<stop>]",
	"Runs extractor code",
	RangeArgs(4, 5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)

		flags.String("file-working-dir", "./localdata/working", "Working store where we accumulate data")
		flags.String("state-store", "./state.yaml", "Output path where to store latest received cursor, if empty, cursor will not be persisted")
		flags.Uint64P("file-block-count", "c", 10000, "Number of blocks per file")
		flags.String("encoder", "", "Sets which encoder to use to parse the Substreams Output Module data. Options are: 'lines', 'parquet', 'proto:<_proto_path_to_field>'")
		flags.Uint64("buffer-max-size", 64*1024*1024, FlagDescription(`
			Amount of memory bytes to allocate to the buffered writer. If your data set is small enough that every is hold in memory, we are going to avoid
			the local I/O operation(s) and upload accumulated content in memory directly to final storage location.

			Ideally, you should set this as to about 80%% of RAM the process has access to. This will maximize amout of element in memory,
			and reduce 'syscall' and I/O operations to write to the temporary file as we are buffering a lot of data.

			This setting has probably the greatest impact on writing throughput.

			Default value for the buffer is 64 MiB.
		`))
	}),
	ExamplePrefixed("substreams-sink-files run",
		"mainnet.eth.streamingfast.io:443 substreams.spkg map_transfers '.transfers[]' ./localdata",
	),
)

func syncRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	sink.RegisterMetrics()

	endpoint := args[0]
	manifestPath := args[1]
	outputModuleName := args[2]
	fileOutputPath := args[3]
	blockRange := ""
	if len(args) > 4 {
		blockRange = args[4]
	}

	fileWorkingDir := sflags.MustGetString(cmd, "file-working-dir")
	stateStorePath := sflags.MustGetString(cmd, "state-store")
	blocksPerFile := sflags.MustGetUint64(cmd, "file-block-count")
	bufferMaxSize := sflags.MustGetUint64(cmd, "buffer-max-size")
	encoderType := sflags.MustGetString(cmd, "encoder")

	zlog.Info("sink to files",
		zap.String("file_output_path", fileOutputPath),
		zap.String("file_working_dir", fileWorkingDir),
		zap.String("encoder_type", encoderType),
		zap.String("state_store", stateStorePath),
		zap.Uint64("blocks_per_file", blocksPerFile),
		zap.Uint64("buffer_max_size", bufferMaxSize),
	)

	sinker, err := sink.NewFromViper(cmd,
		sink.IgnoreOutputModuleType,
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("new sinker: %w", err)
	}

	if blockRange := sinker.BlockRange(); blockRange.EndBlock() != nil {
		size, err := sinker.BlockRange().Size()
		if err != nil {
			panic(fmt.Errorf("size should error only on open ended range, which we should have caught earlier: %w", err))
		}

		cli.Ensure(size >= blocksPerFile, "You requested %d blocks per file but your block range spans only %d blocks, this would produce 0 file, refusing to start", blocksPerFile, size)
	}

	fileOutputStore, err := dstore.NewStore(fileOutputPath, "", "", false)
	if err != nil {
		return fmt.Errorf("new store %q: %w", fileOutputPath, err)
	}

	stateFileDirectory := filepath.Dir(stateStorePath)
	if err := os.MkdirAll(stateFileDirectory, os.ModePerm); err != nil {
		return fmt.Errorf("create state file directories: %w", err)
	}

	stateStore, err := state.NewFileStateStore(stateStorePath)
	if err != nil {
		return fmt.Errorf("new file state store: %w", err)
	}

	var boundaryWriter writer.Writer
	var sinkEncoder encoder.Encoder

	switch {
	case encoderType == "lines" || strings.HasPrefix(encoderType, "proto:"):
		boundaryWriter = writer.NewBufferedIO(bufferMaxSize, fileWorkingDir, writer.FileTypeJSONL, zlog)
		sinkEncoder, err = getEncoder(encoderType, sinker)
		if err != nil {
			return fmt.Errorf("failed to create encoder: %w", err)
		}

	case encoderType == "parquet":
		msgDesc, err := outputProtoreflectMessageDescriptor(sinker)
		if err != nil {
			return fmt.Errorf("output module message descriptor: %w", err)
		}

		parquetWriter, err := writer.NewParquetWriter(msgDesc)
		if err != nil {
			return fmt.Errorf("new parquet writer: %w", err)
		}

		boundaryWriter = parquetWriter
		sinkEncoder = encoder.EncoderFunc(func(output *pbsubstreamsrpc.MapModuleOutput, _ writer.Writer) error {
			return parquetWriter.EncodeMapModule(output)
		})

	default:
		return fmt.Errorf("unknown encoder type %q", encoderType)
	}

	bundler, err := bundler.New(
		blocksPerFile,
		boundaryWriter,
		stateStore,
		fileOutputStore,
		zlog,
	)
	if err != nil {
		return fmt.Errorf("new bundler: %w", err)
	}

	fileSinker := substreamsfile.NewFileSinker(sinker, bundler, sinkEncoder, zlog, tracer)
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

func getEncoder(encoderType string, sinker *sink.Sinker) (encoder.Encoder, error) {
	if encoderType == "lines" {
		return encoder.NewLineEncoder(), nil
	}

	if strings.HasPrefix(encoderType, "proto:") {
		msgDesc, err := outputMessageDescriptor(sinker)
		if err != nil {
			return nil, fmt.Errorf("output message descriptor: %w", err)
		}

		return encoder.NewProtoToJson(strings.Replace(encoderType, "proto:", "", 1), msgDesc)
	}

	return nil, fmt.Errorf("unknown encoder type %q", encoderType)
}

func outputMessageDescriptor(sinker *sink.Sinker) (*desc.MessageDescriptor, error) {
	fileDescs, err := desc.CreateFileDescriptors(sinker.Package().ProtoFiles)
	if err != nil {
		return nil, fmt.Errorf("couldn't convert, should do this check much earlier: %w", err)
	}

	outputModuleType := sinker.OutputModuleTypeUnprefixed()

	var msgDesc *desc.MessageDescriptor
	for _, file := range fileDescs {
		msgDesc = file.FindMessage(outputModuleType)
		if msgDesc != nil {
			return msgDesc, nil
		}
	}

	return nil, fmt.Errorf("output module descriptor not found")
}

func outputProtoreflectMessageDescriptor(sinker *sink.Sinker) (protoreflect.MessageDescriptor, error) {
	outputTypeName := protoreflect.FullName(sinker.OutputModuleTypeUnprefixed())
	value, err := protox.FindMessageByNameInFiles(sinker.Package().ProtoFiles, outputTypeName)
	if err != nil {
		return nil, fmt.Errorf("find message by name in files: %w", err)
	}

	if value == nil {
		return nil, fmt.Errorf("output module %q descriptor not found in proto files", outputTypeName)
	}

	return value, nil
}
