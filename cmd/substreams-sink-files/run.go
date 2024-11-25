package main

import (
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
	"github.com/streamingfast/dstore"
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
	"run <manifest> <module> <output_store> [<start>:<stop>]",
	"Runs extractor code",
	RangeArgs(4, 5),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)

		flags.String("file-working-dir", "./localdata/working", "Working store where we accumulate data")
		flags.String("state-store", "./state.yaml", "Output path where to store latest received cursor, if empty, cursor will not be persisted")
		flags.Uint64P("file-block-count", "c", 10000, "Number of blocks per file")
		flags.String("encoder", "parquet", FlagMultiLineDescription(`
			Sets which encoder to use to parse the Substreams Output Module data. Options are: 'parquet', 'lines', 'protojson:<jq like expression>'

			## Parquet

			When using 'parquet', the output module must be a protobuf message, and the encoder will write the data to a parquet file.
			Refer to project readme at https://github.com/streamingfast/substreams-sink-files/blob/master/README.md for details
			about how Parquet encoder works to transform Protobuf messages into Parquet "rows/columns".

			## Lines

			When using 'lines', the output module must be a 'sf.substreams.sink.files.v1.Lines', which is essentially a list of strings,
			each strings being a line of text. This works well for a variety of line based format like JSONL, CSV, Accounting formats,
			TSC, etc.

			See https://github.com/streamingfast/substreams-sink-files/blob/master/proto/sf/substreams/sink/files/v1/files.proto#L13-L26
			for the Protobuf definition of the 'Lines' message.

			## 'protojson:<jq like expression>'

			When using 'protojson:<jq like expression>', the output module must be a Protobuf message, and the encoder will write the data
			to a JSONL file by extracting each rows using the jq like expression.

			**Note**
			The JSON output format used is the Protobuf JSON output format from https://protobuf.dev/programming-guides/json/ and
			we specifically the Golang [protojson](https://pkg.go.dev/google.golang.org/protobuf/encoding/protojson) package to achieve that

			The <jq like expression> that will be used to extract rows from the Protobuf message only supports a single expression form
			which is '.<repeated_field_name>[]'. For example, if the output module is a Protobuf message:

			  message Output {
			    repeated Entity entities = 1;
			  }

			  message Entity {
			    ...
			  }

			The expression '.entities[]' would yield all entity from the list as individual rows in the JSONL file. So with an Output
			module message like this:

			  {
			    "entities": [
					{id: 1, name: "one"}
					{id: 2, name: "two"}
				]
			  }

			You would get the following JSONL file:

			  {"id": 1, "name": "one"}
			  {"id": 2, "name": "two"}

			This mode is a little bit less performant that the 'lines' encoder, as the JSON encoding is done on the fly.
		`))
		flags.Uint64("buffer-max-size", 64*1024*1024, FlagMultiLineDescription(`
			Amount of memory bytes to allocate to the buffered writer. If your data set is small enough that every is hold in memory, we are going to avoid
			the local I/O operation(s) and upload accumulated content in memory directly to final storage location.

			Ideally, you should set this as to about 80%% of RAM the process has access to. This will maximize amout of element in memory,
			and reduce 'syscall' and I/O operations to write to the temporary file as we are buffering a lot of data.

			This setting has probably the greatest impact on writing throughput.

			Default value for the buffer is 64 MiB.
		`))

		addCommonParquetFlags(flags)
	}),
	ExamplePrefixed("substreams-sink-files run",
		"mainnet.eth.streamingfast.io:443 substreams.spkg map_transfers '.transfers[]' ./localdata",
	),
)

func syncRunE(cmd *cobra.Command, args []string) error {
	app := cli.NewApplication(cmd.Context())

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
	case encoderType == "lines" || strings.HasPrefix(encoderType, "proto:") || strings.HasPrefix(encoderType, "protojson:"):
		boundaryWriter = writer.NewBufferedIO(bufferMaxSize, fileWorkingDir, writer.FileTypeJSONL, zlog)
		sinkEncoder, err = getEncoder(encoderType, sinker)
		if err != nil {
			return fmt.Errorf("failed to create encoder: %w", err)
		}

	case encoderType == "parquet":
		flagValues := readCommonParquetFlags(cmd)

		msgDesc, err := outputProtoreflectMessageDescriptor(sinker)
		if err != nil {
			return fmt.Errorf("output module message descriptor: %w", err)
		}

		parquetWriter, err := writer.NewParquetWriter(msgDesc, zlog, tracer, flagValues.AsParquetWriterOptions()...)
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

	app.SuperviseAndStart(substreamsfile.NewFileSinker(sinker, bundler, sinkEncoder, zlog, tracer))

	if err := app.WaitForTermination(zlog, 0*time.Second, 30*time.Second); err != nil {
		zlog.Info("app termination error", zap.Error(err))
		return err
	}

	zlog.Info("app terminated")
	return nil
}

func getEncoder(encoderType string, sinker *sink.Sinker) (encoder.Encoder, error) {
	if encoderType == "lines" {
		return encoder.NewLineEncoder(), nil
	}

	sanitizedEncoderType := strings.TrimPrefix(strings.TrimPrefix(encoderType, "protojson:"), "proto:")
	hadProtojsonPrefix := len(encoderType) != len(sanitizedEncoderType)

	if hadProtojsonPrefix {
		msgDesc, err := outputMessageDescriptor(sinker)
		if err != nil {
			return nil, fmt.Errorf("output message descriptor: %w", err)
		}

		return encoder.NewProtoToJson(sanitizedEncoderType, msgDesc)
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

func FlagMultiLineDescription(in string) string {
	return string(Description(in))
}
