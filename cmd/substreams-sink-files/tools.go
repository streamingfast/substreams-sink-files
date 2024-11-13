package main

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"github.com/streamingfast/substreams-sink-files/parquetx"
)

var ToolsParquet = Group("parquet", "Parquet related tools",
	Command(toolsParquetSchemaE,
		"schema <manifest> [<output_module>]",
		"Generate a parquet schema from a proto message",
		Flags(func(flags *pflag.FlagSet) {
			addCommonParquetFlags(flags)
		}),
		RangeArgs(1, 2),
	),
)

func toolsParquetSchemaE(cmd *cobra.Command, args []string) error {
	moduleName := sink.InferOutputModuleFromPackage
	if len(args) == 2 {
		moduleName = args[1]
	}

	pkg, module, outputModuleHash, err := sink.ReadManifestAndModule(
		args[0],
		"",
		nil,
		moduleName,
		sink.IgnoreOutputModuleType,
		false,
		zlog,
	)
	cli.NoError(err, "Read manifest failed")

	sinker, err := sink.New(sink.SubstreamsModeProduction, true, pkg, module, outputModuleHash, nil, zlog, tracer)
	cli.NoError(err, "New sinker failed")

	descriptor, err := outputProtoreflectMessageDescriptor(sinker)
	cli.NoError(err, "Failed to extract message descriptor from output module")

	parquetWriterOptions, err := writer.NewParquetWriterOptions(readCommonParquetFlags(cmd).AsParquetWriterOptions())
	cli.NoError(err, "Failed to create parquet writer options")

	tables, _ := parquetx.FindTablesInMessageDescriptor(descriptor, parquetWriterOptions.DefaultColumnCompression)
	if len(tables) == 0 {
		fmt.Printf("No tables found or inferred in message descriptor %q\n", descriptor.FullName())
		os.Exit(1)
	}

	longuestLine := 0
	for _, table := range tables {
		schema := strings.ReplaceAll(table.Schema.String(), "\t", "    ")
		schemaLines := strings.Split(schema, "\n")
		schemaLongestLine := slices.MaxFunc(schemaLines, func(a, b string) int { return len(a) - len(b) })

		if len(schemaLongestLine) > longuestLine {
			longuestLine = len(schemaLongestLine)
		}
	}

	if longuestLine > 120 {
		longuestLine = 120
	}

	for i, table := range tables {
		if i != 0 {
			fmt.Println()
		}

		fmt.Println(centerString(" Table from "+string(table.Descriptor.FullName())+" ", longuestLine))
		fmt.Println(strings.ReplaceAll(table.Schema.String(), "\t", "    "))
		fmt.Println(strings.Repeat("-", longuestLine))
	}

	return nil
}

func centerString(input string, totalWidth int) string {
	inputLength := len(input)
	if inputLength >= totalWidth {
		return input
	}

	padding := (totalWidth - inputLength) / 2
	left := strings.Repeat("-", padding)
	right := strings.Repeat("-", totalWidth-inputLength-padding)

	return fmt.Sprintf("%s%s%s", left, input, right)
}
