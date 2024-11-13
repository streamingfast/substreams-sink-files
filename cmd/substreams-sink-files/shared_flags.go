package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
)

// addCommonParquetFlags adds common flags for Parquet encoder. The list of flags added by this function are:
// - parquet-default-column-compression
func addCommonParquetFlags(flags *pflag.FlagSet) {
	flags.String("parquet-default-column-compression", "", cli.FlagDescription(`
		The default column compression to use for all tables that is going to be created that doesn't have a specific column
		compression set.

		If set, if a Protobuf field doesn't have a column specific compression extension, the default compression will be used
		for that column. If the field has a specific compression set, the field column specific compression will be used.

		Available values are:
			- uncompressed
			- snappy
			- gzip
			- lz4_raw
			- brotli
			- zstd

		Note that this setting is only used when the encoder is set to 'parquet'.
	`))
}

type parquetCommonFlagValues struct {
	DefaultColumnCompression string
}

func (f parquetCommonFlagValues) AsParquetWriterOptions() []writer.ParquetWriterOption {
	writerOptions := []writer.ParquetWriterOption{}
	if f.DefaultColumnCompression != "" {
		writerOptions = append(writerOptions, writer.ParquetDefaultColumnCompression(f.DefaultColumnCompression))
	}

	return writerOptions
}

func readCommonParquetFlags(cmd *cobra.Command) parquetCommonFlagValues {
	return parquetCommonFlagValues{
		DefaultColumnCompression: sflags.MustGetString(cmd, "parquet-default-column-compression"),
	}
}
