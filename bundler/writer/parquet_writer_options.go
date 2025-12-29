package writer

import (
	"fmt"
	"strings"

	"github.com/bobg/go-generics/v2/maps"
	"github.com/bobg/go-generics/v2/slices"
	pbparquet "github.com/streamingfast/substreams-sink-files/v2/pb/parquet"
)

// ParquetWriterOptions holds the configuration options for the Parquet writer.
// It's the fully resolved, well typed version of ParquetWriterUserOptions which
// is the user facing configuration.
type ParquetWriterOptions struct {
	DefaultColumnCompression *pbparquet.Compression
}

// ParquetWriterUserOptions holds the configuration options for the Parquet writer.
type ParquetWriterUserOptions struct {
	DefaultColumnCompression string
}

func NewParquetWriterOptions(opts []ParquetWriterOption) (*ParquetWriterOptions, error) {
	userOptions := &ParquetWriterUserOptions{}
	for _, opt := range opts {
		opt.apply(userOptions)
	}

	options := &ParquetWriterOptions{}
	if userOptions.DefaultColumnCompression != "" {
		compression, found := pbparquet.Compression_value[strings.ToUpper(userOptions.DefaultColumnCompression)]
		if !found {
			return nil, fmt.Errorf("invalid compression type %q, accepted compression values are %v", userOptions.DefaultColumnCompression, slices.Map(maps.Keys(pbparquet.Compression_value), strings.ToLower))
		}

		options.DefaultColumnCompression = ptr(pbparquet.Compression(compression))
	}

	return options, nil
}

// Option is a function that configures a ParquetWriterUserOptions.
type ParquetWriterOption interface {
	apply(*ParquetWriterUserOptions)
}

type optionFunc func(*ParquetWriterUserOptions)

func (f optionFunc) apply(o *ParquetWriterUserOptions) {
	f(o)
}

// ParquetDefaultColumnCompression sets the default column compression for the Parquet writer.
func ParquetDefaultColumnCompression(compression string) ParquetWriterOption {
	return optionFunc(func(o *ParquetWriterUserOptions) {
		o.DefaultColumnCompression = compression
	})
}

func ptr[T any](v T) *T {
	return &v
}
