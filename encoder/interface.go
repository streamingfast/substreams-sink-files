package encoder

import (
	"github.com/streamingfast/substreams-sink-files/v2/bundler/writer"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

type Encoder interface {
	EncodeTo(output *pbsubstreamsrpc.MapModuleOutput, writer writer.Writer) error
}

type EncoderFunc func(output *pbsubstreamsrpc.MapModuleOutput, writer writer.Writer) error

func (f EncoderFunc) EncodeTo(output *pbsubstreamsrpc.MapModuleOutput, writer writer.Writer) error {
	return f(output, writer)
}
