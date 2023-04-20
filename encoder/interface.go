package encoder

import (
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

type Encoder interface {
	EncodeTo(output *pbsubstreamsrpc.MapModuleOutput, writer writer.Writer) error
}
