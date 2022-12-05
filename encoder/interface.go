package encoder

import (
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type Encoder interface {
	EncodeTo(output *pbsubstreams.ModuleOutput, writer writer.Writer) error
}
