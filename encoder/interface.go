package encoder

import pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"

type Encoder interface {
	Encode(output *pbsubstreams.ModuleOutput) ([]byte, error)
}
