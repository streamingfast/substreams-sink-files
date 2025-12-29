package encoder

import (
	"fmt"
	"unsafe"

	"github.com/streamingfast/substreams-sink-files/v2/bundler/writer"
	pbsinkfiles "github.com/streamingfast/substreams-sink-files/v2/pb/sf/substreams/sink/files/v1"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"google.golang.org/protobuf/proto"
)

type LinesEncoder struct {
}

func NewLineEncoder() *LinesEncoder {
	return &LinesEncoder{}
}

func (l *LinesEncoder) EncodeTo(output *pbsubstreamsrpc.MapModuleOutput, writer writer.Writer) error {
	// FIXME: Improve by using a customized probably decoder, maybe Vitess, or we could
	// even create our own which should be quite simpler and could even reduce allocations
	lines := &pbsinkfiles.Lines{}
	if err := proto.Unmarshal(output.GetMapOutput().Value, lines); err != nil {
		return fmt.Errorf("failed to unmarshal lines: %w", err)
	}

	for _, line := range lines.Lines {
		writer.Write(unsafeGetBytes(line))
		writer.Write([]byte("\n"))
	}

	return nil
}

// unsafeGetBytes get the `[]byte` value out of a string without an allocation that `[]byte(s)` does.
//
// See https://stackoverflow.com/a/74658905/697930 and the post in general for background
func unsafeGetBytes(s string) []byte {
	// The unsafe.StringData don't accepts empty strings, we handle that case before
	if len(s) == 0 {
		return nil
	}

	stringDataPtr := unsafe.StringData(s)
	return unsafe.Slice(stringDataPtr, len(s))
}
