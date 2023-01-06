package encoder

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pbsinkfiles "github.com/streamingfast/substreams-sink-files/pb/substreams/sink/files/v1"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type LinesEncoder struct {
}

func NewLineEncoder() *LinesEncoder {
	return &LinesEncoder{}
}

func (l *LinesEncoder) EncodeTo(output *pbsubstreams.ModuleOutput, writer writer.Writer) error {
	// FIXME: Improve by using a customized probably decoder, maybe Vitess, or we could
	// even create our own which should be quiter simpler and could even reduce allocations
	lines := &pbsinkfiles.Lines{}
	if err := proto.Unmarshal(output.GetMapOutput().Value, lines); err != nil {
		return fmt.Errorf("failed to unmarhsall lines: %w", err)
	}

	for _, line := range lines.Lines {
		writer.Write(unsafeGetBytes(line))
		writer.Write([]byte("\n"))
	}

	return nil
}

// unsafeGetBytes get the `[]byte` value out of a string without an allocation that `[]byte(s)` does.
//
// See https://stackoverflow.com/a/68195226/697930 and the post in general for background
func unsafeGetBytes(s string) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)), len(s))
}
