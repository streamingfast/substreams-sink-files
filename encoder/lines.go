package encoder

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pnfilesink "github.com/streamingfast/substreams-sink-files/pb"
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
	lines := &pnfilesink.Lines{}
	if err := proto.Unmarshal(output.GetMapOutput().Value, lines); err != nil {
		return fmt.Errorf("failed to unmarhsall lines: %w", err)
	}

	lastIndex := len(lines.Lines) - 1

	for idx, line := range lines.Lines {
		writer.Write(line)

		if idx < lastIndex {
			writer.Write([]byte("\n"))
		}
	}

	return nil
}
