package encoder

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	pnfilesink "github.com/streamingfast/substreams-sink-files/pb"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type LinesEncoder struct {
}

func NewLineEncoder() *LinesEncoder {
	return &LinesEncoder{}
}

func (l *LinesEncoder) Encode(output *pbsubstreams.ModuleOutput) ([]byte, error) {
	lines := &pnfilesink.Lines{}
	if err := proto.Unmarshal(output.GetMapOutput().Value, lines); err != nil {
		return nil, fmt.Errorf("failed to unmarhsall lines: %w", err)
	}

	lastIndex := len(lines.Lines) - 1

	var buf []byte
	for idx, line := range lines.Lines {
		buf = append(buf, line...)
		if idx < lastIndex {
			buf = append(buf, byte('\n'))
		}
	}
	return buf, nil
}
