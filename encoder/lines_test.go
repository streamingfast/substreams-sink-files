package encoder

import (
	"context"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	pbfilesink "github.com/streamingfast/substreams-sink-files/pb"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/stretchr/testify/assert"
	"github.com/test-go/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestLinesEncoder_EncodeTo(t *testing.T) {
	tests := []struct {
		name      string
		lines     *pbfilesink.Lines
		expected  []byte
		assertion assert.ErrorAssertionFunc
	}{
		{
			"no line",
			&pbfilesink.Lines{Lines: nil},
			nil,
			assert.NoError,
		},
		{
			"one line",
			&pbfilesink.Lines{Lines: [][]byte{[]byte(`{"a":1}`)}},
			[]byte(`{"a":1}`),
			assert.NoError,
		},
		{
			"three line",
			&pbfilesink.Lines{Lines: [][]byte{
				[]byte(`{"a":1}`),
				[]byte(`{"b":2}`),
				[]byte(`{"c":3}`),
			}},
			[]byte(`{"a":1}` + "\n" + `{"b":2}` + "\n" + `{"c":3}`),
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoder := &LinesEncoder{}
			writer := &testWriter{}

			lines, err := anypb.New(tt.lines)
			require.NoError(t, err)

			module := &pbsubstreams.ModuleOutput{
				Data: &pbsubstreams.ModuleOutput_MapOutput{
					MapOutput: lines,
				},
			}

			tt.assertion(t, encoder.EncodeTo(module, writer))
			assert.Equal(t, tt.expected, writer.written)
		})
	}
}

var _ writer.Writer = (*testWriter)(nil)

type testWriter struct {
	written []byte
}

func (*testWriter) CloseBoundary(ctx context.Context) error {
	panic("unimplemented")
}

// StartBoundary implements writer.Writer
func (*testWriter) StartBoundary(*bstream.Range) error {
	panic("unimplemented")
}

// Type implements writer.Writer
func (*testWriter) Type() writer.FileType {
	return writer.FileTypeJSONL
}

// Upload implements writer.Writer
func (*testWriter) Upload(ctx context.Context) error {
	panic("unimplemented")
}

// Write implements writer.Writer
func (w *testWriter) Write(data []byte) (n int, err error) {
	w.written = append(w.written, data...)
	return len(data), nil
}