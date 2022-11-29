package writer

import (
	"context"
	"github.com/streamingfast/bstream"
)

type Writer interface {
	StartBoundary(*bstream.Range) error
	CloseBoundary(ctx context.Context) error
	Upload(ctx context.Context) error
	Write(data []byte) error
	Type() FileType
}
