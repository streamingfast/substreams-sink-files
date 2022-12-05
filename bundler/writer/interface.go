package writer

import (
	"context"
	"io"

	"github.com/streamingfast/bstream"
)

type Writer interface {
	io.Writer

	StartBoundary(*bstream.Range) error
	CloseBoundary(ctx context.Context) error
	Upload(ctx context.Context) error
	Type() FileType
}
