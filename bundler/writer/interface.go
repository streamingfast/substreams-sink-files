package writer

import (
	"context"
	"io"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
)

type Writer interface {
	io.Writer

	StartBoundary(*bstream.Range) error
	CloseBoundary(ctx context.Context) (Uploadeable, error)
	Type() FileType
}

type Uploadeable interface {
	Upload(ctx context.Context, store dstore.Store) (string, error)
}

type UploadeableFunc func(ctx context.Context, store dstore.Store) (string, error)

func (f UploadeableFunc) Upload(ctx context.Context, store dstore.Store) (string, error) {
	return f(ctx, store)
}
