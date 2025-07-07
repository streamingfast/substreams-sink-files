package writer

import (
	"context"
	"io"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
)

type Writer interface {
	io.Writer

	StartBoundary(*bstream.Range) error
	CloseBoundary(ctx context.Context) (Uploadeable, error)
	Type() FileType
}

// TimestampAware is an optional interface that writers can implement
// to receive block timestamps for date-based partitioning
type TimestampAware interface {
	SetCurrentTimestamp(time.Time)
}

// BoundaryAdjustable is an optional interface that writers can implement
// to support updating the boundary range (e.g., for date-based early closure)
type BoundaryAdjustable interface {
	AdjustBoundary(*bstream.Range)
}

type Uploadeable interface {
	Upload(ctx context.Context, store dstore.Store) (string, error)
}

type UploadeableFunc func(ctx context.Context, store dstore.Store) (string, error)

func (f UploadeableFunc) Upload(ctx context.Context, store dstore.Store) (string, error) {
	return f(ctx, store)
}
