package sink

import (
	"github.com/streamingfast/bstream"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type BlockScopeDataHandler = func(cursor *Cursor, data *pbsubstreams.BlockScopedData) error

type Cursor struct {
	Cursor string
	Block  bstream.BlockRef
}

func newCursor(cursor string, block bstream.BlockRef) *Cursor {
	return &Cursor{cursor, block}
}
func newBlankCursor() *Cursor {
	return newCursor("", bstream.BlockRefEmpty)
}
