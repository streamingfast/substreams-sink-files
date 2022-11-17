package substreams_file_sink

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/substreams-sink-files/sink"
)

type BundlerType string

const (
	BundlerTypeJSON BundlerType = "json"
	BundlerTypeCSV  BundlerType = "csv"
)

type BundleItem struct {
	block bstream.BlockRef
	obj   proto.Message
}

type Bundler struct {
	size          uint64
	bundlerType   BundlerType
	store         dstore.Store
	objects       []*BundleItem
	startBlockNum uint64
	currentBlock  bstream.BlockRef
}

func newBundler(store dstore.Store, size uint64, startBlock uint64, bundlerType BundlerType) *Bundler {
	return &Bundler{
		store:         store,
		size:          size,
		bundlerType:   bundlerType,
		startBlockNum: startBlock,
		objects:       []*BundleItem{},
	}
}

func (b *Bundler) Roll() {

}

func (b *Bundler) Write(cursor *sink.Cursor, obj proto.Message) {
	b.objects = append(b.objects, &BundleItem{
		block: cursor.Block,
		obj:   obj,
	})
	b.currentBlock = cursor.Block
}

func (b *Bundler) save() {

}

func (b *Bundler) filename(startBlock, stopBlock uint64) string {
	return fmt.Sprintf("%010d-%010d.%s", startBlock, stopBlock, b.bundlerType)
}

func (b *Bundler) shouldRoll() {

}
