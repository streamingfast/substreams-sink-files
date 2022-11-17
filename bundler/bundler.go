package bundler

import (
	"bytes"
	"context"
	"fmt"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type BundlerType string

const (
	BundlerTypeJSON BundlerType = "json"
)

type Bundler struct {
	size        uint64
	bundlerType BundlerType
	encoder     Encoder
	store       dstore.Store

	objects []proto.Message

	startBlockNum uint64

	zlogger *zap.Logger
}

func New(store dstore.Store, size uint64, startBlock uint64, bundlerType BundlerType, zlogger *zap.Logger) (*Bundler, error) {
	b := &Bundler{
		store:         store,
		size:          size,
		bundlerType:   bundlerType,
		startBlockNum: startBlock,
		objects:       []proto.Message{},
		zlogger:       zlogger,
	}

	switch bundlerType {
	case BundlerTypeJSON:
		b.encoder = JSONEncode
	default:
		return nil, fmt.Errorf("invalid bundler type %q", bundlerType)
	}
	return b, nil
}

func (b *Bundler) ForceFlush(ctx context.Context) error {
	boundary := bstream.NewRangeExcludingEnd(b.startBlockNum, b.startBlockNum+b.size)
	if err := b.save(ctx, boundary); err != nil {
		return fmt.Errorf("save %q: %w", boundary.String(), err)
	}

	return nil
}

func (b *Bundler) Flush(ctx context.Context, blockNum uint64) (bool, error) {
	boundaries := b.boundariesToSave(blockNum)
	if len(boundaries) == 0 {
		return false, nil
	}
	for _, boundary := range boundaries {
		if err := b.save(ctx, boundary); err != nil {
			return false, fmt.Errorf("save %q: %w", boundary.String(), err)
		}
	}
	return true, nil
}

func (b *Bundler) Write(entities []proto.Message) {
	for _, entity := range entities {
		b.objects = append(b.objects, entity)
	}
}

func (b *Bundler) save(ctx context.Context, boundary *bstream.Range) error {
	filename := b.filename(boundary)

	b.zlogger.Debug("storing boundary",
		zap.String("filename", filename),
	)

	content, err := b.encoder(b.objects)
	if err != nil {
		return fmt.Errorf("encode objets: %w", err)
	}

	if err := derr.RetryContext(ctx, 3, func(ctx context.Context) error {
		return b.store.WriteObject(ctx, filename, bytes.NewReader(content))
	}); err != nil {
		return fmt.Errorf("write object: %w", err)
	}

	b.objects = []proto.Message{}
	b.startBlockNum = *boundary.EndBlock()
	return nil
}

func (b *Bundler) filename(blockRange *bstream.Range) string {
	return fmt.Sprintf("%010d-%010d.%s", blockRange.StartBlock(), (*blockRange.EndBlock()), b.bundlerType)
}

func (b *Bundler) boundariesToSave(blockNum uint64) (out []*bstream.Range) {
	rangeStartBlock := b.startBlockNum
	for blockNum >= rangeStartBlock+b.size {
		out = append(out, bstream.NewRangeExcludingEnd(rangeStartBlock, rangeStartBlock+b.size))
		rangeStartBlock = rangeStartBlock + b.size
	}
	return out
}
