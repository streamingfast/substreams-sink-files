package writer

import (
	"context"
	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

type Noop struct {
	zlogger  *zap.Logger
	fileType FileType
}

func (n *Noop) StartBoundary(b *bstream.Range) error {
	n.zlogger.Info("noop starting boundary", zap.Stringer("boundary", b))
	return nil
}

func (n *Noop) CloseBoundary(ctx context.Context) error {
	n.zlogger.Info("noop closing boundary")
	return nil
}

func (n *Noop) Upload(ctx context.Context) error {
	n.zlogger.Info("noop uploading")
	return nil
}

func (n *Noop) Write(data []byte) error {
	return nil
}

func (n *Noop) Type() FileType {
	return n.fileType
}

func NewNoop(
	fileType FileType,
	zlogger *zap.Logger,
) Writer {
	return &Noop{
		fileType: fileType,
		zlogger:  zlogger,
	}
}
