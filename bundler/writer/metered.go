package writer

import (
	"context"
	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

type Metered struct {
	w       Writer
	stats   *stats
	zlogger *zap.Logger
}

func NewMeteredWriter(writer Writer, zlogger *zap.Logger) *Metered {
	return &Metered{
		w:       writer,
		stats:   newStats(),
		zlogger: zlogger,
	}
}

func (m *Metered) Write(data []byte) error {
	return m.w.Write(data)
}

func (m *Metered) Type() FileType {
	return m.w.Type()
}

func (m *Metered) StartBoundary(b *bstream.Range) error {
	if err := m.w.StartBoundary(b); err != nil {
		return err
	}
	m.stats.startCollecting()
	return nil
}

func (m *Metered) CloseBoundary(ctx context.Context) error {
	if err := m.w.CloseBoundary(ctx); err != nil {
		return err
	}
	m.stats.stopCollecting()

	m.stats.startUploading()
	if err := m.w.Upload(ctx); err != nil {
		return err
	}
	m.stats.stopUploading()

	m.zlogger.Info("bundler stats", zap.Object("stats", m.stats))
	return nil
}
