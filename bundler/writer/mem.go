package writer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"time"
)

type Mem struct {
	baseWriter

	activeFilename string
	buf            []byte
}

func NewMem(
	outputStore dstore.Store,
	fileType FileType,
	zlogger *zap.Logger,
) Writer {
	return &Mem{
		baseWriter: baseWriter{
			outputStore: outputStore,
			fileType:    fileType,
			zlogger:     zlogger,
		},
		buf: []byte{},
	}
}

func (m *Mem) StartBoundary(blockRange *bstream.Range) error {
	if m.activeFilename != "" {
		return fmt.Errorf("unable to start a file while one %q is open", m.activeFilename)
	}

	m.activeFilename = m.filename(blockRange)
	m.buf = []byte{}
	return nil
}

func (m *Mem) CloseBoundary(ctx context.Context) error {

	if m.activeFilename == "" {
		return fmt.Errorf("no active file")
	}
	m.zlogger.Info("closing file")

	t0 := time.Now()

	if err := m.outputStore.WriteObject(ctx, m.activeFilename, bytes.NewReader(m.buf)); err != nil {
		return fmt.Errorf("failed to write object: %w", err)
	}

	m.zlogger.Info("in memory buffered copied to output store",
		zap.String("output_path", m.activeFilename),
		zap.Duration("elapsed", time.Since(t0)),
	)

	m.activeFilename = ""
	return nil
}

func (m *Mem) Write(data []byte) error {
	m.buf = append(m.buf, data...)
	return nil
}
