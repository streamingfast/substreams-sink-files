package writer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type Mem struct {
	baseWriter

	activeFilename  string
	activeFileStats *stats
	buf             []byte
}

func NewMem(
	outputStore dstore.Store,
	fileType FileType,
	zlogger *zap.Logger,
) Writer {
	return &Mem{
		baseWriter: newBaseWriter(outputStore, fileType, zlogger),
		buf:        []byte{},
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

func (m *Mem) Upload(ctx context.Context) error {
	if err := m.outputStore.WriteObject(ctx, m.activeFilename, bytes.NewReader(m.buf)); err != nil {
		return fmt.Errorf("failed to write object: %w", err)
	}
	m.zlogger.Info("uploading in memory data", zap.String("filename", m.activeFilename))
	m.activeFilename = ""
	return nil
}

func (m *Mem) CloseBoundary(_ context.Context) error {
	if m.activeFilename == "" {
		return fmt.Errorf("no active file")
	}
	m.zlogger.Info("closing file")
	return nil
}

func (m *Mem) Write(data []byte) error {
	m.buf = append(m.buf, data...)
	return nil
}
