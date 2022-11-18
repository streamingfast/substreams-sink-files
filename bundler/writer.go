package bundler

import (
	"context"
	"fmt"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"io"
	"time"
)

type DStoreIO struct {
	outputStore   dstore.Store
	retryAttempts uint64
	fileType      FileType

	activeFileName string
	activeWriter   *io.PipeWriter

	fileStatus chan *FileWrittenStatus
	zlogger    *zap.Logger
}

func newDStoreIO(
	outputStore dstore.Store,
	retryAttempts uint64,
	zlogger *zap.Logger,
) *DStoreIO {
	return &DStoreIO{
		outputStore:   outputStore,
		retryAttempts: retryAttempts,
		fileStatus:    make(chan *FileWrittenStatus, 1),
		zlogger:       zlogger,
	}
}

func (s *DStoreIO) HasActiveFile() {

}
func (s *DStoreIO) StartFile(filename string) error {
	if s.activeWriter != nil {
		return fmt.Errorf("unable to start a file whilte one %q  is open", s.activeFileName)
	}
	pr, pw := io.Pipe()
	go s.launchWriter(filename, pr)
	s.activeWriter = pw
	s.activeFileName = filename
	return nil
}

func (s *DStoreIO) CloseFile() error {
	s.zlogger.Info("closing file")
	if err := s.activeWriter.Close(); err != nil {
		return fmt.Errorf("close activeWriter: %w", err)
	}

	s.activeFileName = ""
	s.activeWriter = nil

	status := <-s.fileStatus

	if status.err != nil {
		return fmt.Errorf("failed to write file %q: %w", status.filename, status.err)
	}

	s.zlogger.Info("file written successfully",
		zap.String("filename", status.filename),
	)
	return nil
}

type FileWrittenStatus struct {
	filename string
	err      error
}

func (s *DStoreIO) launchWriter(filename string, reader io.Reader) {
	t0 := time.Now()
	err := derr.Retry(s.retryAttempts, func(ctx context.Context) error {
		return s.outputStore.WriteObject(ctx, filename, reader)
	})
	if err != nil {
		s.zlogger.Warn("failed to upload file", zap.Error(err), zap.Duration("elapsed", time.Since(t0)))
	} else {
		s.zlogger.Info("uploaded", zap.String("filename", filename), zap.Duration("elapsed", time.Since(t0)))
	}

	s.fileStatus <- &FileWrittenStatus{
		filename: filename,
		err:      err,
	}
}
