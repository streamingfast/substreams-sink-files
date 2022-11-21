package bundler

import (
	"context"
	"fmt"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"io"
	"time"
)

type DStoreIO struct {
	workingStore dstore.Store
	outputStore  dstore.Store

	retryAttempts uint64
	fileType      FileType

	activeFile *ActiveFile

	fileStatus chan *ActiveFile
	zlogger    *zap.Logger
}

type ActiveFile struct {
	writer          *io.PipeWriter
	blockRange      *bstream.Range
	workingFilename string
	outputFilename  string
	err             error
}

func newDStoreIO(
	workingStore dstore.Store,
	outputStore dstore.Store,
	fileType FileType,
	zlogger *zap.Logger,
) *DStoreIO {
	return &DStoreIO{
		workingStore:  workingStore,
		outputStore:   outputStore,
		fileType:      fileType,
		retryAttempts: 3,
		fileStatus:    make(chan *ActiveFile, 1),
		zlogger:       zlogger,
	}
}

func (s *DStoreIO) workingFilename(blockRange *bstream.Range) string {
	return fmt.Sprintf("%010d-%010d.tmp.%s", blockRange.StartBlock(), (*blockRange.EndBlock()), s.fileType)
}

func (s *DStoreIO) finalFilename(blockRange *bstream.Range) string {
	return fmt.Sprintf("%010d-%010d.%s", blockRange.StartBlock(), (*blockRange.EndBlock()), s.fileType)
}

func (s *DStoreIO) StartFile(blockRange *bstream.Range) (string, error) {
	if s.activeFile != nil {
		return "", fmt.Errorf("unable to start a file whilte one %q  is open", s.activeFile.workingFilename)
	}

	pr, pw := io.Pipe()

	a := &ActiveFile{
		writer:          pw,
		blockRange:      blockRange,
		workingFilename: s.workingFilename(blockRange),
		outputFilename:  s.finalFilename(blockRange),
	}

	go s.launchWriter(a, pr)
	s.activeFile = a

	return a.workingFilename, nil
}

func (s *DStoreIO) CloseFile(ctx context.Context) error {
	if s.activeFile == nil {
		return fmt.Errorf("no active file")
	}
	s.zlogger.Info("closing file")
	if err := s.activeFile.writer.Close(); err != nil {
		return fmt.Errorf("close activeWriter: %w", err)
	}

	status := <-s.fileStatus

	if status.err != nil {
		return fmt.Errorf("failed to write file %q: %w", status.workingFilename, status.err)
	}

	workingPath := s.workingStore.ObjectPath(status.workingFilename)
	
	s.zlogger.Info("working file written successfully, copying to output store",
		zap.String("output_path", status.outputFilename),
		zap.String("working_path", workingPath),
	)

	if err := s.outputStore.PushLocalFile(ctx, workingPath, status.outputFilename); err != nil {
		return fmt.Errorf("copy file from worling output: %w", err)
	}

	s.activeFile = nil

	return nil
}

func (s *DStoreIO) Write(data []byte) (int, error) {
	if s.activeFile == nil {
		return 0, fmt.Errorf("failed to write to active file")
	}
	return s.activeFile.writer.Write(data)
}

type FileWrittenStatus struct {
	filename string
	err      error
}

func (s *DStoreIO) launchWriter(file *ActiveFile, reader io.Reader) {
	t0 := time.Now()
	err := derr.Retry(s.retryAttempts, func(ctx context.Context) error {
		return s.workingStore.WriteObject(ctx, file.workingFilename, reader)
	})
	if err != nil {
		s.zlogger.Warn("failed to upload file", zap.Error(err), zap.Duration("elapsed", time.Since(t0)))
	} else {
		s.zlogger.Info("uploaded", zap.String("workingFilename", file.workingFilename), zap.Duration("elapsed", time.Since(t0)))
	}
	file.err = err

	s.fileStatus <- file
}
