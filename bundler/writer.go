package bundler

import (
	"context"
	"fmt"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"io"
	"os"
	"time"
)

type DStoreIO struct {
	workingDir    string
	outputStore   dstore.Store
	retryAttempts uint64
	fileType      FileType
	activeFile    *ActiveFile
	fileStatus    chan *ActiveFile
	zlogger       *zap.Logger
}

type ActiveFile struct {
	writer          *io.PipeWriter
	fileWriter      io.Writer
	blockRange      *bstream.Range
	workingFilename string
	outputFilename  string
	err             error
}

func newDStoreIO(
	workingDir string,
	outputStore dstore.Store,
	fileType FileType,
	zlogger *zap.Logger,
) *DStoreIO {
	return &DStoreIO{
		workingDir:    workingDir,
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

	workingFilename := s.workingFilename(blockRange)
	fileWriter, err := os.Create(workingFilename)
	if err != nil {
		return "", fmt.Errorf("unable to create working file %q: %w", workingFilename, err)
	}

	pr, pw := io.Pipe()

	a := &ActiveFile{
		writer:          pw,
		blockRange:      blockRange,
		fileWriter:      fileWriter,
		workingFilename: workingFilename,
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

	var status *ActiveFile
	select {
	case status = <-s.fileStatus:
	case <-ctx.Done():
		return fmt.Errorf("context completed")
	}

	if status.err != nil {
		return fmt.Errorf("failed to write file %q: %w", status.workingFilename, status.err)
	}

	s.zlogger.Info("working file written successfully, copying to output store",
		zap.String("output_path", status.outputFilename),
		zap.String("working_path", status.workingFilename),
	)

	if err := s.outputStore.PushLocalFile(ctx, status.workingFilename, status.outputFilename); err != nil {
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

func (s *DStoreIO) launchWriter(file *ActiveFile, reader io.Reader) {
	t0 := time.Now()
	_, err := io.Copy(file.fileWriter, reader)
	if err != nil {
		s.zlogger.Warn("failed to upload file", zap.Error(err), zap.Duration("elapsed", time.Since(t0)))
	} else {
		s.zlogger.Info("uploaded", zap.String("workingFilename", file.workingFilename), zap.Duration("elapsed", time.Since(t0)))
	}
	file.err = err

	s.fileStatus <- file
}
