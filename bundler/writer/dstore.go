package writer

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type activeFile struct {
	writer          *io.PipeWriter
	fileWriter      io.Writer
	blockRange      *bstream.Range
	workingFilename string
	outputFilename  string
	err             error
}

type DStoreIO struct {
	baseWriter

	workingDir string
	activeFile *activeFile
	fileStatus chan bool
}

func NewDStoreIO(
	workingDir string,
	outputStore dstore.Store,
	fileType FileType,
	zlogger *zap.Logger,
) Writer {
	return &DStoreIO{
		baseWriter: newBaseWriter(outputStore, fileType, zlogger),
		workingDir: workingDir,
		fileStatus: make(chan bool, 1),
	}
}

func (s *DStoreIO) workingFilename(blockRange *bstream.Range) string {
	return fmt.Sprintf("%010d-%010d.tmp.%s", blockRange.StartBlock(), (*blockRange.EndBlock()), s.fileType)
}

func (s *DStoreIO) StartBoundary(blockRange *bstream.Range) error {
	if s.activeFile != nil {
		return fmt.Errorf("unable to start a file while one %q is open", s.activeFile.workingFilename)
	}

	if err := os.MkdirAll(s.workingDir, os.ModePerm); err != nil {
		return fmt.Errorf("unable to create working directories: %w", err)
	}

	workingFilename := filepath.Join(s.workingDir, s.workingFilename(blockRange))
	fileWriter, err := os.Create(workingFilename)
	if err != nil {
		return fmt.Errorf("unable to create working file %q: %w", workingFilename, err)
	}

	pr, pw := io.Pipe()

	a := &activeFile{
		writer:          pw,
		blockRange:      blockRange,
		fileWriter:      fileWriter,
		workingFilename: workingFilename,
		outputFilename:  s.filename(blockRange),
	}

	go s.launchWriter(a, pr)
	s.activeFile = a

	return nil
}

func (s *DStoreIO) CloseBoundary(ctx context.Context) error {
	if s.activeFile == nil {
		return fmt.Errorf("no active file")
	}
	s.zlogger.Info("closing file")
	if err := s.activeFile.writer.Close(); err != nil {
		return fmt.Errorf("close activeWriter: %w", err)
	}

	select {
	case _ = <-s.fileStatus:
	case <-ctx.Done():
		return fmt.Errorf("context completed")
	}

	if s.activeFile.err != nil {
		return fmt.Errorf("failed to write file %q: %w", s.activeFile.workingFilename, s.activeFile.err)
	}
	return nil
}

func (s *DStoreIO) Upload(ctx context.Context) error {
	if err := s.outputStore.PushLocalFile(ctx, s.activeFile.workingFilename, s.activeFile.outputFilename); err != nil {
		return fmt.Errorf("copy file from worling output: %w", err)
	}
	s.zlogger.Info("working file successfully copied to output store",
		zap.String("output_path", s.activeFile.outputFilename),
		zap.String("working_path", s.activeFile.workingFilename),
	)
	s.activeFile = nil
	return nil
}

func (s *DStoreIO) Write(data []byte) error {
	if s.activeFile == nil {
		return fmt.Errorf("failed to write to active file")
	}
	if _, err := s.activeFile.writer.Write(data); err != nil {
		return err
	}
	return nil
}

func (s *DStoreIO) launchWriter(file *activeFile, reader io.Reader) {
	t0 := time.Now()
	_, err := io.Copy(file.fileWriter, reader)
	if err != nil {
		s.zlogger.Warn("failed to upload file", zap.Error(err), zap.Duration("elapsed", time.Since(t0)))
	} else {
		s.zlogger.Info("uploaded", zap.String("workingFilename", file.workingFilename), zap.Duration("elapsed", time.Since(t0)))
	}
	file.err = err
	s.activeFile = file

	s.fileStatus <- true
}
