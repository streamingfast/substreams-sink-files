package writer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

var _ Writer = (*BufferedIO)(nil)
var _ TimestampAware = (*BufferedIO)(nil)
var _ BoundaryAdjustable = (*BufferedIO)(nil)

type BufferedIO struct {
	baseWriter

	bufferMazSize uint64
	workingDir    string
	activeFile    *bufferedActiveFile

	// Date partitioning fields
	currentTimestamp time.Time
	dateFormat       string
	datePartitioning bool
}

func NewBufferedIO(
	bufferMaxSize uint64,
	workingDir string,
	fileType FileType,
	zlogger *zap.Logger,
) *BufferedIO {
	if bufferMaxSize == 0 {
		bufferMaxSize = DefaultBufSize
	}

	return &BufferedIO{
		bufferMazSize: bufferMaxSize,
		baseWriter:    newBaseWriter(fileType, zlogger),
		workingDir:    workingDir,
	}
}

func (s *BufferedIO) workingFilename(blockRange *bstream.Range) string {
	return fmt.Sprintf("%010d-%010d.tmp.%s", blockRange.StartBlock(), (*blockRange.EndBlock()), s.fileType)
}

func (s *BufferedIO) StartBoundary(blockRange *bstream.Range) error {
	if s.activeFile != nil {
		return fmt.Errorf("unable to start a file while one (backed by %q) is already open", s.activeFile.Path())
	}

	lazyFile := LazyOpen(filepath.Join(s.workingDir, s.workingFilename(blockRange)))

	a := &bufferedActiveFile{
		lazyFile:       lazyFile,
		writer:         NewIntelligentWriterSize(lazyFile, int(s.bufferMazSize)),
		blockRange:     blockRange,
		outputFilename: "", // Will be generated when boundary closes
	}

	s.activeFile = a
	return nil
}

func (s *BufferedIO) CloseBoundary(ctx context.Context) (Uploadeable, error) {
	defer func() {
		s.activeFile = nil
	}()

	if s.activeFile == nil {
		return nil, fmt.Errorf("no active file")
	}

	// Generate output filename now that we have processed blocks and potentially have a timestamp
	outputFilename := s.generateOutputFilename(s.activeFile.blockRange)

	if s.activeFile.writer.AllDataFitInMemory() {
		s.zlogger.Info("all data from range is in memory, no need to flush")
		return &dataFile{
			reader:         bytes.NewReader(s.activeFile.writer.MemoryData()),
			outputFilename: outputFilename,
		}, nil
	}

	s.zlogger.Info("flushing buffered writer")
	if err := s.activeFile.writer.Flush(); err != nil {
		return nil, fmt.Errorf("flushing buffered active writer: %w", err)
	}

	if err := s.activeFile.lazyFile.Close(); err != nil {
		return nil, fmt.Errorf("closing file: %w", err)
	}

	workingPath := s.activeFile.Path()
	return &localFile{
		localFilePath:  workingPath,
		outputFilename: outputFilename,
	}, nil
}

func (s *BufferedIO) Write(data []byte) (n int, err error) {
	if s.activeFile == nil {
		return 0, fmt.Errorf("failed to write to active file")
	}

	return s.activeFile.writer.Write(data)
}

var _ io.WriteCloser = (*LazyFile)(nil)

// LazyFile only creates and writes to file if `Write` is called at least one.
//
// **Important** Not safe for concurrent access, you need to gate yourself if
// you need that.
type LazyFile struct {
	*os.File

	path string
}

func LazyOpen(path string) *LazyFile {
	return &LazyFile{
		File: nil,
		path: path,
	}
}

func (f *LazyFile) Path() string {
	return f.path
}

func (f *LazyFile) Write(p []byte) (n int, err error) {
	if f.File == nil {
		if err := os.MkdirAll(filepath.Dir(f.path), os.ModePerm); err != nil {
			return 0, fmt.Errorf("mkdir dirs: %w", err)
		}

		file, err := os.Create(f.path)
		if err != nil {
			return 0, fmt.Errorf("open file: %w", err)
		}

		f.File = file
	}

	return f.File.Write(p)
}

func (f *LazyFile) Close() error {
	if f.File != nil {
		return f.File.Close()
	}

	return nil
}

// memoryBufferedWriter has two goals. First, it knows if wrapped writer received
// data or not. When no data has been received, it's possible to use this writer
// to retrieve the content being written to it to a memory buffer.
//
// In expected case where we are controlled by an IntelligentWriter, no memory allocation
// will happen as the IntelligentWriter uses a `bufio.Writer` and we will receive it's internal
// buffer.
type memoryBufferedWriter struct {
	io.Writer

	MemoryBuffer       []byte
	NextWritesToMemory bool
	WrittenToWrapped   bool
}

func newMemoryBufferedWriter(w io.Writer) *memoryBufferedWriter {
	return &memoryBufferedWriter{Writer: w}
}

func (f *memoryBufferedWriter) Write(p []byte) (n int, err error) {
	if f.NextWritesToMemory {
		if f.MemoryBuffer == nil {
			f.MemoryBuffer = p
			return len(p), nil
		}

		f.MemoryBuffer = append(f.MemoryBuffer, p...)
		return len(p), nil
	}

	f.WrittenToWrapped = true
	return f.Writer.Write(p)
}

func (f *memoryBufferedWriter) Close() error {
	if v, ok := f.Writer.(io.Closer); ok {
		return v.Close()
	}

	return nil
}

// NewIntelligentWriterSize is intelligent because it tracks if data was ever
// written to the temporary file or not. If everything entered the buffer fit in memory,
// we can nicely optimize away the full I/O operation and avoid all the cost of it.
//
// Operators should specify a buffer as large as they are willing to pay for RAM (
// leaving a buffer for "normal" operations of the process).
func NewIntelligentWriterSize(w io.Writer, size int) *IntelligentWriter {
	underlyingWriter := newMemoryBufferedWriter(w)

	return &IntelligentWriter{Writer: bufio.NewWriterSize(underlyingWriter, size), underlyingWriter: underlyingWriter}
}

// NewIntelligentWriter returns a new IntelligentWriter whose buffer has the default size.
// If the argument io.Writer is already a bufio.Writer with large enough buffer size,
// it returns the underlying Writer.
func NewIntelligentWriter(w io.Writer) *IntelligentWriter {
	return NewIntelligentWriterSize(w, DefaultBufSize)
}

func (w *IntelligentWriter) AllDataFitInMemory() bool {
	return !w.underlyingWriter.WrittenToWrapped
}

func (w *IntelligentWriter) MemoryData() []byte {
	if !w.AllDataFitInMemory() {
		panic(fmt.Errorf("it's invalid to call MemoryData without checking if all data is held in memory, check AllDataFitInMemory prior calling this method"))
	}

	// It's a bit convoluted here because `bufio.Writer` does not give
	// direct access to underlying buffer. So we need to make some weird
	// steps to access the buffered data without an allocation.
	//
	// The trick here is to use `Flush` primitive, we know the data is fully
	// in memory at this point. Implementation of `Flush` on `bufio.Writer` calls
	// the wrapped writer directly with the `writ.Write(b.buf[0:n])` where b is
	// `bufio.Writer` private inaccessible buffer and `n` is amount of data
	// buffered. You can see in the invocation of `Write` that it passes his private
	// buffer straight. This means the writer's `Write` method will receive this
	// "private" buffer.
	//
	// Now, we need to hijack `Write` somehow. For this, we have created
	// `memoryBufferedWriter` struct. This special writer has a mode that when activated,
	// subsequent calls to `Write` records the received buffer in memory.
	w.underlyingWriter.NextWritesToMemory = true

	// Final step is to call `Flush` which triggers ours specialized
	// `memoryBufferedWriter.Write(b.buf[0:n])` and which will reach which itself
	// keep a reference to `b.buf[0:n]`, giving us access indirectly to underlying
	// buffer.
	if err := w.Writer.Flush(); err != nil {
		panic(fmt.Errorf("this should have been infallible because we write directly received 'b.buf[0:n]', there is a flaw in our logic: %w", err))
	}

	return w.underlyingWriter.MemoryBuffer
}

type IntelligentWriter struct {
	*bufio.Writer

	underlyingWriter *memoryBufferedWriter
}

const (
	DefaultBufSize = 16 * 1024 * 1024 // 16 MiB
)

type bufferedActiveFile struct {
	lazyFile       *LazyFile
	writer         *IntelligentWriter
	blockRange     *bstream.Range
	outputFilename string
}

func (f *bufferedActiveFile) Path() string {
	return f.lazyFile.path
}

// SetCurrentTimestamp implements TimestampAware interface
func (s *BufferedIO) SetCurrentTimestamp(t time.Time) {
	s.currentTimestamp = t
}

// SetDatePartitioning configures date-based partitioning
func (s *BufferedIO) SetDatePartitioning(enabled bool, format string) {
	s.datePartitioning = enabled
	s.dateFormat = format
}

func (s *BufferedIO) generateOutputFilename(blockRange *bstream.Range) string {
	baseFilename := fmt.Sprintf("%010d-%010d.%s", blockRange.StartBlock(), (*blockRange.EndBlock()), s.fileType)

	if s.datePartitioning && !s.currentTimestamp.IsZero() {
		dateDir := s.currentTimestamp.UTC().Format(s.dateFormat)
		return path.Join(dateDir, baseFilename)
	}

	return baseFilename
}

// AdjustBoundary implements BoundaryAdjustable interface
func (s *BufferedIO) AdjustBoundary(newRange *bstream.Range) {
	if s.activeFile != nil {
		s.activeFile.blockRange = newRange
	}
}
