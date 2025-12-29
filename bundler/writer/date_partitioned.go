package writer

import (
	"context"
	"path"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

var _ Writer = (*DatePartitionedWriter)(nil)
var _ TimestampAware = (*DatePartitionedWriter)(nil)

// DatePartitionedWriter wraps another writer and adds date-based partitioning
type DatePartitionedWriter struct {
	underlying  Writer
	enabled     bool
	dateFormat  string
	currentTime time.Time
	logger      *zap.Logger
}

// NewDatePartitioned creates a new DatePartitionedWriter that wraps the underlying writer
func NewDatePartitioned(underlying Writer, enabled bool, dateFormat string, logger *zap.Logger) *DatePartitionedWriter {
	return &DatePartitionedWriter{
		underlying: underlying,
		enabled:    enabled,
		dateFormat: dateFormat,
		logger:     logger,
	}
}

// SetCurrentTimestamp implements TimestampAware interface
func (d *DatePartitionedWriter) SetCurrentTimestamp(t time.Time) {
	d.currentTime = t
}

// StartBoundary implements Writer interface
func (d *DatePartitionedWriter) StartBoundary(blockRange *bstream.Range) error {
	return d.underlying.StartBoundary(blockRange)
}

// CloseBoundary implements Writer interface with date-aware path modification
func (d *DatePartitionedWriter) CloseBoundary(ctx context.Context) (Uploadeable, error) {
	uploadable, err := d.underlying.CloseBoundary(ctx)
	if err != nil {
		return nil, err
	}

	if !d.enabled {
		return uploadable, nil
	}

	// Modify the uploadable to have date-based paths
	return d.wrapUploadableWithDatePath(uploadable), nil
}

// Write implements Writer interface
func (d *DatePartitionedWriter) Write(p []byte) (n int, err error) {
	return d.underlying.Write(p)
}

// Type implements Writer interface
func (d *DatePartitionedWriter) Type() FileType {
	return d.underlying.Type()
}

// GetUnderlying returns the underlying writer for cases where specific methods need to be called
func (d *DatePartitionedWriter) GetUnderlying() Writer {
	return d.underlying
}

// wrapUploadableWithDatePath wraps an uploadable to modify its output path with date directories
func (d *DatePartitionedWriter) wrapUploadableWithDatePath(uploadable Uploadeable) Uploadeable {
	if d.currentTime.IsZero() {
		d.logger.Warn("no timestamp available for date partitioning, using original path")
		return uploadable
	}

	// Since the real uploadable types (dataFile, localFile) have unexported fields,
	// we can't modify them via reflection. Instead, we wrap them.
	return &dateAwareUploadable{
		underlying: uploadable,
		dateTime:   d.currentTime,
		dateFormat: d.dateFormat,
		logger:     d.logger,
	}
}

// dateAwareUploadable wraps an Uploadeable as a fallback when reflection fails
type dateAwareUploadable struct {
	underlying Uploadeable
	dateTime   time.Time
	dateFormat string
	logger     *zap.Logger
}

// Upload implements Uploadeable interface with date-based path modification
func (d *dateAwareUploadable) Upload(ctx context.Context, store dstore.Store) (string, error) {
	if d.dateTime.IsZero() {
		d.logger.Warn("no timestamp available for date partitioning, using original path")
		return d.underlying.Upload(ctx, store)
	}

	// Get the original upload path first
	originalPath, err := d.underlying.Upload(ctx, store)
	if err != nil {
		return "", err
	}

	// Apply date-based path modification to the returned path
	dateDir := d.dateTime.UTC().Format(d.dateFormat)
	newPath := d.addDateToPath(originalPath, dateDir)

	d.logger.Debug("date partitioning applied to path",
		zap.String("original_path", originalPath),
		zap.String("modified_path", newPath),
		zap.Time("block_time", d.dateTime),
	)

	return newPath, nil
}

// addDateToPath adds the date directory to a file path
func (d *dateAwareUploadable) addDateToPath(originalPath, dateDir string) string {
	dir := path.Dir(originalPath)
	filename := path.Base(originalPath)

	if dir == "." {
		return path.Join(dateDir, filename)
	} else {
		return path.Join(dir, dateDir, filename)
	}
}
