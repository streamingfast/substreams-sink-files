package writer

import (
	"fmt"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type FileType string

const (
	FileTypeJSONL FileType = "jsonl"
)

type baseWriter struct {
	outputStore dstore.Store
	fileType    FileType
	zlogger     *zap.Logger
}

func (b baseWriter) filename(blockRange *bstream.Range) string {
	return fmt.Sprintf("%010d-%010d.%s", blockRange.StartBlock(), *blockRange.EndBlock(), b.fileType)
}

func (b baseWriter) Type() FileType {
	return b.fileType
}
