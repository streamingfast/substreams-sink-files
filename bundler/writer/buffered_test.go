package writer

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBufferedIO(t *testing.T) {
	listFiles := func(root string) (out []string) {
		filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err == nil {
				if !d.IsDir() {
					out = append(out, strings.Replace(path, root, "", 1))
				}
			}

			return err
		})
		return
	}

	tests := []struct {
		name       string
		bufferSize uint64
		checks     func(t *testing.T, writer *BufferedIO, workingDir string, output *dstore.MockStore)
	}{
		{
			"all in memory no write",
			16,
			func(t *testing.T, writer *BufferedIO, workingDir string, output *dstore.MockStore) {
				require.NoError(t, writer.StartBoundary(bstream.NewInclusiveRange(0, 10)))
				require.NoError(t, writer.Write([]byte("{first}")))
				require.NoError(t, writer.Write([]byte("{second}")))

				require.NoError(t, writer.CloseBoundary(context.Background()))
				writtenFiles := listFiles(workingDir)

				require.NoError(t, writer.Upload(context.Background()))

				assert.Len(t, writtenFiles, 0)
				assert.Equal(t, map[string][]byte{
					"0000000000-0000000010.jsonl": []byte(`{first}{second}`),
				}, output.Files)
			},
		},

		{
			"write to file",
			4,
			func(t *testing.T, writer *BufferedIO, workingDir string, output *dstore.MockStore) {
				require.NoError(t, writer.StartBoundary(bstream.NewInclusiveRange(0, 10)), "start boundary")
				require.NoError(t, writer.Write([]byte("{first}")), "write first content")
				require.NoError(t, writer.Write([]byte("{second}")), "write second content")

				require.NoError(t, writer.CloseBoundary(context.Background()), "closing boundary")

				writtenFiles := listFiles(workingDir)

				require.NoError(t, writer.Upload(context.Background()), "upload file")

				assert.ElementsMatch(t, []string{"/0000000000-0000000010.tmp.jsonl"}, writtenFiles)
				assert.Equal(t, map[string][]byte{
					"0000000000-0000000010.jsonl": []byte(`{first}{second}`),
				}, output.Files)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workingDir := t.TempDir()
			outputStore := dstore.NewMockStore(nil)

			writer := NewBufferedIO(tt.bufferSize, workingDir, outputStore, FileTypeJSONL, zlog)

			tt.checks(t, writer, workingDir, outputStore)
		})
	}
}
