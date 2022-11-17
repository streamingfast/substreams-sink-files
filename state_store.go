package substreams_file_sink

import (
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/substreams-sink-files/sink"
	"os"
)

type StateStore struct {
	outputPath string
}

func NewStateStore(outputPath string) *StateStore {
	return &StateStore{
		outputPath: outputPath,
	}
}

func (s *StateStore) Read() (cursor *sink.Cursor, err error) {
	content, err := os.ReadFile(s.outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			return sink.NewBlankCursor(), nil
		}

		return nil, fmt.Errorf("read file: %w", err)
	}

	var state *sink.State
	if err := yaml.Unmarshal(content, state); err != nil {
		return nil, fmt.Errorf("unmarshal state file %q: %w", s.outputPath, err)
	}

	return sink.NewCursor(state.Cursor, bstream.NewBlockRef(state.Block.ID, state.Block.Number)), nil
}

func (s *StateStore) Save(state *sink.State) error {
	content, err := yaml.Marshal(state)
	if err != nil {
		return fmt.Errorf("unable to marshal state: %w", err)
	}

	if err := os.WriteFile(s.outputPath, content, os.ModePerm); err != nil {
		return fmt.Errorf("unable to write state file: %w", err)
	}
	return nil
}
