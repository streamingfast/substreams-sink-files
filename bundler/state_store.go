package bundler

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ghodss/yaml"
	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
)

type StateStore struct {
	startOnce sync.Once

	outputPath string

	state *State
}

func loadStateStore(outputPath string) (*StateStore, error) {
	s := &State{}
	content, err := os.ReadFile(outputPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if err != nil && os.IsNotExist(err) {
		s = newSate()
	}

	if err := yaml.Unmarshal(content, s); err != nil {
		return nil, fmt.Errorf("unmarshal state file %q: %w", outputPath, err)
	}
	return &StateStore{
		outputPath: outputPath,
		state:      s,
	}, nil
}

func (s *StateStore) Read() (cursor *sink.Cursor, err error) {
	return sink.NewCursor(s.state.Cursor, bstream.NewBlockRef(s.state.Block.ID, s.state.Block.Number)), nil
}

func (s *StateStore) newBoundary(filename string, boundary *bstream.Range) {
	s.state.ActiveBoundary.StartBlockNumber = boundary.StartBlock()
	s.state.ActiveBoundary.EndBlockNumber = *boundary.EndBlock()
	s.state.ActiveBoundary.Filename = filename
}

func (s *StateStore) setCursor(cursor *sink.Cursor) {
	s.startOnce.Do(func() {
		restartAt := time.Now()
		if s.state.StartedAt.IsZero() {
			s.state.StartedAt = restartAt
		}
		s.state.RestartedAt = restartAt
	})

	s.state.Cursor = cursor.Cursor
	s.state.Block = blockState{
		ID:     cursor.Block.ID(),
		Number: cursor.Block.Num(),
	}
}

func (s *StateStore) Save() error {
	content, err := yaml.Marshal(s.state)
	if err != nil {
		return fmt.Errorf("unable to marshal state: %w", err)
	}

	if err := os.WriteFile(s.outputPath, content, os.ModePerm); err != nil {
		return fmt.Errorf("unable to write state file: %w", err)
	}
	return nil
}
