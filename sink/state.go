package sink

import (
	"github.com/streamingfast/bstream"
	"sync"
	"time"
)

func newState(cursor *Cursor) *State {
	return &State{
		Cursor: cursor.Cursor,
		Block: blockState{
			ID:     cursor.Block.ID(),
			Number: cursor.Block.Num(),
		},
	}
}

type State struct {
	startOnce sync.Once

	Cursor string     `yaml:"cursor" json:"cursor"`
	Block  blockState `yaml:"block" json:"block"`
	// StartedAt is the time this process was launching initially without accounting to any restart, once set, this
	// value, it's never re-written (unless the file does not exist anymore).
	StartedAt time.Time `yaml:"started_at,omitempty" json:"started_at,omitempty"`
	// RestartedAt is the time this process was last launched meaning it's reset each time the process start. This value
	// in contrast to `StartedAt` change over time each time the process is restarted.
	RestartedAt  time.Time `yaml:"restarted_at,omitempty" json:"restarted_at,omitempty"`
	LastSyncedAt time.Time `yaml:"last_synced_at,omitempty" json:"last_synced_at,omitempty"`
}

type blockState struct {
	ID     string `yaml:"id"`
	Number uint64 `yaml:"number"`
}

func (s *State) getCursor() *Cursor {
	return &Cursor{
		Cursor: s.Cursor,
		Block:  bstream.NewBlockRef(s.Block.ID, s.Block.Number),
	}
}

func (s *State) setCursor(c *Cursor) {
	s.startOnce.Do(func() {
		restartAt := time.Now()
		if s.StartedAt.IsZero() {
			s.StartedAt = restartAt
		}
		s.RestartedAt = restartAt
	})

	s.Cursor = c.Cursor
	s.Block = blockState{
		ID:     c.Block.ID(),
		Number: c.Block.Num(),
	}
	
}