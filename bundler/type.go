package bundler

import (
	"time"
)

type State struct {
	Cursor         string         `yaml:"cursor" json:"cursor"`
	Block          BlockState     `yaml:"block" json:"block"`
	ActiveBoundary ActiveBoundary `yaml:"active_boundary" json:"active_boundary"`

	// StartedAt is the time this process was launching initially without accounting to any restart, once set, this
	// value, it's never re-written (unless the file does not exist anymore).
	StartedAt time.Time `yaml:"started_at,omitempty" json:"started_at,omitempty"`
	// RestartedAt is the time this process was last launched meaning it's reset each time the process start. This value
	// in contrast to `StartedAt` change over time each time the process is restarted.
	RestartedAt time.Time `yaml:"restarted_at,omitempty" json:"restarted_at,omitempty"`
}

func newState() *State {
	return &State{
		Cursor: "",
		Block:  BlockState{"", 0},
	}
}

type BlockState struct {
	ID     string `yaml:"id" json:"id"`
	Number uint64 `yaml:"number" json:"number"`
}

type ActiveBoundary struct {
	StartBlockNumber uint64 `yaml:"start_block_number"  json:"start_block_number"`
	EndBlockNumber   uint64 `yaml:"end_block_number"  json:"end_block_number"`
}
