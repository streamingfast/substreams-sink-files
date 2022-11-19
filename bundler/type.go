package bundler

import (
	"time"
)

type State struct {
	Cursor         string         `yaml:"cursor" json:"cursor"`
	Block          blockState     `yaml:"block" json:"block"`
	ActiveBoundary activeBoundary `yaml:"active_boundary" json:"active_boundary"`

	// StartedAt is the time this process was launching initially without accounting to any restart, once set, this
	// value, it's never re-written (unless the file does not exist anymore).
	StartedAt time.Time `yaml:"started_at,omitempty" json:"started_at,omitempty"`
	// RestartedAt is the time this process was last launched meaning it's reset each time the process start. This value
	// in contrast to `StartedAt` change over time each time the process is restarted.
	RestartedAt  time.Time `yaml:"restarted_at,omitempty" json:"restarted_at,omitempty"`
	LastSyncedAt time.Time `yaml:"last_synced_at,omitempty" json:"last_synced_at,omitempty"`
}

func newSate() *State {
	return &State{
		Cursor: "",
		Block:  blockState{"", 0},
	}
}

type blockState struct {
	ID     string `yaml:"id"`
	Number uint64 `yaml:"number"`
}

type activeBoundary struct {
	StartBlockNumber uint64 `yaml:"start_block_number"`
	EndBlockNumber   uint64 `yaml:"end_block_number"`
	Filename         string `yaml:"workingFilename"`
}
