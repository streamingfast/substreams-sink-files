package substreams_file_sink

import (
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/streamingfast/bstream"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

func TestConfig_Run(t *testing.T) {
	tests := []struct {
		name             string
		inputBlockRange  string
		module           *pbsubstreams.Module
		expectBlockRange *bstream.Range
		expectError      bool
	}{
		{
			name:            "specifying start and stop block",
			inputBlockRange: "12:24",
			module: &pbsubstreams.Module{
				InitialBlock: 5,
			},
			expectBlockRange: bstream.NewRangeExcludingEnd(12, 24),
		},
		{
			name:            "no block range input",
			inputBlockRange: "",
			module: &pbsubstreams.Module{
				InitialBlock: 5,
			},
			expectBlockRange: bstream.NewOpenRange(5),
		},
		{
			name:            "with only a stop block",
			inputBlockRange: "10",
			module: &pbsubstreams.Module{
				InitialBlock: 5,
			},
			expectBlockRange: bstream.NewRangeExcludingEnd(5, 10),
		},
		{
			name:            "with only a relative stop block",
			inputBlockRange: "+10",
			module: &pbsubstreams.Module{
				InitialBlock: 5,
			},
			expectBlockRange: bstream.NewRangeExcludingEnd(5, 15),
		},
		{
			name:            "with only a relative stop block",
			inputBlockRange: "+4:10",
			module: &pbsubstreams.Module{
				InitialBlock: 5,
			},
			expectBlockRange: bstream.NewRangeExcludingEnd(9, 10),
		},
		{
			name:            "with negative end block",
			inputBlockRange: "-1",
			module: &pbsubstreams.Module{
				InitialBlock: 5,
			},
			expectBlockRange: bstream.NewOpenRange(5),
		},
		{
			name:            "with start block negative end block",
			inputBlockRange: "283:-1",
			module: &pbsubstreams.Module{
				InitialBlock: 5,
			},
			expectBlockRange: bstream.NewOpenRange(283),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			blockRange, err := resolveBlockRange(test.inputBlockRange, test.module)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectBlockRange, blockRange)
			}
		})
	}
}
