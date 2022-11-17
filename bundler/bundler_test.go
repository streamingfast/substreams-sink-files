package bundler

import (
	"github.com/magiconair/properties/assert"
	"github.com/streamingfast/bstream"
	"testing"
)

func TestBundler_boundariesToSave(t *testing.T) {
	tests := []struct {
		name          string
		startBlockNum uint64
		bundlerSize   uint64
		blockNum      uint64
		expect        []*bstream.Range
	}{
		{"before boundary", 0, 100, 98, []*bstream.Range{}},
		{"on boundary", 0, 100, 100, []*bstream.Range{
			bstream.NewRangeExcludingEnd(0, 100),
		}},
		{"above  boundary", 0, 100, 107, []*bstream.Range{
			bstream.NewRangeExcludingEnd(0, 100),
		}},
		{"above  boundary", 0, 100, 199, []*bstream.Range{
			bstream.NewRangeExcludingEnd(0, 100),
		}},
		{"above  boundary", 2, 100, 200, []*bstream.Range{
			bstream.NewRangeExcludingEnd(2, 100),
			bstream.NewRangeExcludingEnd(100, 200),
		}},
		{"above  boundary", 4, 100, 763, []*bstream.Range{
			bstream.NewRangeExcludingEnd(4, 100),
			bstream.NewRangeExcludingEnd(100, 200),
			bstream.NewRangeExcludingEnd(200, 300),
			bstream.NewRangeExcludingEnd(300, 400),
			bstream.NewRangeExcludingEnd(400, 500),
			bstream.NewRangeExcludingEnd(500, 600),
			bstream.NewRangeExcludingEnd(600, 700),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := &Bundler{
				size:          test.bundlerSize,
				startBlockNum: test.startBlockNum,
			}
			assert.Equal(t, test.expect, b.boundariesToSave(test.blockNum))
		})
	}

}
