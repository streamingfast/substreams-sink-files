package bundler

import (
	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBoundary_newBoundary(t *testing.T) {
	tests := []struct {
		name        string
		bundlerSize uint64
		blockNum    uint64
		expect      *bstream.Range
	}{
		{"start of boundary w/ size 10", 10, 0, bstream.NewRangeExcludingEnd(0, 10)},
		{"middle of boundary w/ size 10", 10, 7, bstream.NewRangeExcludingEnd(0, 10)},
		{"last block of boundary w/ size 10", 10, 9, bstream.NewRangeExcludingEnd(0, 10)},
		{"end block of boundary w/ size 10", 10, 10, bstream.NewRangeExcludingEnd(10, 20)},
		{"start of boundary w/ size 100", 100, 0, bstream.NewRangeExcludingEnd(0, 100)},
		{"middle of boundary w/ size 100", 100, 73, bstream.NewRangeExcludingEnd(0, 100)},
		{"last block of boundary w/ size 100", 100, 99, bstream.NewRangeExcludingEnd(0, 100)},
		{"end block of boundary w/ size 100", 100, 100, bstream.NewRangeExcludingEnd(100, 200)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := &Bundler{
				size: test.bundlerSize,
			}
			assert.Equal(t, test.expect, b.newBoundary(test.blockNum))
		})
	}
}

func TestBoundary_computeEndBlock(t *testing.T) {
	tests := []struct {
		name   string
		start  uint64
		size   uint64
		expect uint64
	}{
		{"on boundary", 100, 100, 200},
		{"off boundary", 123, 100, 200},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, computeEndBlock(test.start, test.size))
		})
	}
}

func TestBundler_boundariesToSkip(t *testing.T) {
	tests := []struct {
		name               string
		lastActiveBoundary *bstream.Range
		bundlerSize        uint64
		blockNum           uint64
		expect             []*bstream.Range
	}{
		{"before boundary", bstream.NewRangeExcludingEnd(0, 100), 100, 98, nil},
		{"on boundary", bstream.NewRangeExcludingEnd(0, 100), 100, 100, nil},
		{"above  boundary", bstream.NewRangeExcludingEnd(0, 100), 100, 107, nil},
		{"above  boundary", bstream.NewRangeExcludingEnd(0, 100), 100, 199, nil},
		{"above  boundary", bstream.NewRangeExcludingEnd(2, 100), 100, 200, []*bstream.Range{
			bstream.NewRangeExcludingEnd(100, 200),
		}},
		{"above  boundary", bstream.NewRangeExcludingEnd(4, 100), 100, 763, []*bstream.Range{
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
			assert.Equal(t, test.expect, boundariesToSkip(test.lastActiveBoundary, test.blockNum, test.bundlerSize))
		})
	}
}
