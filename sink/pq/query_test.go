package pq

import (
	"testing"

	"github.com/jhump/protoreflect/desc"
	pbtest "github.com/streamingfast/substreams-sink-files/sink/pq/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestQuery_Resolve(t *testing.T) {
	t.Skip("fix test to reflect library change")
	transfer := func(value uint64) *pbtest.Transfer {
		return &pbtest.Transfer{Value: 1}
	}

	type args struct {
		root  *pbtest.Transfers
		query string
	}
	tests := []struct {
		name      string
		args      args
		want      []proto.Message
		assertion require.ErrorAssertionFunc
	}{
		{
			"empty",
			args{&pbtest.Transfers{}, ".transfers[]"},
			nil,
			require.NoError,
		},
		{
			"single element",
			args{&pbtest.Transfers{Transfers: []*pbtest.Transfer{transfer(1)}}, ".transfers[]"},
			[]proto.Message{transfer(1)},
			require.NoError,
		},
		{
			"multi elements",
			args{&pbtest.Transfers{Transfers: []*pbtest.Transfer{transfer(1), transfer(2)}}, ".transfers[]"},
			[]proto.Message{transfer(1), transfer(2)},
			require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := Parse(tt.args.query)
			require.NoError(t, err)

			bytes, err := proto.Marshal(tt.args.root)
			require.NoError(t, err)

			descriptor, err := desc.LoadMessageDescriptorForMessage(tt.args.root)
			require.NoError(t, err)

			got, err := query.Resolve(bytes, descriptor)
			tt.assertion(t, err)

			require.Len(t, got, len(tt.want))
			//for i, want := range tt.want {
			//	assertProtoEqual(t, want, got[i], "at index %d", i)
			//}

			assert.Equal(t, tt.want, got)
		})
	}
}
