package encoder

import (
	"testing"

	anypbLegacy "github.com/golang/protobuf/ptypes/any"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	pbtest "github.com/streamingfast/substreams-sink-files/encoder/testdata"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"github.com/stretchr/testify/assert"
	"github.com/test-go/testify/require"
)

func TestProtoToJson_EncodeTo(t *testing.T) {
	t.Skipf("Panics write now, did not find the correct way to work properly, this happens " +
		"because in config.go#L105-L131, we create the file description from the Substreams" +
		"package which seems to generate quick a different structure when decoding the" +
		"actual message. I was not able to replicate using a simpler setup where I control the" +
		"proto message. Would need to see how Substreams package the proto to replicate same patter")

	transfer := func(value uint64) *pbtest.Transfer {
		return &pbtest.Transfer{Value: 1}
	}

	tests := []struct {
		name      string
		entities  *pbtest.Transfers
		expected  []byte
		assertion assert.ErrorAssertionFunc
	}{
		{
			"zero transfer",
			&pbtest.Transfers{
				Transfers: nil,
			},
			nil,
			assert.NoError,
		},
		{
			"one transfer",
			&pbtest.Transfers{
				Transfers: []*pbtest.Transfer{
					transfer(1),
				},
			}, []byte(`{"a":1}`),
			assert.NoError,
		},
		// {
		// 	"three line",
		// 	&pbfilesink.Lines{Lines: [][]byte{
		// 		[]byte(`{"a":1}`),
		// 		[]byte(`{"b":2}`),
		// 		[]byte(`{"c":3}`),
		// 	}},
		// 	[]byte(`{"a":1}` + "\n" + `{"b":2}` + "\n" + `{"c":3}`),
		// 	assert.NoError,
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// desc.LoadMessageDescriptor(tt.entities.Desc)

			dynamicMsg, err := dynamic.AsDynamicMessage(tt.entities)
			require.NoError(t, err)

			descriptor, err := desc.LoadMessageDescriptorForMessage(dynamicMsg)
			require.NoError(t, err)

			tt.entities.ProtoReflect().Descriptor()

			msg := dynamic.NewMessage(descriptor)
			msg.Merge(tt.entities)

			descriptor2, err := desc.LoadMessageDescriptorForMessage(msg)
			require.NoError(t, err)

			encoder, err := NewProtoToJson(".transfers[]", descriptor2)
			require.NoError(t, err)

			writer := &testWriter{}

			// out, err := protoLegacy.Marshal(tt.entities)
			// require.NoError(t, err)

			// fmt.Println("Hex", hex.EncodeToString(out))

			// output, err := anypb.New(tt.entities)
			// require.NoError(t, err)

			test := &anypbLegacy.Any{
				TypeUrl: "test",
				Value:   []byte{0x0a, 0x02, 0x08, 0x01},
			}

			module := &pbsubstreamsrpc.MapModuleOutput{
				MapOutput: test,
			}

			tt.assertion(t, encoder.EncodeTo(module, writer))
			assert.Equal(t, tt.expected, writer.written)
		})
	}
}
