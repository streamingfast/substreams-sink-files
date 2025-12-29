package pq

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestQuery_Resolve(t *testing.T) {
	// Helper function to create test data using Google's protobuf descriptors
	createTransfersData := func(transferValues ...uint64) ([]byte, protoreflect.MessageDescriptor, error) {
		// Create Transfer message descriptor using descriptorpb
		transferMsgDesc := &descriptorpb.DescriptorProto{
			Name: proto.String("Transfer"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:   proto.String("value"),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(),
				},
			},
		}

		// Create Transfers message descriptor
		transfersMsgDesc := &descriptorpb.DescriptorProto{
			Name: proto.String("Transfers"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:     proto.String("transfers"),
					Number:   proto.Int32(1),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					TypeName: proto.String(".Transfer"),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				},
			},
		}

		// Create file descriptor
		fileDesc := &descriptorpb.FileDescriptorProto{
			Name: proto.String("test.proto"),
			MessageType: []*descriptorpb.DescriptorProto{
				transferMsgDesc,
				transfersMsgDesc,
			},
		}

		// Create file descriptor set
		fileDescSet := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{fileDesc},
		}

		// Create protoreflect files
		files, err := protodesc.NewFiles(fileDescSet)
		if err != nil {
			return nil, nil, err
		}

		// Get the message descriptors
		transfersDescriptor, err := files.FindDescriptorByName("Transfers")
		if err != nil {
			return nil, nil, err
		}
		transfersDesc := transfersDescriptor.(protoreflect.MessageDescriptor)

		transferDescriptor, err := files.FindDescriptorByName("Transfer")
		if err != nil {
			return nil, nil, err
		}
		transferDesc := transferDescriptor.(protoreflect.MessageDescriptor)

		// Create dynamic transfers message
		transfersMsg := dynamicpb.NewMessage(transfersDesc)

		// Create and add transfer messages
		transfersField := transfersDesc.Fields().ByName("transfers")
		transfersList := transfersMsg.Mutable(transfersField).List()

		for _, value := range transferValues {
			transferMsg := dynamicpb.NewMessage(transferDesc)
			valueField := transferDesc.Fields().ByName("value")
			transferMsg.Set(valueField, protoreflect.ValueOfUint64(value))
			transfersList.Append(protoreflect.ValueOfMessage(transferMsg))
		}

		// Marshal to bytes
		bytes, err := proto.Marshal(transfersMsg)
		if err != nil {
			return nil, nil, err
		}

		return bytes, transfersDesc, nil
	}

	type testCase struct {
		name      string
		query     string
		wantCount int
		testData  []uint64
		assertion require.ErrorAssertionFunc
	}
	tests := []testCase{
		{
			"empty",
			".transfers[]",
			0,
			[]uint64{},
			require.NoError,
		},
		{
			"single element",
			".transfers[]",
			1,
			[]uint64{1},
			require.NoError,
		},
		{
			"multi elements",
			".transfers[]",
			2,
			[]uint64{1, 2},
			require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := Parse(tt.query)
			require.NoError(t, err)

			bytes, descriptor, err := createTransfersData(tt.testData...)
			require.NoError(t, err)

			got, err := query.Resolve(bytes, descriptor)
			tt.assertion(t, err)

			require.Len(t, got, tt.wantCount)

			// Verify that we got dynamicpb messages back
			for j, msg := range got {
				require.NotNil(t, msg, "message at index %d should not be nil", j)
				require.IsType(t, &dynamicpb.Message{}, msg, "should be a dynamicpb.Message")

				// Verify we can get field value for non-empty cases
				if tt.wantCount > 0 {
					// Get the message descriptor to find the field
					msgDesc := msg.Descriptor()
					valueField := msgDesc.Fields().ByNumber(1) // value field
					require.NotNil(t, valueField, "value field descriptor should exist")

					// Get the field value
					value := msg.Get(valueField).Uint()
					require.Equal(t, tt.testData[j], value, "value should match at index %d", j)
				}
			}
		})
	}
}

func TestQuery_ResolveSingleTransfer(t *testing.T) {
	// Helper function to create a single Transfer message
	createSingleTransferData := func(transferValue uint64) ([]byte, protoreflect.MessageDescriptor, error) {
		// Create Transfer message descriptor
		transferMsgDesc := &descriptorpb.DescriptorProto{
			Name: proto.String("Transfer"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:   proto.String("value"),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(),
				},
			},
		}

		// Create file descriptor
		fileDesc := &descriptorpb.FileDescriptorProto{
			Name: proto.String("test.proto"),
			MessageType: []*descriptorpb.DescriptorProto{
				transferMsgDesc,
			},
		}

		// Create file descriptor set
		fileDescSet := &descriptorpb.FileDescriptorSet{
			File: []*descriptorpb.FileDescriptorProto{fileDesc},
		}

		// Create protoreflect files
		files, err := protodesc.NewFiles(fileDescSet)
		if err != nil {
			return nil, nil, err
		}

		// Get the message descriptor
		transferDescriptor, err := files.FindDescriptorByName("Transfer")
		if err != nil {
			return nil, nil, err
		}
		transferDesc := transferDescriptor.(protoreflect.MessageDescriptor)

		// Create single transfer message
		transferMsg := dynamicpb.NewMessage(transferDesc)
		valueField := transferDesc.Fields().ByName("value")
		transferMsg.Set(valueField, protoreflect.ValueOfUint64(transferValue))

		// Marshal to bytes
		bytes, err := proto.Marshal(transferMsg)
		if err != nil {
			return nil, nil, err
		}

		return bytes, transferDesc, nil
	}

	type testCase struct {
		name      string
		query     string
		wantCount int
		testValue uint64
		assertion require.ErrorAssertionFunc
	}
	tests := []testCase{
		{
			"single transfer root query",
			".",
			1,
			42,
			require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := Parse(tt.query)
			require.NoError(t, err)

			bytes, descriptor, err := createSingleTransferData(tt.testValue)
			require.NoError(t, err)

			got, err := query.Resolve(bytes, descriptor)
			tt.assertion(t, err)

			require.Len(t, got, tt.wantCount)

			// For root query, verify we got the transfer message back
			if tt.query == "." {
				require.NotNil(t, got[0], "transfer message should not be nil")
				require.IsType(t, &dynamicpb.Message{}, got[0], "should be a dynamicpb.Message")

				// Verify we can get field value
				msgDesc := got[0].Descriptor()
				valueField := msgDesc.Fields().ByNumber(1) // value field
				require.NotNil(t, valueField, "value field descriptor should exist")

				value := got[0].Get(valueField).Uint()
				require.Equal(t, tt.testValue, value, "value should match")
			}
		})
	}
}
