package pq

import (
	"testing"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/builder"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
)

func TestQuery_Resolve(t *testing.T) {
	// Helper function to create test data using completely independent descriptors
	createTransfersData := func(transferValues ...uint64) ([]byte, *desc.MessageDescriptor, error) {
		// Create completely independent descriptors using builder
		// This avoids any association with the compiled Go types

		// Create Transfer message descriptor
		transferBuilder := builder.NewMessage("Transfer")
		transferBuilder.AddField(builder.NewField("value", builder.FieldTypeUInt64()).SetNumber(1))
		transferDesc, err := transferBuilder.Build()
		if err != nil {
			return nil, nil, err
		}

		// Create Transfers message descriptor
		transfersBuilder := builder.NewMessage("Transfers")
		transfersBuilder.AddField(builder.NewField("transfers", builder.FieldTypeImportedMessage(transferDesc)).SetRepeated().SetNumber(1))
		transfersDesc, err := transfersBuilder.Build()
		if err != nil {
			return nil, nil, err
		}

		// Create dynamic message factory
		factory := dynamic.NewMessageFactoryWithDefaults()

		// Create dynamic transfers message
		transfersMsg := factory.NewDynamicMessage(transfersDesc)

		// Create and add transfer messages
		for _, value := range transferValues {
			transferMsg := factory.NewDynamicMessage(transferDesc)
			transferMsg.SetFieldByNumber(1, value)                // value field
			transfersMsg.AddRepeatedFieldByNumber(1, transferMsg) // transfers field
		}

		// Marshal to bytes
		bytes, err := transfersMsg.Marshal()
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

			// Verify that we got dynamic messages back
			for j, msg := range got {
				require.NotNil(t, msg, "message at index %d should not be nil", j)
				// Verify it's a dynamic message by checking we can get field value
				if tt.wantCount > 0 {
					value, err := msg.TryGetFieldByNumber(1) // value field
					require.NoError(t, err)
					require.NotNil(t, value, "value field should not be nil at index %d", j)
					// Verify the value matches what we put in
					require.Equal(t, tt.testData[j], value.(uint64), "value should match at index %d", j)
				}
			}
		})
	}
}

func TestQuery_ResolveSingleTransfer(t *testing.T) {
	// Helper function to create a single Transfer message
	createSingleTransferData := func(transferValue uint64) ([]byte, *desc.MessageDescriptor, error) {
		// Create Transfer message descriptor
		transferBuilder := builder.NewMessage("Transfer")
		transferBuilder.AddField(builder.NewField("value", builder.FieldTypeUInt64()).SetNumber(1))
		transferDesc, err := transferBuilder.Build()
		if err != nil {
			return nil, nil, err
		}

		// Create dynamic message factory
		factory := dynamic.NewMessageFactoryWithDefaults()

		// Create single transfer message
		transferMsg := factory.NewDynamicMessage(transferDesc)
		transferMsg.SetFieldByNumber(1, transferValue) // value field

		// Marshal to bytes
		bytes, err := transferMsg.Marshal()
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
				// Verify it's a dynamic message by checking we can get field value
				value, err := got[0].TryGetFieldByNumber(1) // value field
				require.NoError(t, err)
				require.NotNil(t, value, "value field should not be nil")
				require.Equal(t, tt.testValue, value.(uint64), "value should match")
			}
		})
	}
}
