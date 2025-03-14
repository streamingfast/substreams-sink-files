package parquetx

import (
	"strings"
	"testing"

	"github.com/lithammer/dedent"
	pbtesting "github.com/streamingfast/substreams-sink-files/internal/pb/tests"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestSchemaFromMessageDescriptor(t *testing.T) {
	tests := []struct {
		name   string
		args   protoreflect.MessageDescriptor
		schema string
	}{
		{
			"custom column type",
			(&pbtesting.RowColumnTypeUint256{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
				message RowColumnTypeUint256 {
				  required fixed_len_byte_array(32) amount (DECIMAL(76,0));
				}
			`),
		},
		{
			"repeated string field",
			(&pbtesting.RowColumnRepeatedString{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
                message rows {
                  repeated binary values (STRING);
                }
			`),
		},
		{
			"sandwiched optional field",
			(&pbtesting.RowColumnSandwichedOptional{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
                message rows {
                  required binary prefix (STRING);
                  optional binary value (STRING);
                  required binary suffix (STRING);
                }
			`),
		},
		{
			"enum field",
			(&pbtesting.RowColumEnum{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
                message rows {
                  required binary value (ENUM);
                }
			`),
		},
		{
			"nested field",
			(&pbtesting.RowColumnNestedMessage{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
            	message rows {
            	  required group nested {
            	    required binary value (STRING);
            	  }
            	}
			`),
		},
		{
			"repeated nested field",
			(&pbtesting.RowColumnRepeatedNestedMessage{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
            	message rows {
            	  repeated group nested {
            	    required binary value (STRING);
            	  }
            	}
			`),
		},
		{
			"nested has repeated field",
			(&pbtesting.RowColumnNestedRepeatedMessage{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
				message rows {
				  required group nested {
				    repeated binary value (STRING);
				  }
				}
			`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := SchemaFromMessageDescriptor(tt.args, nil)
			schemaString := strings.ReplaceAll(schema.String(), "\t", "  ")

			assert.Equal(t, tt.schema, schemaString)
		})
	}
}

func schemaLiteral(s string) string {
	return strings.Trim(dedent.Dedent(s), "\n")
}
