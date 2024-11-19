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
