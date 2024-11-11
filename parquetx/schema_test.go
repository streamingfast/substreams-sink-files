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
			(&pbtesting.RowColumnType{}).ProtoReflect().Descriptor(),
			schemaLiteral(`
				message RowColumnType {
				  required fixed_len_byte_array(32) amount;
				}
			`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := SchemaFromMessageDescriptor(tt.args)
			schemaString := strings.ReplaceAll(schema.String(), "\t", "  ")

			assert.Equal(t, tt.schema, schemaString)
		})
	}
}

func schemaLiteral(s string) string {
	return strings.Trim(dedent.Dedent(s), "\n")
}
