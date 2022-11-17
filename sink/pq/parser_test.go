package pq

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		want      *Query
		assertion require.ErrorAssertionFunc
	}{
		{
			"current with field with array",
			".fieldName[]",
			&Query{[]Expression{&CurrentAccess{}, &FieldAccess{Name: "fieldName"}, &ArrayAccess{}}},
			require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.in)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
