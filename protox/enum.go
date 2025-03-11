package protox

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// EnumValueToString returns the string representation of the given enum value.
func EnumValueToString(enumValue protoreflect.EnumValueDescriptor) string {
	return string(enumValue.Name())
}

func EnumKnownValuesDebugString(enum protoreflect.EnumDescriptor) string {
	values := make([]string, 0, enum.Values().Len())
	for i := 0; i < enum.Values().Len(); i++ {
		values = append(values, fmt.Sprintf("%s (%d)", EnumValueToString(enum.Values().Get(i)), i))
	}
	return strings.Join(values, ", ")
}
