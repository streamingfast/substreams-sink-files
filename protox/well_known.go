package protox

import (
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// IsWellKnownTimestampField returns true if the field is a well-known timestamp field from google.protobuf.Timestamp.
func IsWellKnownTimestampField(field protoreflect.FieldDescriptor) bool {
	return field.Kind() == protoreflect.MessageKind && field.Message().FullName() == "google.protobuf.Timestamp"
}

// IsWellKnownGoogleField returns true if the field is a well-known google types field like timestamp, any, empty,
// etc, a field is considered well-known if it's message name starts with "google.protobuf.".
func IsWellKnownGoogleField(field protoreflect.FieldDescriptor) bool {
	return field.Kind() == protoreflect.MessageKind && strings.HasPrefix(string(field.Message().FullName()), "google.protobuf.")
}
