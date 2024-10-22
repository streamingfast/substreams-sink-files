package protox

import "google.golang.org/protobuf/reflect/protoreflect"

func IsWellKnownTimestampField(field protoreflect.FieldDescriptor) bool {
	return field.Kind() == protoreflect.MessageKind && field.Message().FullName() == "google.protobuf.Timestamp"
}
