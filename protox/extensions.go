package protox

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func GetMessageExtensionValue[T any](message protoreflect.MessageDescriptor, extensionType protoreflect.ExtensionType, defaultIfUnset T) (value T, found bool) {
	return getExtensionValue[T, *descriptorpb.MessageOptions](message, extensionType, defaultIfUnset)
}

func GetFieldExtensionValue[T any](field protoreflect.FieldDescriptor, extensionType protoreflect.ExtensionType, defaultIfUnset T) (value T, found bool) {
	return getExtensionValue[T, *descriptorpb.FieldOptions](field, extensionType, defaultIfUnset)
}

func getExtensionValue[T any, O protoreflect.ProtoMessage](
	descriptor protoreflect.Descriptor,
	extensionType protoreflect.ExtensionType,
	defaultIfUnset T,
) (value T, found bool) {
	options, hasOptions := descriptor.Options().(O)
	if !hasOptions {
		return defaultIfUnset, false
	}

	if !proto.HasExtension(options, extensionType) {
		return defaultIfUnset, false
	}

	return proto.GetExtension(options, extensionType).(T), true
}
