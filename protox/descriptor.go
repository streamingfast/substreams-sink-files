package protox

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// FindMessageByNameInFiles finds a message descriptor by name in a list of file descriptors. It returns nil if the
// message is not found.
//
// An error is returned if the file descriptors cannot be used to create a new proto files set or an unknown error
// occurred while finding the descriptor by name.
func FindMessageByNameInFiles(files []*descriptorpb.FileDescriptorProto, name protoreflect.FullName) (protoreflect.MessageDescriptor, error) {
	protoSet := &descriptorpb.FileDescriptorSet{
		File: files,
	}

	protoFiles, err := protodesc.NewFiles(protoSet)
	if err != nil {
		return nil, fmt.Errorf("new proto files: %w", err)
	}

	descriptor, err := protoFiles.FindDescriptorByName(name)
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("find descriptor by name: %w", err)
	}

	if v, ok := descriptor.(protoreflect.MessageDescriptor); ok {
		return v, nil
	}

	return nil, nil
}

// FindMessageRepeatedFields returns all fields (shallow traversal, so first level fields) that are
// a repeated field, e.g. that [protoreflect.FieldDescriptor] `IsList` is true).
func FindMessageRepeatedFields(descriptor protoreflect.MessageDescriptor) (out []protoreflect.FieldDescriptor) {
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsList() {
			out = append(out, field)
		}
	}

	return
}

// FindMessageFirstRepeatedField returns the first field (shallow traversal, so first level fields) that is
// a repeated field, e.g. that [protoreflect.FieldDescriptor] `IsList` is true).
func FindMessageFirstRepeatedField(descriptor protoreflect.MessageDescriptor) protoreflect.FieldDescriptor {
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsList() {
			return field
		}
	}

	return nil
}

func MessageRepeatedFieldCount(descriptor protoreflect.MessageDescriptor) (count int) {
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		if fields.Get(i).IsList() {
			count++
		}
	}

	return
}

func MessageRepeatedFieldNames(descriptor protoreflect.MessageDescriptor) (fieldNames []string) {
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsList() {
			fieldNames = append(fieldNames, string(field.Name()))
		}
	}

	return
}
