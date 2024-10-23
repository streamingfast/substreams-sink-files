package protox

import (
	"errors"
	"fmt"

	"github.com/streamingfast/logging"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

var zlog, tracer = logging.PackageLogger("protox", "github.com/streamingfast/substreams-sink-files")

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

// WalkMessageDescriptors walks the message descriptors tree starting from the root message descriptor. It calls the
// `onChild` function for each child message descriptor.
//
// The function stops recursion if the same node is visited more than once.
//
// It follows the fields message recursively and also the repeated fields that are messages.
func WalkMessageDescriptors(root protoreflect.MessageDescriptor, onChild func(onChild protoreflect.MessageDescriptor)) {
	seenNodes := make(map[protoreflect.FullName]bool, 0)

	var inner func(node protoreflect.MessageDescriptor)
	inner = func(node protoreflect.MessageDescriptor) {
		if exists := seenNodes[node.FullName()]; exists {
			// Stop recursion if we already visited this node
			return
		}

		seenNodes[node.FullName()] = true
		if tracer.Enabled() {
			zlog.Debug("walking message descriptor", zap.String("message", string(node.FullName())))
		}

		fields := node.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)

			if tracer.Enabled() {
				zlog.Debug("message field descriptor",
					zap.String("field", string(field.FullName())),
					zap.String("kind", field.Kind().String()),
					zap.Bool("is_list", field.IsList()),
				)
			}

			if field.Kind() != protoreflect.MessageKind && !field.IsList() {
				continue
			}

			message := field.Message()
			if message == nil {
				// This can happen if field is `repeated string` (or any primitive), `Message()` returns `nil` in those cases
				continue
			}

			onChild(message)
			inner(message)
			continue
		}
	}

	inner(root)
}
