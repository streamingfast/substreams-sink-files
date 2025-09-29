package protox

import (
	"errors"
	"fmt"
	"iter"

	"github.com/streamingfast/logging"
	"go.uber.org/zap"
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

// WalkMessageDescriptors walks the message descriptors tree starting from the root message descriptor. It calls the
// `onChild` function for each child message descriptor.
//
// The function stops recursion if the same node is visited more than once.
//
// It follows the fields message recursively and also the repeated fields that are messages.
func WalkMessageDescriptors(root protoreflect.MessageDescriptor, logger *zap.Logger, tracer logging.Tracer, onMessageDescriptor func(messageDescriptor protoreflect.MessageDescriptor)) {
	seenNodes := make(map[protoreflect.FullName]bool, 0)

	var inner func(node protoreflect.MessageDescriptor)
	inner = func(node protoreflect.MessageDescriptor) {
		if exists := seenNodes[node.FullName()]; exists {
			logger.Debug("already visited message descriptor", zap.String("message", string(node.FullName())))
			// Stop recursion if we already visited this node
			return
		}

		seenNodes[node.FullName()] = true
		if tracer.Enabled() {
			logger.Debug("walking message descriptor", zap.String("message", string(node.FullName())))
		}

		fields := node.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)

			if tracer.Enabled() {
				logger.Debug("message field descriptor",
					zap.String("field", string(field.FullName())),
					zap.Stringer("kind", field.Kind()),
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

			onMessageDescriptor(message)
			inner(message)
			continue
		}
	}

	// The root itself must be visited as it might have a table extension making it a table
	onMessageDescriptor(root)
	inner(root)
}

// WalkMessageFields walks through all fields in a message descriptor recursively, yielding each field
// encountered during the traversal. This is useful for operations that need to process all fields
// in a message hierarchy, such as calculating leaf column counts or processing field values.
//
// The function performs a depth-first traversal of the message structure:
// - For each field in the current message, it yields the field
// - If the field is a message kind and not a well-known Google type, it recursively walks the nested message fields
// - The traversal respects field filtering (fields that should be ignored are skipped)
//
// Parameters:
//   - root: The message descriptor to start walking from
//   - logger: Logger for debug output (can be nil)
//   - tracer: Tracer for conditional debug logging (can be nil)
//   - fieldFilter: Optional function to determine if a field should be ignored (can be nil)
//
// Returns an iterator that yields protoreflect.FieldDescriptor for each field encountered.
func WalkMessageFields(root protoreflect.MessageDescriptor, logger *zap.Logger, tracer logging.Tracer, fieldFilter func(protoreflect.FieldDescriptor) bool) iter.Seq[protoreflect.FieldDescriptor] {
	return func(yield func(protoreflect.FieldDescriptor) bool) {
		var walkFields func(protoreflect.MessageDescriptor) bool
		walkFields = func(desc protoreflect.MessageDescriptor) bool {
			fields := desc.Fields()
			for i := 0; i < fields.Len(); i++ {
				field := fields.Get(i)

				// Apply field filter if provided
				if fieldFilter != nil && fieldFilter(field) {
					continue
				}

				// Yield the current field
				if !yield(field) {
					return false
				}

				// If this is a message field and not a well-known type, recurse into it
				if field.Kind() == protoreflect.MessageKind && !IsWellKnownGoogleField(field) {
					if !walkFields(field.Message()) {
						return false
					}
				}
			}
			return true
		}

		walkFields(root)
	}
}
