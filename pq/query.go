package pq

import (
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Resolve resolves the query and returns dynamicpb.Message instances
// This uses google.golang.org/protobuf for efficient JSON marshaling
func (q *Query) Resolve(root []byte, descriptor protoreflect.MessageDescriptor) (out []*dynamicpb.Message, err error) {
	zlog.Debug("resolving query", zap.String("message_type", string(descriptor.FullName())))

	// Handle root-level query case (single element: current access)
	if len(q.Elements) == 1 && q.Elements[0].Kind() == ExpressionKindCurrent {
		dynMsg := dynamicpb.NewMessage(descriptor)
		if err := proto.Unmarshal(root, dynMsg); err != nil {
			return nil, fmt.Errorf("unmarshal dynamic message: %w", err)
		}

		return []*dynamicpb.Message{dynMsg}, nil
	}

	if len(q.Elements) != 3 {
		return nil, fmt.Errorf("only accepting query of 1 element (root query) or 3 elements, got %d", len(q.Elements))
	}

	if q.Elements[0].Kind() != ExpressionKindCurrent || q.Elements[1].Kind() != ExpressionKindField || q.Elements[2].Kind() != ExpressionKindArray {
		return nil, fmt.Errorf("only accepting query in the form '.' or '.<fieldName>[]'")
	}

	dynMsg := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(root, dynMsg); err != nil {
		return nil, fmt.Errorf("unmarshal dynamic message: %w", err)
	}

	// Hard-coded for now
	fieldName := q.Elements[1].(*FieldAccess).Name

	fieldDesc := descriptor.Fields().ByName(protoreflect.Name(fieldName))
	if fieldDesc == nil {
		return nil, fmt.Errorf("field %q does not exist on proto of type %q", fieldName, descriptor.FullName())
	}

	if !fieldDesc.IsList() {
		return nil, fmt.Errorf("field %q of type %q is not repeated while accessing array field", fieldName, fieldDesc.FullName())
	}

	if tracer.Enabled() {
		zlog.Debug("resolved field",
			zap.Int32("id", int32(fieldDesc.Number())),
			zap.String("name", string(fieldDesc.FullName())),
			zap.Stringer("type", fieldDesc.Kind()),
		)
	}

	list := dynMsg.Get(fieldDesc).List()
	count := list.Len()

	if tracer.Enabled() {
		zlog.Debug("resolved repeated field count", zap.String("name", string(fieldDesc.FullName())), zap.Int("count", count))
	}

	if count == 0 {
		return nil, nil
	}

	out = make([]*dynamicpb.Message, count)
	for i := 0; i < count; i++ {
		value := list.Get(i).Message()
		out[i] = value.Interface().(*dynamicpb.Message)
	}

	if tracer.Enabled() {
		zlog.Debug("resolved repeated field values", zap.String("name", string(fieldDesc.FullName())), zap.Int("count", len(out)))
	}

	return out, nil
}
