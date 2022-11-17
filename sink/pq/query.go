package pq

import (
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"go.uber.org/zap"
)

func (q *Query) Resolve(root []byte, descriptor *desc.MessageDescriptor) (out []*dynamic.Message, err error) {
	zlog.Debug("resolving query", zap.String("message_type", descriptor.GetFullyQualifiedName()))

	if len(q.Elements) != 3 {
		return nil, fmt.Errorf("only accepting query of 3 elements, got %d", len(q.Elements))
	}

	if q.Elements[0].Kind() != ExpressionKindCurrent || q.Elements[1].Kind() != ExpressionKindField || q.Elements[2].Kind() != ExpressionKindArray {
		return nil, fmt.Errorf("only accepting query in the form '.<fieldName>[]'")
	}

	dynMsg := dynamic.NewMessageFactoryWithDefaults().NewDynamicMessage(descriptor)
	if err := dynMsg.Unmarshal(root); err != nil {
		return nil, fmt.Errorf("unmarshal dynamic message: %w", err)
	}

	// Hard-coded for now
	fieldName := q.Elements[1].(*FieldAccess).Name

	fieldDesc := dynMsg.FindFieldDescriptorByName(fieldName)
	if fieldDesc == nil {
		return nil, fmt.Errorf("field %q does not exist on proto of type %q", fieldName, descriptor.GetFullyQualifiedName())
	}

	if !fieldDesc.IsRepeated() {
		return nil, fmt.Errorf("field %q of type %q is not repeated while accessing array field", fieldName, fieldDesc.GetFullyQualifiedName())
	}

	if tracer.Enabled() {
		zlog.Debug("resolved field",
			zap.Int32("id", fieldDesc.GetNumber()),
			zap.String("name", fieldDesc.GetFullyQualifiedName()),
			zap.Stringer("type", fieldDesc.GetType()),
		)
	}

	count, err := dynMsg.TryFieldLength(fieldDesc)
	if err != nil {
		return nil, fmt.Errorf("get field %q of type %q length: %w", fieldName, fieldDesc.GetFullyQualifiedName(), err)
	}

	if tracer.Enabled() {
		zlog.Debug("resolved repeated field count", zap.String("name", fieldDesc.GetFullyQualifiedName()), zap.Int("count", count))
	}

	if count == 0 {
		return nil, nil
	}

	out = make([]*dynamic.Message, count)
	for i := 0; i < count; i++ {
		value, err := dynMsg.TryGetRepeatedField(fieldDesc, i)
		if err != nil {
			return nil, fmt.Errorf("get field %q of type %q at index %d length: %w", fieldName, fieldDesc.GetFullyQualifiedName(), i, err)
		}

		out[i] = value.(*dynamic.Message)
	}

	if tracer.Enabled() {
		zlog.Debug("resolved repeated field values", zap.String("name", fieldDesc.GetFullyQualifiedName()), zap.Int("count", len(out)))
	}

	return out, nil
}
