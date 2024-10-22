package protox

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
	parquetpb "github.com/streamingfast/substreams-sink-files/pb/parquet"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var (
	// Uncompressed is a parquet compression codec representing uncompressed
	// pages.
	Uncompressed = &uncompressed.Codec{}
)

func ParquetSchemaFromMessageDescriptor(descriptor protoreflect.MessageDescriptor) *parquet.Schema {
	return parquet.NewSchema(string(descriptor.Name()), newMessageNode(descriptor))
}

type ParquetTableResult struct {
	Descriptor protoreflect.MessageDescriptor
	Schema     *parquet.Schema
}

func parquetTableResult(descriptor protoreflect.MessageDescriptor, schema *parquet.Schema) ParquetTableResult {
	return ParquetTableResult{
		Descriptor: descriptor,
		Schema:     schema,
	}
}

func ParquetFindTablesInMessageDescriptor(descriptor protoreflect.MessageDescriptor) (out []ParquetTableResult) {
	walkMessage(descriptor, func(child protoreflect.MessageDescriptor) {
		options, hasOptions := child.Options().(*descriptorpb.MessageOptions)
		if !hasOptions || options == nil {
			return
		}

		tableName := proto.GetExtension(options, parquetpb.E_TableName).(string)
		if tableName == "" {
			return
		}

		// We've got `option (parquet.table_name)` set, let's create a table for it
		out = append(out, parquetTableResult(child, parquet.NewSchema(tableName, newMessageNode(child))))
	})

	// If we found any specific tables, we stop there
	if len(out) > 0 {
		return out
	}

	// Otherwise, let's support the case to pickup each repeated fields as a table
	repeatedFieldCount := MessageRepeatedFieldCount(descriptor)

	// There is no repeated fields, assume we have a 1:1 mapping between a block and a row, use the message as-is
	if repeatedFieldCount == 0 {
		return []ParquetTableResult{parquetTableResult(descriptor, parquet.NewSchema(string(descriptor.Name()), newMessageNode(descriptor)))}
	}

	// We skip fields that are repeated of primitive types for now
	for _, field := range FindMessageRepeatedFields(descriptor) {
		if field.Message() == nil {
			// It means we are dealing with a `repeated <primitive>` type like list of string which we don't support
			continue
		}

		tableName := field.JSONName()
		out = append(out, parquetTableResult(field.Message(), parquet.NewSchema(tableName, newMessageNode(field.Message()))))
	}

	return out
}

func walkMessage(root protoreflect.MessageDescriptor, onChild func(onChild protoreflect.MessageDescriptor)) {
	seenNodes := make(map[protoreflect.FullName]bool, 0)

	var inner func(node protoreflect.MessageDescriptor)
	inner = func(node protoreflect.MessageDescriptor) {
		if exists := seenNodes[node.FullName()]; exists {
			// Stop recursion if we already visited this node
			return
		}

		seenNodes[node.FullName()] = true

		fields := node.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			if field.Kind() == protoreflect.MessageKind {
				onChild(field.Message())
				inner(node)
				continue
			}

			if field.IsList() && field.Message() != nil {
				onChild(field.Message())
				inner(node)
			}
		}
	}

	inner(root)
}

var _ parquet.Node = (*messageNode)(nil)

type messageNode struct {
	descriptor protoreflect.MessageDescriptor
	fields     []parquet.Field
}

func newMessageNode(descriptor protoreflect.MessageDescriptor) *messageNode {
	fields := descriptor.Fields()
	parquetFields := make([]parquet.Field, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		field.Options()

		options, hasOptions := field.Options().(*descriptorpb.FieldOptions)
		if hasOptions && options != nil {
			isIgnored := proto.HasExtension(options, parquetpb.E_Ignored) && proto.GetExtension(options, parquetpb.E_Ignored).(bool)
			if isIgnored {
				continue
			}
		}

		parquetFields = append(parquetFields, toParquetField(field))
	}

	return &messageNode{
		descriptor: descriptor,
		fields:     parquetFields,
	}
}

func toParquetField(field protoreflect.FieldDescriptor) parquet.Field {
	return &messageField{
		Node:      protoreflectKindToParquetNode(field),
		fieldName: string(field.Name()),
	}
}

func protoreflectKindToParquetNode(field protoreflect.FieldDescriptor) parquet.Node {
	switch field.Kind() {
	case protoreflect.StringKind:
		return parquet.String()
	case protoreflect.BoolKind:
		return parquet.Leaf(parquet.BooleanType)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return parquet.Int(32)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return parquet.Int(64)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return parquet.Uint(32)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return parquet.Uint(64)
	case protoreflect.FloatKind:
		return parquet.Leaf(parquet.FloatType)
	case protoreflect.DoubleKind:
		return parquet.Leaf(parquet.DoubleType)
	case protoreflect.BytesKind:
		return parquet.Leaf(parquet.ByteArrayType)
	case protoreflect.EnumKind:
		return parquet.Enum()

	case protoreflect.MessageKind:
		if IsWellKnownTimestampField(field) {
			return parquet.Timestamp(parquet.Nanosecond)
		}

		panic(fmt.Errorf("field %s is of kind protoreflect.MessageKind which is not supported yet", field.FullName()))
	case protoreflect.GroupKind:
		panic(fmt.Errorf("field %s is of kind protoreflect.GroupKind which is not supported yet", field.FullName()))

	default:
		panic(fmt.Errorf("field %s is of kind %s which is not supported yet", field.FullName(), field.Kind()))
	}
}

// Compression implements parquet.Node.
func (m *messageNode) Compression() compress.Codec {
	return Uncompressed
}

// Encoding implements parquet.Node.
func (m *messageNode) Encoding() encoding.Encoding {
	return nil
}

// Fields implements parquet.Node.
func (m *messageNode) Fields() []parquet.Field {
	return m.fields
}

// GoType implements parquet.Node.
func (m *messageNode) GoType() reflect.Type {
	return goTypeOfNode(m)
}

// ID implements parquet.Node.
func (m *messageNode) ID() int {
	return 0
}

// Leaf implements parquet.Node.
func (m *messageNode) Leaf() bool {
	return false
}

// Optional implements parquet.Node.
func (m *messageNode) Optional() bool {
	return false
}

// Repeated implements parquet.Node.
func (m *messageNode) Repeated() bool {
	return false
}

// Required implements parquet.Node.
func (m *messageNode) Required() bool {
	return true
}

// String implements parquet.Node.
func (m *messageNode) String() string {
	return sprint("", m)
}

// Type implements parquet.Node.
func (m *messageNode) Type() parquet.Type {
	return groupType{}
}

func sprint(name string, node parquet.Node) string {
	s := new(strings.Builder)
	parquet.PrintSchema(s, name, node)
	return s.String()
}

var _ parquet.Field = messageField{}

type messageField struct {
	parquet.Node
	fieldName string
}

// Returns the name of this field in its parent node.
func (m messageField) Name() string {
	return m.fieldName
}

// Given a reference to the Go value matching the structure of the parent
// node, returns the Go value of the field.
func (m messageField) Value(base reflect.Value) reflect.Value {
	panic("parquet.Field#Value(base) is not implemented yet, really needed just to write stuff?")
}

type groupType struct{}

func (groupType) String() string { return "group" }

func (groupType) Kind() parquet.Kind {
	panic("cannot call Kind on parquet group")
}

func (groupType) Compare(parquet.Value, parquet.Value) int {
	panic("cannot compare values on parquet group")
}

func (groupType) NewColumnIndexer(int) parquet.ColumnIndexer {
	panic("cannot create column indexer from parquet group")
}

func (groupType) NewDictionary(int, int, encoding.Values) parquet.Dictionary {
	panic("cannot create dictionary from parquet group")
}

func (t groupType) NewColumnBuffer(int, int) parquet.ColumnBuffer {
	panic("cannot create column buffer from parquet group")
}

func (t groupType) NewPage(int, int, encoding.Values) parquet.Page {
	panic("cannot create page from parquet group")
}

func (t groupType) NewValues(_ []byte, _ []uint32) encoding.Values {
	panic("cannot create values from parquet group")
}

func (groupType) Encode(_ []byte, _ encoding.Values, _ encoding.Encoding) ([]byte, error) {
	panic("cannot encode parquet group")
}

func (groupType) Decode(_ encoding.Values, _ []byte, _ encoding.Encoding) (encoding.Values, error) {
	panic("cannot decode parquet group")
}

func (groupType) EstimateDecodeSize(_ int, _ []byte, _ encoding.Encoding) int {
	panic("cannot estimate decode size of parquet group")
}

func (groupType) AssignValue(reflect.Value, parquet.Value) error {
	panic("cannot assign value to a parquet group")
}

func (t groupType) ConvertValue(parquet.Value, parquet.Type) (parquet.Value, error) {
	panic("cannot convert value to a parquet group")
}

func (groupType) Length() int { return 0 }

func (groupType) EstimateSize(int) int { return 0 }

func (groupType) EstimateNumValues(int) int { return 0 }

func (groupType) ColumnOrder() *format.ColumnOrder { return nil }

func (groupType) PhysicalType() *format.Type { return nil }

func (groupType) LogicalType() *format.LogicalType { return nil }

func (groupType) ConvertedType() *deprecated.ConvertedType { return nil }

func goTypeOfNode(node parquet.Node) reflect.Type {
	fields := node.Fields()
	structFields := make([]reflect.StructField, len(fields))
	for i, field := range fields {
		structFields[i].Name = exportedStructFieldName(field.Name())
		structFields[i].Type = field.GoType()
	}
	return reflect.StructOf(structFields)
}

func exportedStructFieldName(name string) string {
	firstRune, size := utf8.DecodeRuneInString(name)
	return string([]rune{unicode.ToUpper(firstRune)}) + name[size:]
}
