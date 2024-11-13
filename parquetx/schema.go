package parquetx

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/iancoleman/strcase"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
	parquetpb "github.com/streamingfast/substreams-sink-files/pb/parquet"
	pbparquet "github.com/streamingfast/substreams-sink-files/pb/parquet"
	"github.com/streamingfast/substreams-sink-files/protox"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	// Uncompressed is a parquet compression codec representing uncompressed
	// pages.
	Uncompressed = &uncompressed.Codec{}
)

func SchemaFromMessageDescriptor(descriptor protoreflect.MessageDescriptor, defaultColumnCompression *pbparquet.Compression) *parquet.Schema {
	return parquet.NewSchema(string(descriptor.Name()), newMessageNode(descriptor, defaultColumnCompression))
}

type TableResult struct {
	Descriptor protoreflect.MessageDescriptor
	Schema     *parquet.Schema
}

func tableResult(descriptor protoreflect.MessageDescriptor, schema *parquet.Schema) TableResult {
	return TableResult{
		Descriptor: descriptor,
		Schema:     schema,
	}
}

func FindTablesInMessageDescriptor(descriptor protoreflect.MessageDescriptor, defaultColumnCompression *pbparquet.Compression) (out []TableResult, rowExtractor ProtoRowExtractor) {
	protox.WalkMessageDescriptors(descriptor, func(child protoreflect.MessageDescriptor) {
		if tableName, hasTableName := GetMessageTableName(child); hasTableName {
			// We've got `option (parquet.table_name)` set, let's create a table for it
			out = append(out, tableResult(
				child,
				parquet.NewSchema(tableName, newMessageNode(child, defaultColumnCompression)),
			))
		}
	})

	// If we found any specific tables, we stop there
	if len(out) > 0 {
		return out, ProtoRowExtractorFromTables(out)
	}

	// Otherwise, let's support the case to pickup each repeated fields as a table
	messageRepeatedFields := protox.FindMessageRepeatedFields(descriptor)

	// There is no repeated fields, assume we have a 1:1 mapping between a block and a row, use the message as-is
	if len(messageRepeatedFields) == 0 {
		tableName := strcase.ToSnake(string(descriptor.Name()))

		return []TableResult{
			tableResult(
				descriptor,
				parquet.NewSchema(tableName, newMessageNode(descriptor, defaultColumnCompression)),
			),
		}, ProtoRowExtractorFromRoot(tableName)
	}

	// We skip fields that are repeated of primitive types for now
	repeatedFields := map[string]protoreflect.FieldDescriptor{}
	for _, field := range protox.FindMessageRepeatedFields(descriptor) {
		if IsFieldIgnored(field) {
			continue
		}

		if field.Message() == nil {
			// It means we are dealing with a `repeated <primitive>` type like list of string which we don't support
			continue
		}

		tableName := strcase.ToSnake(field.JSONName())
		out = append(out, tableResult(
			field.Message(),
			parquet.NewSchema(tableName, newMessageNode(field.Message(), defaultColumnCompression)),
		))

		repeatedFields[tableName] = field
	}

	return out, ProtoRowExtractorFromRepeatedFields(repeatedFields)
}

func GetMessageTableName(descriptor protoreflect.MessageDescriptor) (string, bool) {
	return protox.GetMessageExtensionValue(descriptor, parquetpb.E_TableName, "")
}

func IsFieldIgnored(field protoreflect.FieldDescriptor) bool {
	ignored, _ := protox.GetFieldExtensionValue(field, parquetpb.E_Ignored, false)
	return ignored
}

func GetFieldColumnType(field protoreflect.FieldDescriptor) (parquetpb.ColumnType, bool) {
	if columnDef, _ := protox.GetFieldExtensionValue(field, parquetpb.E_Column, (*pbparquet.Column)(nil)); columnDef.GetType() != pbparquet.ColumnType_UNSPECIFIED_COLUMN_TYPE {
		return *columnDef.Type, true
	}

	return parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE, false
}

var _ parquet.Node = (*messageNode)(nil)

type messageNode struct {
	descriptor protoreflect.MessageDescriptor
	fields     []parquet.Field
}

func newMessageNode(descriptor protoreflect.MessageDescriptor, defaultColumnCompression *pbparquet.Compression) *messageNode {
	fields := descriptor.Fields()
	parquetFields := make([]parquet.Field, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if IsFieldIgnored(field) {
			continue
		}

		parquetFields = append(parquetFields, toParquetField(field, defaultColumnCompression))
	}

	return &messageNode{
		descriptor: descriptor,
		fields:     parquetFields,
	}
}

func toParquetField(field protoreflect.FieldDescriptor, defaultColumnCompression *pbparquet.Compression) parquet.Field {
	columnDef, hasColumnDef := protox.GetFieldExtensionValue(field, parquetpb.E_Column, (*pbparquet.Column)(nil))

	node := protoFieldToParquetNode(field, columnDef)

	// If a compression is explicitly set on the column, it has precedence over the default column compression
	if hasColumnDef && columnDef.Compression != nil {
		if columnDef.GetCompression() != parquetpb.Compression_UNCOMPRESSED {
			node = parquet.Compressed(node, compressionToParquetCompressionCodec(columnDef.GetCompression()))
		}
	} else if defaultColumnCompression != nil && *defaultColumnCompression != pbparquet.Compression_UNCOMPRESSED {
		node = parquet.Compressed(node, compressionToParquetCompressionCodec(*defaultColumnCompression))
	}

	return &messageField{
		Node:      node,
		fieldName: string(field.Name()),
	}
}

func protoFieldToParquetNode(field protoreflect.FieldDescriptor, columnDef *pbparquet.Column) parquet.Node {
	if columnDef.GetType() != parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE {
		return columnTypeOptionToParquetNode(columnDef.GetType())
	}

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
		if protox.IsWellKnownTimestampField(field) {
			return parquet.Timestamp(parquet.Nanosecond)
		}

		panic(fmt.Errorf("field %s is of kind protoreflect.MessageKind which is not supported yet", field.FullName()))
	case protoreflect.GroupKind:
		panic(fmt.Errorf("field %s is of kind protoreflect.GroupKind which is not supported yet", field.FullName()))

	default:
		panic(fmt.Errorf("field %s is of kind %s which is not supported yet", field.FullName(), field.Kind()))
	}
}

func compressionToParquetCompressionCodec(compression parquetpb.Compression) compress.Codec {
	switch compression {
	case parquetpb.Compression_UNCOMPRESSED:
		return Uncompressed
	case parquetpb.Compression_SNAPPY:
		return &parquet.Snappy
	case parquetpb.Compression_GZIP:
		return &parquet.Gzip
	case parquetpb.Compression_LZ4_RAW:
		return &parquet.Lz4Raw
	case parquetpb.Compression_BROTLI:
		return &parquet.Brotli
	case parquetpb.Compression_ZSTD:
		return &parquet.Zstd
	default:
		panic(fmt.Errorf("compression %s is not supported yet", compression))
	}
}

func columnTypeOptionToParquetNode(columnType parquetpb.ColumnType) parquet.Node {
	switch columnType {
	case parquetpb.ColumnType_INT256:
		return parquet.Leaf(parquet.FixedLenByteArrayType(32))

	case parquetpb.ColumnType_UINT256:
		return parquet.Leaf(parquet.FixedLenByteArrayType(32))

	default:
		panic(fmt.Errorf("column type %s is not supported yet", columnType))
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
	s := new(strings.Builder)
	parquet.PrintSchema(s, "", m)
	return s.String()
}

// Type implements parquet.Node.
func (m *messageNode) Type() parquet.Type {
	return groupType{}
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
