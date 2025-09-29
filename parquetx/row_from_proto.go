package parquetx

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/holiman/uint256"
	"github.com/parquet-go/parquet-go"
	parquetpb "github.com/streamingfast/substreams-sink-files/pb/parquet"
	"github.com/streamingfast/substreams-sink-files/protox"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProtoMessageToRow converts a protobuf message to a parquet row, handling repeated single level fields
// for now.
//
// At term, this will handle nested messages and repeated fields of any depth.
func ProtoMessageToRow(message protoreflect.Message) (parquet.Row, error) {
	recursionCtx := newRecursionContext(message, zlog, tracer)
	totalColumns := messageLeafColumnCount(message.Descriptor())

	if tracer.Enabled() {
		logger := zlog.With(
			zap.String("path", string(message.Descriptor().FullName())),
		)

		logger.Debug("processing root message")
		defer func() {
			logger.Debug("root message processed", zap.Int("column_count", totalColumns))
		}()
	}

	// To better understand how we turn a protobuf message into a parquet.Row which
	// is a slice of parquet.Value, you will need to understand the Dremel encoding
	// used by Parquet to store repeated and nested group into a flat columnar format.
	//
	// The Dremel encoding is based on the concept of repetition and definition levels.
	// The repetition level is the number of repeated fields between the current field
	// and the root of the repeated field. The definition level is the number of optional
	// fields between the current field and the root of the optional field.
	//
	// See https://blog.x.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
	// for more information about the Dremel encoding.

	values, err := protoMessageToValues(recursionCtx, message, 0)
	if err != nil {
		return nil, fmt.Errorf("root message: %w", err)
	}

	columns := make([][]parquet.Value, totalColumns)
	for _, value := range values {
		columnIndex := value.Column()
		if columnIndex < 0 || columnIndex >= totalColumns {
			return nil, fmt.Errorf("value column index %d is outside of total column count %d", columnIndex, totalColumns)
		}

		columns[columnIndex] = append(columns[columnIndex], value)
	}

	if tracer.Enabled() {
		logger := zlog.With(zap.String("path", string(message.Descriptor().FullName())))
		for columnIndex, columnValues := range columns {
			for i, value := range columnValues {
				logger.Debug("column value",
					zap.Int("column_index", columnIndex),
					zap.Int("value_index", i),
					zap.Int("repetition_level", value.RepetitionLevel()),
					zap.Int("definition_level", value.DefinitionLevel()),
					zap.Bool("is_null", value.IsNull()),
				)
			}
		}
	}

	return parquet.MakeRow(columns...), nil
}

func protoMessageToValues(recursionCtx *recursionContext, message protoreflect.Message, baseColumnIndex int) (out []parquet.Value, err error) {
	if tracer.Enabled() {
		logger := zlog.With(zap.String("path", recursionCtx.Path()))
		logger.Debug("processing message", zap.Int("base_column_index", baseColumnIndex))
		defer func() { logger.Debug("message processed", zap.Int("value_count", len(out))) }()
	}

	fields := message.Descriptor().Fields()
	columnOffset := 0

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if IsFieldIgnored(field) {
			continue
		}

		fieldBase := baseColumnIndex + columnOffset
		values, err := protoFieldToValues(recursionCtx, message, field, fieldBase)
		if err != nil {
			return nil, fmt.Errorf("message: %w", err)
		}

		if len(values) > 0 {
			out = append(out, values...)
		}
		columnOffset += fieldLeafColumnCount(field)
	}

	return out, nil
}

func messageLeafColumnCount(desc protoreflect.MessageDescriptor) int {
	total := 0
	for field := range protox.WalkMessageFields(desc, zlog, tracer, IsFieldIgnored) {
		if isLeafField(field) {
			total++
		}
	}

	return total
}

func fieldLeafColumnCount(field protoreflect.FieldDescriptor) int {
	switch {
	case IsFieldIgnored(field):
		return 0
	case isLeafField(field):
		return 1
	}

	return messageLeafColumnCount(field.Message())
}

func isLeafField(field protoreflect.FieldDescriptor) bool {
	columnType, _ := GetFieldColumnType(field)

	switch {
	case columnType != parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE:
		return true
	case protox.IsWellKnownTimestampField(field):
		return true
	case field.Kind() != protoreflect.MessageKind || protox.IsWellKnownGoogleField(field):
		// FIXME: This is not really handled properly, will fail later
		return true
	}

	return false
}

func forEachLeafColumnIndex(field protoreflect.FieldDescriptor, baseColumnIndex int, fn func(int)) {
	if IsFieldIgnored(field) {
		return
	}

	if columnType, ok := GetFieldColumnType(field); ok && columnType != parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE {
		fn(baseColumnIndex)
		return
	}

	if protox.IsWellKnownTimestampField(field) || field.Kind() != protoreflect.MessageKind || protox.IsWellKnownGoogleField(field) {
		fn(baseColumnIndex)
		return
	}

	nested := field.Message().Fields()
	offset := 0
	for i := 0; i < nested.Len(); i++ {
		nestedField := nested.Get(i)
		if IsFieldIgnored(nestedField) {
			continue
		}
		leafCount := fieldLeafColumnCount(nestedField)
		if leafCount == 0 {
			continue
		}
		forEachLeafColumnIndex(nestedField, baseColumnIndex+offset, fn)
		offset += leafCount
	}

	if offset == 0 {
		fn(baseColumnIndex)
	}
}

func appendNullLeafValues(recursionCtx *recursionContext, field protoreflect.FieldDescriptor, baseColumnIndex int, out *[]parquet.Value) {
	leafCount := fieldLeafColumnCount(field)
	if leafCount == 0 {
		return
	}

	forEachLeafColumnIndex(field, baseColumnIndex, func(columnIndex int) {
		*out = append(*out, recursionCtx.NullValue(columnIndex))
	})
}

func protoFieldToValues(recursionCtx *recursionContext, message protoreflect.Message, field protoreflect.FieldDescriptor, baseColumnIndex int) (out []parquet.Value, err error) {
	if tracer.Enabled() {
		logger := zlog.With(
			zap.String("path", recursionCtx.FieldPath(field)),
		)

		logger.Debug("processing field",
			zap.Stringer("kind", field.Kind()),
			zap.Bool("is_list", field.IsList()),
			zap.Bool("is_optional", field.HasOptionalKeyword()),
			zap.Bool("is_nested_message", field.Kind() == protoreflect.MessageKind && !protox.IsWellKnownGoogleField(field)),
			zap.Int("base_column_index", baseColumnIndex),
		)
		defer func() { logger.Debug("field processed", zap.Int("value_count", len(out))) }()
	}

	if IsFieldIgnored(field) {
		return nil, nil
	}

	columnType, hasColumnType := GetFieldColumnType(field)
	if hasColumnType && columnType == parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE {
		hasColumnType = false
	}

	fieldValue := message.Get(field)

	if field.IsList() {
		fieldList := fieldValue.List()

		if fieldList.Len() == 0 {
			appendNullLeafValues(recursionCtx, field, baseColumnIndex, &out)
			return out, nil
		}

		recursionCtx.StartRepeated(fieldList.Len())
		perElementColumns := fieldLeafColumnCount(field)
		if perElementColumns <= 0 {
			perElementColumns = 1
		}

		out = make([]parquet.Value, 0, fieldList.Len()*perElementColumns)

		for i := 0; i < fieldList.Len(); i++ {
			if field.Kind() == protoreflect.MessageKind && !protox.IsWellKnownGoogleField(field) && !protox.IsWellKnownTimestampField(field) {
				nestedMessage := fieldList.Get(i).Message()

				recursionCtx.EnterRepeatedNested(fmt.Sprintf("%s[%d]", field.Name(), i), nestedMessage)
				values, err := protoMessageToValues(recursionCtx, nestedMessage, baseColumnIndex)
				recursionCtx.ExitRepeatedNested()

				if err != nil {
					return nil, fmt.Errorf("list message type @ index %d: %w", i, err)
				}

				out = append(out, values...)
			} else {
				element := fieldList.Get(i)
				var value parquet.Value
				if hasColumnType {
					// This handles custom column types like `string amount = 1 [(parquet.column) = {type: UINT256}];`.
					// Those today are always leaf fields, so we can safely convert them to a column value directly.

					value, err = protoValueToColumnTypeValue(columnType, field, element)
					if err != nil {
						return nil, fmt.Errorf("list column type @ index %d: %w", i, err)
					}
					out = append(out, recursionCtx.Level(value, baseColumnIndex))
				} else {
					value, err = protoLeafToValue(field, element, recursionCtx, baseColumnIndex)
					if err != nil {
						return nil, fmt.Errorf("list leaf type @ index %d: %w", i, err)
					}
					out = append(out, value)
				}
			}

			recursionCtx.RepeatedIterationCompleted(i)
		}

		return out, nil
	}

	if field.HasOptionalKeyword() {
		if !message.Has(field) {
			appendNullLeafValues(recursionCtx, field, baseColumnIndex, &out)
			return out, nil
		}

		recursionCtx.EnterOptional()
		defer recursionCtx.ExitOptional()
	}

	if field.Kind() == protoreflect.MessageKind && !protox.IsWellKnownGoogleField(field) && !protox.IsWellKnownTimestampField(field) {
		if !message.Has(field) {
			appendNullLeafValues(recursionCtx, field, baseColumnIndex, &out)
			return out, nil
		}

		nestedMessage := fieldValue.Message()

		recursionCtx.EnterNested(string(field.Name()), nestedMessage)
		nestedRows, err := protoMessageToValues(recursionCtx, nestedMessage, baseColumnIndex)
		recursionCtx.ExitNested()

		if err != nil {
			return nil, fmt.Errorf("nested message to value: %w", err)
		}

		return nestedRows, nil
	}

	if hasColumnType {
		value, err := protoValueToColumnTypeValue(columnType, field, fieldValue)
		if err != nil {
			return nil, fmt.Errorf("column type to value: %w", err)
		}

		return []parquet.Value{recursionCtx.Level(value, baseColumnIndex)}, nil
	}

	leafValue, err := protoLeafToValue(field, fieldValue, recursionCtx, baseColumnIndex)
	if err != nil {
		return nil, fmt.Errorf("leaf to value: %w", err)
	}

	return []parquet.Value{leafValue}, nil
}

// valueLeveler is a simple interface that knows how to set the repetition and definition levels
// of a parquet.Value. It's provided by the [parentStack] struct when recursing into nested
// fields and repeated fields.
type valueLeveler interface {
	// Level returns the value with the correct repetition and definition levels set for a specific column index.
	Level(value parquet.Value, columnIndex int) parquet.Value

	// NullValue returns a null value with the correct repetition and definition levels set for a specific column index.
	NullValue(columnIndex int) parquet.Value
}

func protoLeafToValue(field protoreflect.FieldDescriptor, value protoreflect.Value, leveler valueLeveler, columnIndex int) (out parquet.Value, err error) {
	defer func() {
		out = leveler.Level(out, columnIndex)
	}()

	switch field.Kind() {
	case protoreflect.BoolKind:
		return parquet.BooleanValue(value.Bool()), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return parquet.Int32Value(int32(value.Int())), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return parquet.Int64Value(value.Int()), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return parquet.Int32Value(int32(value.Uint())), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return parquet.Int64Value(int64(value.Uint())), nil
	case protoreflect.FloatKind:
		return parquet.FloatValue(float32(value.Float())), nil
	case protoreflect.DoubleKind:
		return parquet.DoubleValue(value.Float()), nil
	case protoreflect.StringKind:
		return parquet.ByteArrayValue([]byte(value.String())), nil
	case protoreflect.BytesKind:
		return parquet.ByteArrayValue(value.Bytes()), nil
	case protoreflect.MessageKind:
		if protox.IsWellKnownTimestampField(field) {
			return parquet.Int64Value(protox.DynamicAsTimestampTime(value.Message()).UnixNano()), nil
		}
	case protoreflect.EnumKind:
		enumField := field.Enum()
		enumNumber := value.Enum()

		valueDescriptor := enumField.Values().ByNumber(enumNumber)
		if valueDescriptor == nil {
			return out, fmt.Errorf("enum value %d is not a valid enumeration value for field '%s', known enum values are [%s]", enumNumber, field.Name(), protox.EnumKnownValuesDebugString(enumField))
		}
		return parquet.ByteArrayValue([]byte(protox.EnumValueToString(valueDescriptor))), nil
	}

	return out, fmt.Errorf("type %s isn't supported yet as a leaf node", field.Kind())
}

func protoValueToColumnTypeValue(columnType parquetpb.ColumnType, field protoreflect.FieldDescriptor, value protoreflect.Value) (out parquet.Value, err error) {
	switch columnType {
	case parquetpb.ColumnType_INT256:
		return columnTypeInt256ToParquetValue(field, value)

	case parquetpb.ColumnType_UINT256:
		return columnTypeUint256ToParquetValue(field, value)

	default:
		return out, fmt.Errorf("unsupported column type %s", columnType)
	}
}

func columnTypeInt256ToParquetValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (out parquet.Value, err error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		stringValue := value.String()

		number, ok := new(big.Int).SetString(stringValue, 0)
		if !ok {
			return out, fmt.Errorf("converting string %q to big.Int", stringValue)
		}

		if number.BitLen() > 256 {
			return out, fmt.Errorf("converting string %q to big.Int: number is too large", stringValue)
		}

		// FIXME: Deal with negative numbers vs positive numbers correctly!
		data := make([]byte, 32)
		number.FillBytes(data)

		// We **must** use a []byte of exactly 32 bytes, otherwise the parquet writer skip the row.
		return parquet.FixedLenByteArrayValue(data), nil

	default:
		return out, fmt.Errorf("unsupported conversion from field kind %s to column value of type %s", field.Kind(), parquetpb.ColumnType_UINT256)
	}
}

func columnTypeUint256ToParquetValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (out parquet.Value, err error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		stringValue := value.String()

		var number *uint256.Int
		if strings.HasPrefix(stringValue, "0x") {
			number, err = uint256.FromHex(stringValue)
			if err != nil {
				return out, fmt.Errorf("converting hex string %q to uint256: %w", stringValue, err)
			}
		} else {
			number, err = uint256.FromDecimal(stringValue)
			if err != nil {
				return out, fmt.Errorf("converting decimal string %q to uint256: %w", stringValue, err)
			}
		}

		// We **must** use a []byte of exactly 32 bytes, otherwise the parquet writer skip the row.
		// See https://github.com/parquet-go/parquet-go/issues/178

		// So further research leads to fact that different reader of Parquet will read the data in
		// different format. It seems also that physical/logical types affects the way the data is read.
		//
		// Clickhouse on Physical: FixedByte(32) and Logical: Decimal(N, 0) expects the data to be in big-endian format.
		// Clickhouse on Physical: FixedByte(32) and Logical: None expects the data to be in little-endian format.
		//
		// The more natural way would be to use big-endian format, keep little endian for now until
		// further research is done.
		// unsafe := (*[32]byte)(unsafe.Pointer(number))
		// data := *unsafe

		// Big-endian version
		data := number.Bytes32()

		return parquet.FixedLenByteArrayValue(data[:]), nil

	default:
		return out, fmt.Errorf("unsupported conversion from field kind %s to column value of type %s", field.Kind(), parquetpb.ColumnType_UINT256)
	}
}
