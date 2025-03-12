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
func ProtoMessageToRow(message protoreflect.Message) (row parquet.Row, err error) {
	recursionCtx := newRecursionContext(message, zlog, tracer)

	if tracer.Enabled() {
		logger := zlog.With(
			zap.String("path", string(message.Descriptor().FullName())),
		)

		logger.Debug("processing root message")
		defer func() {
			logger.Debug("root message processed", zap.Int16("column_count", recursionCtx.columnIndex-1), zap.Int("value_count", len(row)))
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

	var columns []parquet.Value

	addColumn := func(values ...parquet.Value) {
		if tracer.Enabled() {
			zlog.Debug("adding column values", zap.Int("column_index", int(recursionCtx.columnIndex)), zap.Int("value_count", len(values)))
		}

		columns = append(columns, values...)
		recursionCtx.columnIndex++
	}

	messageDesc := message.Descriptor()
	fields := messageDesc.Fields()

	for i := range fields.Len() {
		values, err := protoFieldToValues(recursionCtx, message, fields.Get(i))
		if err != nil {
			return nil, fmt.Errorf("root message: %w", err)
		}

		// If the field is ignored, there will be no values to add, so we can skip it.
		if len(values) == 0 {
			continue
		}

		addColumn(values...)
	}

	return parquet.Row(columns), nil
}

func protoMessageToValues(recursionCtx *recursionContext, message protoreflect.Message) (out []parquet.Value, err error) {
	if tracer.Enabled() {
		logger := zlog.With(zap.String("path", recursionCtx.Path()))
		logger.Debug("processing message")
		defer func() { logger.Debug("message processed", zap.Int("value_count", len(out))) }()
	}

	messageDesc := message.Descriptor()
	fields := messageDesc.Fields()

	for i := range fields.Len() {
		values, err := protoFieldToValues(recursionCtx, message, fields.Get(i))
		if err != nil {
			return nil, fmt.Errorf("message: %w", err)
		}

		if values != nil {
			out = append(out, values...)
		}
	}

	return out, nil
}

func protoFieldToValues(recursionCtx *recursionContext, message protoreflect.Message, field protoreflect.FieldDescriptor) (out []parquet.Value, err error) {
	if tracer.Enabled() {
		logger := zlog.With(
			zap.String("path", recursionCtx.FieldPath(field)),
		)

		logger.Debug("processing field",
			zap.Stringer("kind", field.Kind()),
			zap.Bool("is_list", field.IsList()),
			zap.Bool("is_optional", field.HasOptionalKeyword()),
			zap.Bool("is_nested_message", field.Kind() == protoreflect.MessageKind && !protox.IsWellKnownGoogleField(field)),
		)
		defer func() { logger.Debug("field processed", zap.Int("value_count", len(out))) }()
	}

	if IsFieldIgnored(field) {
		return nil, nil
	}

	value := message.Get(field)

	// This handles custom column types like `string amount = 1 [(parquet.column) = {type: UINT256}];`.
	// Those today are always leaf fields, so we can safely convert them to a column value directly.
	if columnType, found := GetFieldColumnType(field); found && columnType != parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE {
		value, err := protoValueToColumnTypeValue(columnType, field, value)
		if err != nil {
			return nil, fmt.Errorf("column type to value: %w", err)
		}

		return []parquet.Value{value}, nil
	}

	if field.IsList() {
		fieldList := value.List()

		if fieldList.Len() == 0 {
			return []parquet.Value{recursionCtx.NullValue()}, nil
		}

		out := make([]parquet.Value, 0, fieldList.Len())
		recursionCtx.StartRepeated(fieldList.Len())

		for i := range fieldList.Len() {
			if field.Kind() == protoreflect.MessageKind && !protox.IsWellKnownGoogleField(field) {
				nestedMessage := fieldList.Get(i).Message()

				recursionCtx.EnterRepeatedNested(fmt.Sprintf("%s[%d]", field.Name(), i), nestedMessage)
				values, err := protoMessageToValues(recursionCtx, nestedMessage)
				recursionCtx.ExitRepeatedNested()

				if err != nil {
					return nil, fmt.Errorf("list message type @ index %d: %w", i, err)
				}

				out = append(out, values...)
			} else {
				sliceValue, err := protoLeafToValue(field, fieldList.Get(i), recursionCtx)
				if err != nil {
					return nil, fmt.Errorf("list leaf type @ index %d: %w", i, err)
				}

				out = append(out, sliceValue)
			}

			recursionCtx.RepeatedIterationCompleted(i)
		}

		return out, nil
	}

	// Optional field behaves differently, around the definition level, when the field is actually
	// set or not present at all than a regular required field. If a field is optional, is definition level
	// is 1 when the field is present, and 0 when it's not (true only for non-nested fields, nested fields
	// can go higher than 1).
	//
	// Failing to adjust the definition level correctly when the field is optional but present leads to
	// the data not being populated correctly.
	//
	// FIXME: In presence of nested optional fields of type message, what is the order in which optional
	// should be treated compared to nested fields and repeated fields?
	if field.HasOptionalKeyword() {
		if !message.Has(field) {
			return []parquet.Value{recursionCtx.NullValue()}, nil
		}

		recursionCtx.EnterOptional()
		leafValue, err := protoLeafToValue(field, value, recursionCtx)
		recursionCtx.ExitOptional()

		if err != nil {
			return nil, fmt.Errorf("optional leaf to value: %w", err)
		}

		return []parquet.Value{leafValue}, nil
	}

	if field.Kind() == protoreflect.MessageKind && !protox.IsWellKnownGoogleField(field) {
		nestedMessage := value.Message()

		recursionCtx.EnterNested(string(field.Name()), nestedMessage)
		nestedRows, err := protoMessageToValues(recursionCtx, nestedMessage)
		recursionCtx.ExitNested()

		if err != nil {
			return nil, fmt.Errorf("nested message to value: %w", err)
		}

		return nestedRows, nil
	}

	leafValue, err := protoLeafToValue(field, value, recursionCtx)
	if err != nil {
		return nil, fmt.Errorf("leaf to value: %w", err)
	}

	return []parquet.Value{leafValue}, nil
}

// valueLeveler is a simple interface that knows how to set the repetition and definition levels
// of a parquet.Value. It's provided by the [parentStack] struct when recursing into nested
// fields and repeated fields.
type valueLeveler interface {
	// Level returns the value with the correct repetition and definition levels set.
	Level(value parquet.Value) parquet.Value

	// NullValue returns a null value with the correct repetition and definition levels set.
	NullValue() parquet.Value
}

func protoLeafToValue(field protoreflect.FieldDescriptor, value protoreflect.Value, leveler valueLeveler) (out parquet.Value, err error) {
	defer func() {
		// It's safe to call Level in all cases even if there is an error
		out = leveler.Level(out)
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
