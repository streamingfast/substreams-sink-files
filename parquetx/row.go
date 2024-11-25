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

type ProtoRowExtractor interface {
	// ExtractRows extracts rows from the given message by starting at the root message.
	// The extractor is constructed while inspecting the message descriptor, so it knows
	// the path how to extract the rows.
	ExtractRows(root protoreflect.Message) (map[string][]parquet.Row, error)
}

type protoRowExtractorFunc func(root protoreflect.Message) (map[string][]parquet.Row, error)

func (f protoRowExtractorFunc) ExtractRows(root protoreflect.Message) (map[string][]parquet.Row, error) {
	return f(root)
}

func ProtoRowExtractorFromTables(tables []TableResult) ProtoRowExtractor {
	// FIXME: This is relatively inefficient as we are traversing some message fields that
	// are dead end and will not yield any rows. We need to change that by first walking
	// the message and building a list of every "paths" to field (deeply nested too + list
	// handling) that will collect all paths that must be followed to extract rows.
	//
	// Once we have that, we can stop naively walking the message and instead only follows
	// paths with the Message value that will yield rows.

	tablesByMessage := make(map[protoreflect.FullName]*parquet.Schema, len(tables))
	for _, table := range tables {
		tablesByMessage[table.Descriptor.FullName()] = table.Schema
	}

	return protoRowExtractorFunc(func(root protoreflect.Message) (out map[string][]parquet.Row, err error) {
		out = make(map[string][]parquet.Row)

		var processNode func(node protoreflect.Message) error
		processNode = func(node protoreflect.Message) error {
			if tracer.Enabled() {
				zlog.Debug("walking value", zap.String("message", string(node.Descriptor().FullName())))
			}

			fields := node.Descriptor().Fields()
			for i := 0; i < fields.Len(); i++ {
				field := fields.Get(i)
				ignored := IsFieldIgnored(field)

				if tracer.Enabled() {
					zlog.Debug("value field descriptor",
						zap.String("field", string(field.FullName())),
						zap.String("kind", field.Kind().String()),
						zap.Bool("is_list", field.IsList()),
						zap.Bool("ignored", ignored),
					)
				}

				if ignored {
					continue
				}

				if field.Kind() != protoreflect.MessageKind && !field.IsList() {
					if tracer.Enabled() {
						zlog.Debug("skipping field descriptor, not message kind and not list type", zap.String("field", string(field.FullName())))
					}
					continue
				}

				fieldMessageDescriptor := field.Message()
				if fieldMessageDescriptor == nil {
					// This happens on primitive repeated fields which we do not walk
					continue
				}

				schema, found := tablesByMessage[fieldMessageDescriptor.FullName()]
				if found {
					// If it's a repeated field, we need to iterate over the list
					if field.IsList() {
						list := node.Get(field).List()
						rows := make([]parquet.Row, list.Len())
						for i := 0; i < list.Len(); i++ {
							row, err := ProtoMessageToRow(list.Get(i).Message())
							if err != nil {
								return fmt.Errorf("converting repeated field %q index %d to row: %w", field.FullName(), i, err)
							}

							rows[i] = row
						}

						out[schema.Name()] = append(out[schema.Name()], rows...)
						continue
					}

					row, err := ProtoMessageToRow(node.Get(field).Message())
					if err != nil {
						return fmt.Errorf("converting field %q to row: %w", field.FullName(), err)
					}

					out[schema.Name()] = append(out[schema.Name()], row)
					continue
				}

				if field.IsList() {
					list := node.Get(field).List()
					for i := 0; i < list.Len(); i++ {
						// Recurse into the message if message was not found in the tables in is not a list
						if err := processNode(list.Get(i).Message()); err != nil {
							// Propagate the error up
							return err
						}
					}
				} else {
					// Recurse into the message if message was not found in the tables in is not a list
					if err := processNode(node.Get(field).Message()); err != nil {
						// Propagate the error up
						return err
					}
				}
			}

			return nil
		}

		// Maybe the root message is a table itself, so we need to process it
		rootDescriptor := root.Descriptor()
		if schema, found := tablesByMessage[rootDescriptor.FullName()]; found {
			row, err := ProtoMessageToRow(root)
			if err != nil {
				return nil, fmt.Errorf("converting message %q to row: %w", rootDescriptor.FullName(), err)
			}

			out[schema.Name()] = append(out[schema.Name()], row)
		}

		if err := processNode(root); err != nil {
			return nil, err
		}

		return
	})
}

func ProtoRowExtractorFromRoot(tableName string) ProtoRowExtractor {
	return protoRowExtractorFunc(func(root protoreflect.Message) (map[string][]parquet.Row, error) {
		row, err := ProtoMessageToRow(root)
		if err != nil {
			return nil, fmt.Errorf("converting root message %q to row: %w", root.Descriptor().FullName(), err)
		}

		return map[string][]parquet.Row{
			tableName: {row},
		}, nil
	})
}

func ProtoRowExtractorFromRepeatedFields(fieldByTableName map[string]protoreflect.FieldDescriptor) ProtoRowExtractor {
	for _, field := range fieldByTableName {
		if !field.IsList() {
			panic(fmt.Errorf("field %s is not a list", field.FullName()))
		}
	}

	return protoRowExtractorFunc(func(root protoreflect.Message) (out map[string][]parquet.Row, err error) {
		out = make(map[string][]parquet.Row)

		for tableName, field := range fieldByTableName {
			list := root.Get(field).List()

			rows := make([]parquet.Row, list.Len())
			for i := 0; i < list.Len(); i++ {
				row, err := ProtoMessageToRow(list.Get(i).Message())
				if err != nil {
					return nil, fmt.Errorf("converting repeated field %q index %d to row: %w", field.FullName(), i, err)
				}

				rows[i] = row
			}

			out[tableName] = rows
		}

		return
	})
}

// ProtoMessageToRow converts a protobuf message to a parquet row, handling repeated single level fields
// for now.
//
// At term, this will handle nested messages and repeated fields of any depth.
func ProtoMessageToRow(message protoreflect.Message) (parquet.Row, error) {
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

	messageDesc := message.Descriptor()
	fields := messageDesc.Fields()

	// We preallocate the columns slice to the maximum number of fields, but we might skip some
	// fields if they are ignored. Also, repeated fields will be turned into multiple parquet.Value
	// sequentially, so we might have much more values than columns in those case.
	columns := make([]parquet.Value, 0, fields.Len())

	columnIndex := 0
	addColumn := func(values ...parquet.Value) {
		columns = append(columns, values...)
		columnIndex++
	}

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if IsFieldIgnored(field) {
			continue
		}

		value := message.Get(field)

		if columnType, found := GetFieldColumnType(field); found && columnType != parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE {
			value, err := protoValueToColumnTypeValue(columnType, field, value)
			if err != nil {
				return nil, fmt.Errorf("converting field %q to column value: %w", field.FullName(), err)
			}

			addColumn(value)
			continue
		}

		if field.IsList() {
			fieldList := value.List()
			if fieldList.Len() == 0 {
				addColumn(parquet.NullValue().Level(0, 0, columnIndex))
			} else {
				sliceValues := make([]parquet.Value, fieldList.Len())
				for i := 0; i < fieldList.Len(); i++ {
					sliceValue, err := protoLeafToValue(field, fieldList.Get(i))
					if err != nil {
						return nil, fmt.Errorf("converting leaf field %s @ index %d: %w", field.FullName(), i, err)
					}

					repetitionLevel := i
					if repetitionLevel != 0 {
						repetitionLevel = 1
					}

					// FIXME: Handle nested repeated fields here where definition level would be
					// something else than 1.
					sliceValues[i] = sliceValue.Level(repetitionLevel, 1, columnIndex)
				}

				addColumn(sliceValues...)
			}

			continue
		}

		leafValue, err := protoLeafToValue(field, value)
		if err != nil {
			return nil, fmt.Errorf("converting leaf field %s: %w", field.FullName(), err)
		}

		addColumn(leafValue)
		continue
	}

	return parquet.Row(columns), nil
}

func protoLeafToValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (out parquet.Value, err error) {
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
