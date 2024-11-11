package parquetx

import (
	"encoding/binary"
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

				if IsFieldIgnored(field) {
					continue
				}

				if field.Kind() != protoreflect.MessageKind && !field.IsList() {
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

func ProtoMessageToRow(message protoreflect.Message) (parquet.Row, error) {
	messageDesc := message.Descriptor()
	fields := messageDesc.Fields()

	// We preallocate the columns slice to the maximum number of fields, but we might skip some
	// fields if they are ignored.
	columns := make([]parquet.Value, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if IsFieldIgnored(field) {
			continue
		}

		value := message.Get(field)

		if columnType, found := GetFieldColumnType(field); found && columnType != parquetpb.ColumnType_UNSPECIFIED_COLUMN_TYPE {
			column, err := protoValueToColumnTypeValue(columnType, field, value)
			if err != nil {
				return nil, fmt.Errorf("converting field %q to column value: %w", field.FullName(), err)
			}

			columns = append(columns, column)
			continue
		}

		switch field.Kind() {
		case protoreflect.BoolKind:
			columns = append(columns, parquet.BooleanValue(value.Bool()))
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			columns = append(columns, parquet.Int32Value(int32(value.Int())))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			columns = append(columns, parquet.Int64Value(value.Int()))
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			columns = append(columns, parquet.Int32Value(int32(value.Uint())))
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			columns = append(columns, parquet.Int64Value(int64(value.Uint())))
		case protoreflect.FloatKind:
			columns = append(columns, parquet.FloatValue(float32(value.Float())))
		case protoreflect.DoubleKind:
			columns = append(columns, parquet.DoubleValue(value.Float()))
		case protoreflect.StringKind:
			columns = append(columns, parquet.ByteArrayValue([]byte(value.String())))
		case protoreflect.BytesKind:
			columns = append(columns, parquet.ByteArrayValue(value.Bytes()))

		case protoreflect.MessageKind:
			if protox.IsWellKnownTimestampField(field) {
				columns = append(columns, parquet.Int64Value(protox.DynamicAsTimestampTime(value.Message()).UnixNano()))
				continue
			}

			columns = append(columns, parquet.ValueOf(value.Interface()))

		default:
			return nil, fmt.Errorf("field %s is of type %s which isn't supported yet", field.FullName(), field.Kind())
		}
	}

	return parquet.Row(columns), nil
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

		// Seems we need to be in little endian, number.Bytes32() doesn't work, is it a problem with Parquet writer?
		data := make([]byte, 32)
		binary.LittleEndian.PutUint64(data[0:8], number[0])
		binary.LittleEndian.PutUint64(data[8:16], number[1])
		binary.LittleEndian.PutUint64(data[16:24], number[2])
		binary.LittleEndian.PutUint64(data[24:32], number[3])

		// We **must** use a []byte of exactly 32 bytes, otherwise the parquet writer skip the row.
		// See https://github.com/parquet-go/parquet-go/issues/178
		return parquet.FixedLenByteArrayValue(data), nil

	default:
		return out, fmt.Errorf("unsupported conversion from field kind %s to column value of type %s", field.Kind(), parquetpb.ColumnType_UINT256)
	}
}
