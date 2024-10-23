package parquetx

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/streamingfast/substreams-sink-files/protox"
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
	tablesByMessage := make(map[protoreflect.FullName]*parquet.Schema, len(tables))
	for _, table := range tables {
		tablesByMessage[table.Descriptor.FullName()] = table.Schema
	}

	return protoRowExtractorFunc(func(root protoreflect.Message) (out map[string][]parquet.Row, err error) {
		out = make(map[string][]parquet.Row)

		var processNode func(node protoreflect.Message) error
		processNode = func(node protoreflect.Message) error {
			fields := node.Descriptor().Fields()
			for i := 0; i < fields.Len(); i++ {
				field := fields.Get(i)
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

				// Recurse into the message if message was not found in the tables in is not a list
				if err := processNode(node.Get(field).Message()); err != nil {
					// Propagate the error up
					return err
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
