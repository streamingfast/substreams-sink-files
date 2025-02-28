package parquetx

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
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
			for i := range fields.Len() {
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
						for i := range list.Len() {
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
					for i := range list.Len() {
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
			for i := range list.Len() {
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
