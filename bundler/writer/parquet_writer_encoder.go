package writer

import (
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/streamingfast/substreams-sink-files/protox"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func (p *ParquetWriter) EncodeMapModule(output *pbsubstreamsrpc.MapModuleOutput) error {
	messageFullName := strings.TrimPrefix(output.MapOutput.TypeUrl, "type.googleapis.com/")
	if messageFullName != string(p.descriptor.FullName()) {
		return fmt.Errorf("received message type URL %q doesn't match expected output type %q", messageFullName, p.descriptor.FullName())
	}

	dynamicMsg := dynamicpb.NewMessage(p.descriptor)
	err := proto.Unmarshal(output.MapOutput.Value, dynamicMsg)
	if err != nil {
		return fmt.Errorf("unmarshal message as proto: %w", err)
	}

	listField, err := findParquetListField(dynamicMsg, p.descriptor)
	if err != nil {
		return fmt.Errorf("find parquet list field: %w", err)
	}

	for i := 0; i < listField.Len(); i++ {
		protoValue := listField.Get(i)

		if err := p.AppendDynamicMessage(protoValue.Message()); err != nil {
			return fmt.Errorf("append message to writer")
		}
	}

	return nil
}

func findParquetListField(dynamicMsg *dynamicpb.Message, descriptor protoreflect.MessageDescriptor) (protoreflect.List, error) {
	firstRepeatedField := protox.FindMessageFirstRepeatedField(descriptor)
	if firstRepeatedField == nil {
		return nil, fmt.Errorf("no repeated field found in Protobuf model")
	}

	return dynamicMsg.Get(firstRepeatedField).List(), nil
}

func (p *ParquetWriter) AppendDynamicMessage(message protoreflect.Message) error {
	if p.activeRange == nil {
		return fmt.Errorf("no boundary active")
	}

	row, err := p.toParquetRow(message)
	if err != nil {
		return fmt.Errorf("unable to transform message to row")
	}

	p.rowBuffer.WriteRows([]parquet.Row{row})

	return nil
}

func (p *ParquetWriter) toParquetRow(message protoreflect.Message) (parquet.Row, error) {
	messageDesc := message.Descriptor()
	fields := messageDesc.Fields()

	columns := make([]parquet.Value, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		value := message.Get(field)

		switch field.Kind() {
		case protoreflect.BoolKind:
			columns[i] = parquet.BooleanValue(value.Bool())
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			columns[i] = parquet.Int32Value(int32(value.Int()))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			columns[i] = parquet.Int64Value(value.Int())
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			columns[i] = parquet.Int32Value(int32(value.Uint()))
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			columns[i] = parquet.Int64Value(int64(value.Uint()))
		case protoreflect.FloatKind:
			columns[i] = parquet.FloatValue(float32(value.Float()))
		case protoreflect.DoubleKind:
			columns[i] = parquet.DoubleValue(value.Float())
		case protoreflect.StringKind:
			columns[i] = parquet.ByteArrayValue([]byte(value.String()))
		case protoreflect.BytesKind:
			columns[i] = parquet.ByteArrayValue(value.Bytes())

		case protoreflect.MessageKind:
			if protox.IsWellKnownTimestampField(field) {
				columns[i] = parquet.Int64Value(protox.DynamicAsTimestampTime(value.Message()).UnixNano())
				continue
			}

			columns[i] = parquet.ValueOf(value.Interface())

		default:
			return nil, fmt.Errorf("field %s is of type %s which isn't supported yet", field.FullName(), field.Kind())
		}
	}

	return parquet.Row(columns), nil
}
