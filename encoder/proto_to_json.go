package encoder

import (
	"fmt"

	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"github.com/streamingfast/substreams-sink-files/pq"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type ProtoToJson struct {
	querier          *pq.Query
	outputModuleDesc protoreflect.MessageDescriptor
}

func NewProtoToJson(fiedPath string, outputModuleDesc protoreflect.MessageDescriptor) (*ProtoToJson, error) {
	entitiesQuery, err := pq.Parse(fiedPath)
	if err != nil {
		return nil, fmt.Errorf("parse entities path %q: %w", fiedPath, err)
	}

	return &ProtoToJson{querier: entitiesQuery, outputModuleDesc: outputModuleDesc}, nil
}

func (p *ProtoToJson) EncodeTo(output *pbsubstreamsrpc.MapModuleOutput, writer writer.Writer) error {
	entities, err := p.querier.Resolve(output.GetMapOutput().GetValue(), p.outputModuleDesc)
	if err != nil {
		return fmt.Errorf("failed to resolve entities query: %w", err)
	}
	for idx, entity := range entities {
		err := protoToJson(entity, writer)
		if err != nil {
			return fmt.Errorf("encode entity at index %d: %w", idx, err)
		}
	}

	return nil
}

func protoToJson(message *dynamicpb.Message, writer writer.Writer) error {
	// Directly use protojson.Marshal without any conversion
	out, err := protojson.Marshal(message)
	if err != nil {
		return fmt.Errorf("protojson marshal: %w", err)
	}

	// Write the JSON bytes followed by newline
	if _, err := writer.Write(out); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	if _, err := writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("write newline: %w", err)
	}

	return nil
}
