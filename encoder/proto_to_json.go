package encoder

import (
	"encoding/json"
	"fmt"

	// We use the deprecated version on purpose!
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"github.com/streamingfast/substreams-sink-files/pq"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

type ProtoToJson struct {
	querier          *pq.Query
	outputModuleDesc *desc.MessageDescriptor
}

func NewProtoToJson(fiedPath string, outputModuleDesc *desc.MessageDescriptor) (*ProtoToJson, error) {
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
		err := protoToJson(proto.Message(entity), writer)
		if err != nil {
			return fmt.Errorf("encode entity at index %d: %w", idx, err)
		}
	}

	return nil
}

func protoToJson(message proto.Message, writer writer.Writer) error {
	// The `NewEncoder(writer).Encode(message)` automatically adds "\n" at the end
	if err := json.NewEncoder(writer).Encode(message); err != nil {
		return fmt.Errorf("json encoder: %w", err)
	}

	return nil
}
