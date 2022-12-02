package encoder

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/pq"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
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

func (p *ProtoToJson) Encode(output *pbsubstreams.ModuleOutput) ([]byte, error) {
	entities, err := p.querier.Resolve(output.GetMapOutput().GetValue(), p.outputModuleDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve entities query: %w", err)
	}

	var buf []byte
	for _, entity := range entities {
		cnt, err := protoToJson(proto.Message(entity))
		if err != nil {
			return nil, fmt.Errorf("failed to encode: %w", err)
		}
		buf = append(buf, cnt...)
	}

	return buf, nil
}

func protoToJson(message proto.Message) ([]byte, error) {
	buf := []byte{}
	data, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	buf = append(buf, data...)
	buf = append(buf, byte('\n'))
	return buf, nil
}
