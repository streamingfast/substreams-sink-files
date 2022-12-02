package encoder

import (
	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type OutputModule struct {
	descriptor *desc.MessageDescriptor
	module     *pbsubstreams.Module
	hash       manifest.ModuleHash
}
