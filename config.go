package substreams_file_sink

import (
	"fmt"
	"github.com/streamingfast/substreams-sink-files/bundler/writer"
	"go.uber.org/zap"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/pq"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type Config struct {
	SubstreamStateStorePath string
	FileWorkingDir          string
	FileOutputStore         dstore.Store
	BlockRange              string
	Pkg                     *pbsubstreams.Package
	EntitiesQuery           *pq.Query
	OutputModuleName        string
	ClientConfig            *client.SubstreamsClientConfig
	BlockPerFile            uint64
	InMemoryWriter          bool
}

type OutputModule struct {
	descriptor *desc.MessageDescriptor
	module     *pbsubstreams.Module
	hash       manifest.ModuleHash
}

func (c *Config) getBoundaryWriter(zlogger *zap.Logger) writer.Writer {
	fileType := writer.FileTypeJSONL
	if c.InMemoryWriter {
		return writer.NewMem(c.FileOutputStore, fileType, zlogger)
	}
	return writer.NewDStoreIO(c.FileWorkingDir, c.FileOutputStore, fileType, zlogger)
}

func (c *Config) validateOutputModule() (*OutputModule, error) {
	graph, err := manifest.NewModuleGraph(c.Pkg.Modules.Modules)
	if err != nil {
		return nil, fmt.Errorf("create substreams module graph: %w", err)
	}

	module, err := graph.Module(c.OutputModuleName)
	if err != nil {
		return nil, fmt.Errorf("get output module %q: %w", c.OutputModuleName, err)
	}

	if module.GetKindMap() == nil {
		return nil, fmt.Errorf("ouput module %q is *not* of  type 'Mapper'", c.OutputModuleName)
	}

	if !strings.HasPrefix(module.Output.Type, "proto") {
		return nil, fmt.Errorf("output module %q should be of type proto", module.Name)
	}

	msgDec, err := c.outputMessageDescriptor()
	if err != nil {
		return nil, fmt.Errorf("output message descriptor: %w", err)
	}

	hashes := manifest.NewModuleHashes()

	return &OutputModule{
		descriptor: msgDec,
		module:     module,
		hash:       hashes.HashModule(c.Pkg.Modules, module, graph),
	}, nil
}

func (c *Config) outputMessageDescriptor() (*desc.MessageDescriptor, error) {
	fileDescs, err := desc.CreateFileDescriptors(c.Pkg.ProtoFiles)
	if err != nil {
		return nil, fmt.Errorf("couldn't convert, should do this check much earlier: %w", err)
	}

	for _, mod := range c.Pkg.Modules.Modules {
		if mod.Name == c.OutputModuleName {
			var msgType string
			switch modKind := mod.Kind.(type) {
			case *pbsubstreams.Module_KindStore_:
				msgType = modKind.KindStore.ValueType
			case *pbsubstreams.Module_KindMap_:
				msgType = modKind.KindMap.OutputType
			}
			msgType = strings.TrimPrefix(msgType, "proto:")
			var msgDesc *desc.MessageDescriptor
			for _, file := range fileDescs {
				msgDesc = file.FindMessage(msgType)
				if msgDesc != nil {
					return msgDesc, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("output module descriptor not found")
}
