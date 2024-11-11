module github.com/streamingfast/substreams-sink-files

go 1.21

toolchain go1.22.4

require (
	github.com/chdb-io/chdb-go v1.7.1
	github.com/jhump/protoreflect v1.14.0
	github.com/parquet-go/parquet-go v0.23.0
	github.com/spf13/cobra v1.7.0
	github.com/spf13/pflag v1.0.5
	github.com/streamingfast/bstream v0.0.2-0.20240916154503-c9c5c8bbeca0
	github.com/streamingfast/cli v0.0.4-0.20230825151644-8cc84512cd80
	github.com/streamingfast/derr v0.0.0-20230515163924-8570aaa43fe1
	github.com/streamingfast/dhammer v0.0.0-20220506192416-3797a7906da2
	github.com/streamingfast/dmetrics v0.0.0-20240214191810-524a5c58fbaa
	github.com/streamingfast/dstore v0.1.1-0.20241011152904-9acd6205dc14
	github.com/streamingfast/logging v0.0.0-20230608130331-f22c91403091
	github.com/streamingfast/shutter v1.5.0
	github.com/streamingfast/substreams v1.10.7
	github.com/streamingfast/substreams-sink v0.4.2
	github.com/test-go/testify v1.1.4
	go.uber.org/zap v1.26.0
)

require (
	buf.build/gen/go/bufbuild/reflect/connectrpc/go v1.16.1-20240117202343-bf8f65e8876c.1 // indirect
	buf.build/gen/go/bufbuild/reflect/protocolbuffers/go v1.33.0-20240117202343-bf8f65e8876c.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	connectrpc.com/connect v1.16.1 // indirect
	github.com/RoaringBitmap/roaring v1.9.1 // indirect
	github.com/alecthomas/participle v0.7.1 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/bobg/go-generics/v2 v2.2.2 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/holiman/uint256 v1.3.1 // indirect
	github.com/huandu/go-sqlbuilder v1.27.3 // indirect
	github.com/huandu/xstrings v1.4.0 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/schollz/closestmatch v2.1.0+incompatible // indirect
	github.com/segmentio/encoding v0.4.0 // indirect
	github.com/spf13/viper v1.15.0 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.23.0 // indirect
	golang.org/x/xerrors v0.0.0-20240716161551-93cc26a95ae9 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

require (
	cloud.google.com/go v0.112.1 // indirect
	cloud.google.com/go/compute v1.25.1 // indirect
	cloud.google.com/go/iam v1.1.6 // indirect
	cloud.google.com/go/storage v1.38.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-storage-blob-go v0.14.0 // indirect
	github.com/aws/aws-sdk-go v1.44.325 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blendle/zapdriver v1.3.2-0.20200203083823-9200777f8a3d // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chzyer/readline v1.5.0 // indirect
	github.com/cncf/xds/go v0.0.0-20240318125728-8a4994d93e50 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/envoyproxy/go-control-plane v0.12.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.3 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/lithammer/dedent v1.1.0 // indirect
	github.com/logrusorgru/aurora v2.0.3+incompatible // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/manifoldco/promptui v0.9.0 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/go-testing-interface v1.14.1 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/paulbellamy/ratecounter v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.16.0 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.0 // indirect
	github.com/sethvargo/go-retry v0.2.3 // indirect
	github.com/spf13/afero v1.10.0 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/streamingfast/dbin v0.9.1-0.20231117225723-59790c798e2c // indirect
	github.com/streamingfast/dgrpc v0.0.0-20240219152146-57bb131c39ca // indirect
	github.com/streamingfast/opaque v0.0.0-20210811180740-0c01d37ea308 // indirect
	github.com/streamingfast/pbgo v0.0.6-0.20240823134334-812f6a16c5cb // indirect
	github.com/stretchr/testify v1.9.0
	github.com/subosito/gotenv v1.4.2 // indirect
	github.com/teris-io/shortid v0.0.0-20171029131806-771a37caa5cf // indirect
	github.com/yourbasic/graph v0.0.0-20210606180040-8ecfec1c2869 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.25.0 // indirect
	golang.org/x/mod v0.19.0 // indirect
	golang.org/x/net v0.27.0 // indirect
	golang.org/x/oauth2 v0.18.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
	golang.org/x/term v0.22.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	google.golang.org/api v0.172.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/grpc v1.64.0 // indirect
	google.golang.org/protobuf v1.34.2
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1
)
