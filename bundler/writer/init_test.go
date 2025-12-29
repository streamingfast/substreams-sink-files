package writer

import "github.com/streamingfast/logging"

var zlog, _ = logging.PackageLogger("writer", "github.com/streamingfast/substreams-sink-files/v2/bundler/writer_test")

func init() {
	logging.InstantiateLoggers()
}
