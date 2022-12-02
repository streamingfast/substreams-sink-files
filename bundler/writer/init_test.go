package writer

import "github.com/streamingfast/logging"

var zlog, _ = logging.PackageLogger("writer", "github.com/streamingfast/substreams-sink-files/bundler/writer_test")

func init() {
	logging.InstantiateLoggers()
}
