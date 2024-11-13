package tests

import "github.com/streamingfast/logging"

var testLogger, testTracer = logging.PackageLogger("tests", "github.com/streamingfast/substreams-sink-files/tests")

func init() {
	logging.InstantiateLoggers()
}
