package sink

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var SubstreamsErrorCount = metrics.NewCounter("substreams_sink_error", "The error count we encountered when interacting with Substreams for which we had to restart the connection loop")
var ProgressMessageCount = metrics.NewCounterVec("substreams_sink_progress_message", []string{"module"}, "The number of progress message received")
var BlockCount = metrics.NewCounter("substreams_sink_block_count", "The number of blocks received")

//var FlushedEntriesCount = metrics.NewCounter("substreams_sink_flushed_entries_count", "The number of flushed entries")
//var FlushCount = metrics.NewCounter("lidar_store_flush_count", "The amount of flush that happened so far")
//var FlushDuration = metrics.NewCounter("lidar_store_flush_duration", "The amount of time spent flushing cache to db")
