package main

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

// Version value, injected via go build `ldflags` at build time
var version = "dev"

var zlog, tracer = logging.RootLogger("substreams-sink-files", "github.com/streamingfast/substreams-sink-files/v2/cmd/substreams-sink-files")

func init() {
	logging.InstantiateLoggers(logging.WithDefaultLevel(zap.InfoLevel))
}

func main() {
	Run("substreams-sink-files", "Substreams Sink to Files (JSONL, CSV, etc.)",
		SyncRunCmd,

		ConfigureViper("SINK_FILES"),
		ConfigureVersion(version),

		Group("tools", "Tools related to Substreams sink files",
			ToolsParquet,
		),

		PersistentFlags(func(flags *pflag.FlagSet) {
			flags.Duration("delay-before-start", 0, "[Operator] Amount of time to wait before starting any internal processes, can be used to perform to maintenance on the pod before actually letting it starts")
			flags.String("metrics-listen-addr", "localhost:9102", "[Operator] If non-empty, the process will listen on this address for Prometheus metrics request(s)")
			flags.String("pprof-listen-addr", "localhost:6060", "[Operator] If non-empty, the process will listen on this address for pprof analysis (see https://golang.org/pkg/net/http/pprof/)")
		}),
		AfterAllHook(func(cmd *cobra.Command) {
			cmd.PersistentPreRun = preStart
		}),
		OnCommandErrorLogAndExit(zlog),
	)
}

func preStart(cmd *cobra.Command, _ []string) {
	delay := sflags.MustGetDuration(cmd, "delay-before-start")
	if delay > 0 {
		zlog.Info("sleeping to respect delay before start setting", zap.Duration("delay", delay))
		time.Sleep(delay)
	}

	if v := sflags.MustGetString(cmd, "metrics-listen-addr"); v != "" {
		zlog.Info("starting prometheus metrics server", zap.String("listen_addr", v))
		go dmetrics.Serve(v)
	}

	if v := sflags.MustGetString(cmd, "pprof-listen-addr"); v != "" {
		go func() {
			zlog.Info("starting pprof server", zap.String("listen_addr", v))
			err := http.ListenAndServe(v, nil)
			if err != nil {
				zlog.Debug("unable to start profiling server", zap.Error(err), zap.String("listen_addr", v))
			}
		}()
	}
}
