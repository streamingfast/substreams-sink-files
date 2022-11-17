package sink

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

type Syncer struct {
	*shutter.Shutter

	clientConfig          *client.SubstreamsClientConfig
	modules               *pbsubstreams.Modules
	outputModule          *pbsubstreams.Module
	outputModuleHash      string
	stats                 *Stats
	state                 *State
	blockScopeDataHandler BlockScopeDataHandler

	logger *zap.Logger
	tracer logging.Tracer
}

func New(
	modules *pbsubstreams.Modules,
	outputModule *pbsubstreams.Module,
	hash manifest.ModuleHash,
	h BlockScopeDataHandler,
	clientConfig *client.SubstreamsClientConfig,
	cursor *Cursor,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Syncer, error) {
	s := &Syncer{
		Shutter:               shutter.New(),
		clientConfig:          clientConfig,
		modules:               modules,
		outputModule:          outputModule,
		outputModuleHash:      hex.EncodeToString(hash),
		blockScopeDataHandler: h,
		stats:                 newStats(logger),
		state:                 newState(cursor),
		logger:                logger,
		tracer:                tracer,
	}

	return s, nil
}

func (s *Syncer) Start(ctx context.Context, blockRange *bstream.Range) error {
	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })
	s.stats.Start(2 * time.Second)

	return s.run(ctx, blockRange)
}

func (s *Syncer) GetState() *State {
	return s.state
}

func (s *Syncer) SetBlockDataHandler(h BlockScopeDataHandler) {
	s.blockScopeDataHandler = h
}

func (s *Syncer) run(ctx context.Context, blockRange *bstream.Range) (err error) {
	if s.blockScopeDataHandler == nil {
		return fmt.Errorf("block scope data hanlder not set")
	}

	activeCursor := s.state.getCursor()

	ssClient, closeFunc, callOpts, err := client.NewSubstreamsClient(s.clientConfig)
	if err != nil {
		return fmt.Errorf("new substreams client: %w", err)
	}
	s.OnTerminating(func(_ error) { closeFunc() })

	// We will wait at max approximatively 5m before diying
	backOff := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 15), ctx)

	startBlock := blockRange.StartBlock()
	stopBlock := uint64(0)
	if blockRange.EndBlock() != nil {
		stopBlock = *blockRange.EndBlock()
	}

	for {
		req := &pbsubstreams.Request{
			StartBlockNum: int64(startBlock),
			StopBlockNum:  stopBlock,
			StartCursor:   activeCursor.Cursor,
			ForkSteps:     []pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_IRREVERSIBLE},
			Modules:       s.modules,
			OutputModules: []string{s.outputModule.Name},
		}

		activeCursor, err = s.doRequest(ctx, activeCursor, req, ssClient, callOpts)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if blockRange.ReachedEndBlock(activeCursor.Block.Num()) {
					s.logger.Info("substreams ended correctly, reached your stop block",
						zap.String("last_cursor", activeCursor.Cursor),
					)
					return nil
				}
				s.logger.Info("substreams ended correctly, will attempt to reconnect in 15 seconds",
					zap.String("last_cursor", activeCursor.Cursor),
				)
			}
			SubstreamsErrorCount.Inc()
			s.logger.Error("substreams encountered an error", zap.Error(err))

			sleepFor := backOff.NextBackOff()
			if sleepFor == backoff.Stop {
				s.logger.Info("backoff requested to stop retries")
				return err
			}

			s.logger.Info("sleeping before re-connecting", zap.Duration("sleep", sleepFor))
			time.Sleep(sleepFor)
		}
	}
}

func (s *Syncer) doRequest(ctx context.Context, defaultCursor *Cursor, req *pbsubstreams.Request, ssClient pbsubstreams.StreamClient, callOpts []grpc.CallOption) (*Cursor, error) {
	activeCursor := defaultCursor

	s.logger.Debug("launching substreams request", zap.Int64("start_block", req.StartBlockNum))

	progressMessageCount := 0
	stream, err := ssClient.Blocks(ctx, req, callOpts...)
	if err != nil {
		return activeCursor, fmt.Errorf("call sf.substreams.v1.Stream/Blocks: %w", err)
	}

	for {
		if s.tracer.Enabled() {
			s.logger.Debug("substreams waiting to receive message", zap.String("cursor", activeCursor.Cursor))
		}

		resp, err := stream.Recv()
		if err != nil {
			return activeCursor, fmt.Errorf("receive stream next message: %w", err)
		}

		switch r := resp.Message.(type) {
		case *pbsubstreams.Response_Progress:

			for _, module := range r.Progress.Modules {
				progressMessageCount++
				ProgressMessageCount.Inc(module.Name)
			}

			if s.tracer.Enabled() {
				s.logger.Debug("received response progress", zap.Reflect("progress", r))
			}

		case *pbsubstreams.Response_Data:
			block := bstream.NewBlockRef(r.Data.Clock.Id, r.Data.Clock.Number)
			cursor := NewCursor(r.Data.Cursor, block)

			if err := s.blockScopeDataHandler(ctx, cursor, r.Data); err != nil {
				return activeCursor, fmt.Errorf("handle block scope data: %w", err)
			}

			activeCursor = cursor
			s.state.setCursor(cursor)
			s.stats.RecordBlock(block)
			BlockCount.AddInt(1)

		default:
			s.logger.Error("received unknown type of message")
		}

	}
}
