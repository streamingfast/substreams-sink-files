package writer

import (
	"github.com/streamingfast/dmetrics"
	"go.uber.org/zap/zapcore"
	"time"
)

type stats struct {
	creationStart time.Time
	uploadStart   time.Time

	lastUploadTime   time.Duration
	uploadingTime    *dmetrics.AvgDurationCounter
	lastCreationTime time.Duration
	creationTime     *dmetrics.AvgDurationCounter
}

func newStats() *stats {
	return &stats{
		uploadingTime: dmetrics.NewAvgDurationCounter(30*time.Second, time.Second, "upload time"),
		creationTime:  dmetrics.NewAvgDurationCounter(30*time.Second, time.Second, "creation time"),
	}
}

func (s *stats) startCollecting() {
	s.creationStart = time.Now()
}

func (s *stats) stopCollecting() {
	dur := time.Since(s.creationStart)
	s.creationTime.AddDuration(dur)
	s.lastCreationTime = dur
}

func (s *stats) startUploading() {
	s.uploadStart = time.Now()
}

func (s *stats) stopUploading() {
	dur := time.Since(s.uploadStart)
	s.uploadingTime.AddDuration(dur)
	s.lastUploadTime = dur
}

func (s *stats) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("upload_time", s.uploadingTime.String())
	encoder.AddDuration("last_upload_time", s.lastUploadTime)
	encoder.AddString("creation_time", s.creationTime.String())
	encoder.AddDuration("last_creation_time", s.lastCreationTime)
	return nil
}
