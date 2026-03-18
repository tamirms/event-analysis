package eventstore

import (
	"github.com/tamir/events-analysis/packfile"
)

// LiveWriter supports incremental eventstore construction with concurrent
// reads on flushed data and crash recovery via packfile.Checkpoint.
type LiveWriter struct {
	eventReader
	lw *packfile.LiveWriter
}

// CreateLive starts a new live eventstore at path.
func CreateLive(path string, opts WriterOptions) (*LiveWriter, error) {
	lw, err := packfile.CreateLive(path, packfileOpts(opts))
	if err != nil {
		return nil, err
	}
	return &LiveWriter{eventReader: eventReader{ir: lw}, lw: lw}, nil
}

// OpenLive recovers a LiveWriter from a packfile.Checkpoint.
func OpenLive(path string, cp packfile.Checkpoint, opts WriterOptions) (*LiveWriter, error) {
	lw, err := packfile.OpenLive(path, cp, packfileOpts(opts))
	if err != nil {
		return nil, err
	}
	return &LiveWriter{eventReader: eventReader{ir: lw}, lw: lw}, nil
}

// Append adds a single event.
func (lw *LiveWriter) Append(event []byte) error {
	return lw.lw.Append(event)
}

// Sync fsyncs and returns a packfile.Checkpoint reflecting durable state.
func (lw *LiveWriter) Sync() (packfile.Checkpoint, error) {
	return lw.lw.Sync()
}

// Freeze finalizes into a standard eventstore packfile.
func (lw *LiveWriter) Freeze() error {
	return lw.lw.Freeze(nil)
}

