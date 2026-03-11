package eventstore

import "github.com/tamir/events-analysis/packfile"

const DefaultRecordSize = 128

// WriterOptions configures how the eventstore is written.
type WriterOptions struct {
	RecordSize  int                  // events per record; 0 defaults to DefaultRecordSize (128)
	Concurrency int                  // compression goroutines; 0 or 1 = serial
	Format      packfile.RecordFormat // Compressed (default), Uncompressed, or Raw
	ContentHash bool                 // compute SHA-256 content hash over logical event stream
}

// Writer writes events to an eventstore packfile.
type Writer struct {
	pw *packfile.Writer
}

// Create starts writing a new eventstore at path.
func Create(path string, opts WriterOptions) (*Writer, error) {
	pw, err := packfile.Create(path, packfileOpts(opts))
	if err != nil {
		return nil, err
	}
	return &Writer{pw: pw}, nil
}

func packfileOpts(opts WriterOptions) packfile.WriterOptions {
	recordSize := opts.RecordSize
	if recordSize == 0 {
		recordSize = DefaultRecordSize
	}
	return packfile.WriterOptions{
		RecordSize:   recordSize,
		Concurrency:  opts.Concurrency,
		Format:       opts.Format,
		ContentHash:  opts.ContentHash,
		BytesPerSync: 1 << 20,
	}
}

// Append adds a single event.
func (w *Writer) Append(event []byte) error {
	return w.pw.Append(event)
}

// Finish finalizes the eventstore.
func (w *Writer) Finish() error {
	return w.pw.Finish(nil)
}

// Close releases resources. If Finish was not called, the incomplete file
// is removed. If Finish was called, Close is a no-op.
func (w *Writer) Close() error {
	return w.pw.Close()
}
