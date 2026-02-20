package recordcodec

import "github.com/klauspost/compress/zstd"

var encoder *zstd.Encoder

func init() {
	var err error
	encoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		panic("recordcodec: failed to create zstd encoder: " + err.Error())
	}
}

// NewDecoder creates a new zstd decoder for use in hot parallel paths
// where a shared singleton decoder would cause contention.
// Uses single-threaded decoding (only DecodeAll is used, not streaming)
// and limits max decoded size to 256MB to prevent zip bombs.
func NewDecoder() *zstd.Decoder {
	d, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(256<<20),
	)
	if err != nil {
		panic("recordcodec: failed to create zstd decoder: " + err.Error())
	}
	return d
}

// Encode compresses with zstd (content checksums enabled).
// Safe for concurrent use — the encoder has an internal pool of states
// sized to GOMAXPROCS.
func Encode(data []byte) []byte {
	return encoder.EncodeAll(data, nil)
}
