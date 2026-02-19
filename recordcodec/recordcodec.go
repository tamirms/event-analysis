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
func NewDecoder() *zstd.Decoder {
	d, err := zstd.NewReader(nil)
	if err != nil {
		panic("recordcodec: failed to create zstd decoder: " + err.Error())
	}
	return d
}

// Encode compresses with zstd (content checksums enabled).
func Encode(data []byte) []byte {
	return encoder.EncodeAll(data, nil)
}
