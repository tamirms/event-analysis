package recordcodec

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	data := make([]byte, 10000)
	rand.Read(data)

	compressed := Encode(data)

	dec := NewDecoder()
	decompressed, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatalf("DecodeAll: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Fatal("round-trip data mismatch")
	}
}

func TestCorruptData(t *testing.T) {
	data := make([]byte, 1000)
	rand.Read(data)

	compressed := Encode(data)
	dec := NewDecoder()

	// Truncate.
	truncated := compressed[:len(compressed)/2]
	_, err := dec.DecodeAll(truncated, nil)
	if err == nil {
		t.Fatal("DecodeAll of truncated data should fail")
	}

	// Flip a byte.
	corrupted := make([]byte, len(compressed))
	copy(corrupted, compressed)
	corrupted[len(corrupted)/2] ^= 0xFF
	_, err = dec.DecodeAll(corrupted, nil)
	if err == nil {
		t.Fatal("DecodeAll of corrupted data should fail")
	}
}
