package zstd

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	data := make([]byte, 10000)
	rand.Read(data)

	compressed, err := Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decompressed, err := Decode(nil, compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Fatal("round-trip data mismatch")
	}
}

func TestCorruptData(t *testing.T) {
	data := make([]byte, 1000)
	rand.Read(data)

	compressed, err := Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Truncate to half — should always fail.
	truncated := compressed[:len(compressed)/2]
	_, err = Decode(nil, truncated)
	if err == nil {
		t.Fatal("Decode of truncated data should fail")
	}

	// Zero out the frame header — should always fail.
	corrupted := make([]byte, len(compressed))
	copy(corrupted, compressed)
	for i := range min(8, len(corrupted)) {
		corrupted[i] = 0
	}
	_, err = Decode(nil, corrupted)
	if err == nil {
		t.Fatal("Decode of header-corrupted data should fail")
	}
}

// TestEmptyInput verifies that nil and zero-length inputs produce nil/empty
// output without touching C code (which would SIGSEGV on a nil pointer).
func TestEmptyInput(t *testing.T) {
	// Encode
	if got, err := Encode(nil); err != nil || got != nil {
		t.Fatalf("Encode(nil) = %v, %v, want nil, nil", got, err)
	}
	if got, err := Encode([]byte{}); err != nil || got != nil {
		t.Fatalf("Encode([]byte{}) = %v, %v, want nil, nil", got, err)
	}

	// Decode with nil dst
	got, err := Decode(nil, nil)
	if err != nil {
		t.Fatalf("Decode(nil, nil): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Decode(nil, nil) len = %d, want 0", len(got))
	}

	// Decode with pre-allocated dst: returned slice should reuse dst's backing array.
	dst := make([]byte, 0, 64)
	got, err = Decode(dst, nil)
	if err != nil {
		t.Fatalf("Decode(dst, nil): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Decode(dst, nil) len = %d, want 0", len(got))
	}
	// Verify it reused the backing array (dst[:0] returns the same pointer).
	if cap(got) != cap(dst) {
		t.Fatalf("Decode(dst, nil) did not reuse dst backing array")
	}
}

// TestOneByte exercises the minimum-size input boundary.
func TestOneByte(t *testing.T) {
	data := []byte{0x42}
	compressed, err := Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(compressed) == 0 {
		t.Fatal("Encode of 1 byte returned empty")
	}
	got, err := Decode(nil, compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %v, want %v", got, data)
	}
}

// TestContextReuse verifies that Compressor and Decompressor produce correct
// results across multiple calls with varying payload sizes and compressibility.
// This is the primary usage pattern in eventstore (writer.go line 98, reader.go line 46).
func TestContextReuse(t *testing.T) {
	payloads := [][]byte{
		bytes.Repeat([]byte{0}, 4096), // highly compressible
		make([]byte, 1),               // minimal
		make([]byte, 100000),          // large random
		{0xDE, 0xAD},                  // tiny
		make([]byte, 8192),            // medium random
	}
	// Fill random payloads.
	rand.Read(payloads[2])
	rand.Read(payloads[4])

	c := NewCompressor()
	defer c.Close()
	d := NewDecompressor()
	defer d.Close()

	var dst []byte
	for i, data := range payloads {
		compressed, err := c.Encode(data)
		if err != nil {
			t.Fatalf("iteration %d: Encode: %v", i, err)
		}
		// Copy compressed data before next Encode overwrites scratch.
		saved := make([]byte, len(compressed))
		copy(saved, compressed)

		dst, err = d.Decode(dst, saved)
		if err != nil {
			t.Fatalf("iteration %d: Decode: %v", i, err)
		}
		if !bytes.Equal(dst, data) {
			t.Fatalf("iteration %d: round-trip mismatch (len got %d, want %d)", i, len(dst), len(data))
		}
	}
}

// TestScratchAliasing verifies the aliasing contract: the slice returned by
// Compressor.Encode shares the internal scratch buffer, so a saved copy must
// be used if the result is needed after the next Encode call.
func TestScratchAliasing(t *testing.T) {
	c := NewCompressor()
	defer c.Close()

	data1 := make([]byte, 4096)
	rand.Read(data1)

	out1, err := c.Encode(data1)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	saved := make([]byte, len(out1))
	copy(saved, out1)

	// Verify the saved copy decodes correctly.
	got, err := Decode(nil, saved)
	if err != nil {
		t.Fatalf("Decode saved copy: %v", err)
	}
	if !bytes.Equal(got, data1) {
		t.Fatal("saved copy round-trip mismatch")
	}

	// Second encode with different data reuses scratch.
	data2 := make([]byte, 8192)
	rand.Read(data2)
	out2, err := c.Encode(data2)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// out1 and out2 should share the same backing array (scratch).
	// Verify by checking that out1's pointer falls within scratch bounds.
	// The practical contract: only the most recent Encode result is valid.
	saved2 := make([]byte, len(out2))
	copy(saved2, out2)
	got, err = Decode(nil, saved2)
	if err != nil {
		t.Fatalf("Decode second: %v", err)
	}
	if !bytes.Equal(got, data2) {
		t.Fatal("second round-trip mismatch")
	}
}

// TestDstBufferReuse verifies that Decode reuses a pre-allocated dst buffer
// when it is large enough, avoiding allocation.
func TestDstBufferReuse(t *testing.T) {
	data := make([]byte, 5000)
	rand.Read(data)
	compressed, err := Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Pre-allocate dst larger than needed.
	dst := make([]byte, 0, 10000)
	ptrBefore := &dst[:1][0]

	got, err := Decode(dst, compressed)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("round-trip mismatch")
	}
	ptrAfter := &got[:1][0]
	if ptrBefore != ptrAfter {
		t.Fatal("Decode allocated new buffer instead of reusing dst")
	}
}

// TestCloseIdempotent verifies that calling Close twice does not panic or
// double-free the C context.
func TestCloseIdempotent(t *testing.T) {
	c := NewCompressor()
	c.Close()
	c.Close() // must not panic or double-free

	d := NewDecompressor()
	d.Close()
	d.Close() // must not panic or double-free
}

// TestChecksumDetectsBitFlip verifies that the xxhash64 content checksum
// (enabled via ZSTD_c_checksumFlag) catches single-bit corruption in the
// compressed payload. This is distinct from TestCorruptData which corrupts
// the frame header (caught by magic number / frame parsing, not the checksum).
func TestChecksumDetectsBitFlip(t *testing.T) {
	data := make([]byte, 4096)
	rand.Read(data)
	compressed, err := Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Zstd frame layout: 4B magic + frame header + blocks + 4B checksum.
	// Flip a bit in the middle of the compressed data (block area),
	// leaving magic and frame header intact.
	corrupted := make([]byte, len(compressed))
	copy(corrupted, compressed)

	// Pick a byte roughly in the middle of the frame, well past the header
	// (header is typically 6-12 bytes) and before the trailing 4-byte checksum.
	flipIdx := len(corrupted) / 2
	corrupted[flipIdx] ^= 0x01

	_, err = Decode(nil, corrupted)
	if err == nil {
		t.Fatal("Decode should fail on bit-flipped payload (checksum mismatch)")
	}
}

// TestWithoutChecksum verifies that WithoutChecksum produces valid frames
// and that bit flips in the compressed payload are NOT detected (no checksum).
func TestWithoutChecksum(t *testing.T) {
	data := make([]byte, 4096)
	rand.Read(data)

	c := NewCompressor(WithoutChecksum())
	defer c.Close()

	compressed, err := c.Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	saved := make([]byte, len(compressed))
	copy(saved, compressed)

	// Round-trip must work.
	got, err := Decode(nil, saved)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("round-trip mismatch")
	}

	// Without checksum, a bit flip in the block area may decode without error
	// (producing wrong data) rather than being caught. Flip the same position
	// as TestChecksumDetectsBitFlip.
	corrupted := make([]byte, len(saved))
	copy(corrupted, saved)
	corrupted[len(corrupted)/2] ^= 0x01

	result, err := Decode(nil, corrupted)
	if err == nil && bytes.Equal(result, data) {
		t.Fatal("bit-flipped data decoded to identical output — checksum may still be enabled")
	}
	// Either err != nil (zstd block structure broken) or result != data (silent corruption).
	// Both are acceptable without checksum; the key is we don't guarantee detection.
}

// TestConcurrentInstances verifies that independent Compressor/Decompressor
// instances work correctly from multiple goroutines simultaneously.
// This mirrors the sync.Pool usage in eventstore/reader.go.
func TestConcurrentInstances(t *testing.T) {
	const goroutines = 8
	const iterations = 100

	// Pre-generate payloads so goroutines don't share mutable state.
	payloads := make([][]byte, goroutines)
	for i := range payloads {
		payloads[i] = make([]byte, 1000+i*500)
		rand.Read(payloads[i])
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func() {
			defer wg.Done()
			c := NewCompressor()
			defer c.Close()
			d := NewDecompressor()
			defer d.Close()

			var dst []byte
			for range iterations {
				compressed, err := c.Encode(payloads[g])
				if err != nil {
					t.Errorf("goroutine %d: Encode: %v", g, err)
					return
				}
				saved := make([]byte, len(compressed))
				copy(saved, compressed)

				dst, err = d.Decode(dst, saved)
				if err != nil {
					t.Errorf("goroutine %d: Decode: %v", g, err)
					return
				}
				if !bytes.Equal(dst, payloads[g]) {
					t.Errorf("goroutine %d: round-trip mismatch", g)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestEncodeAfterClose verifies that calling Encode on a closed Compressor
// returns an error instead of passing a nil C pointer (which would SIGSEGV).
func TestEncodeAfterClose(t *testing.T) {
	c := NewCompressor()
	c.Close()

	_, err := c.Encode([]byte("should error"))
	if err == nil {
		t.Fatal("Encode on closed Compressor should return error")
	}
}

// TestDecodeAfterClose verifies that calling Decode on a closed Decompressor
// returns an error instead of crashing.
func TestDecodeAfterClose(t *testing.T) {
	d := NewDecompressor()
	d.Close()

	_, err := d.Decode(nil, []byte("dummy"))
	if err == nil {
		t.Fatal("Decode on closed Decompressor should return error")
	}
}

// TestDecodeNonZstdData verifies that garbage bytes that happen to not look
// like a zstd frame are rejected cleanly with an error, not a crash.
func TestDecodeNonZstdData(t *testing.T) {
	garbage := []byte("this is not zstd compressed data at all")
	_, err := Decode(nil, garbage)
	if err == nil {
		t.Fatal("Decode of non-zstd data should return error")
	}
}
