package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// TestExtractProfileCGO runs the same per-phase profile as
// TestExtractEventsProfile but uses the CGO libzstd path (via ChunkReader's
// internal decompressor) rather than klauspost pure-Go, matching the
// production code path.
func TestExtractProfileCGO(t *testing.T) {
	reader, err := NewChunkReader("006016.index", "006016.data")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	numLedgers := reader.NumLedgers()

	var (
		ioTotal      time.Duration
		zstdDecTotal time.Duration
		xdrDecTotal  time.Duration
		xdrEncTotal  time.Duration
	)

	for i := 0; i < numLedgers; i++ {
		t0 := time.Now()
		compressed, err := reader.ReadCompressed(i)
		ioTotal += time.Since(t0)
		if err != nil {
			continue
		}

		t1 := time.Now()
		ledgerBytes, err := reader.Decompress(compressed) // CGO libzstd
		zstdDecTotal += time.Since(t1)
		if err != nil {
			continue
		}

		t2 := time.Now()
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(ledgerBytes); err != nil {
			continue
		}
		xdrDecTotal += time.Since(t2)

		t3 := time.Now()
		_, err = lcm.MarshalBinary()
		xdrEncTotal += time.Since(t3)
	}

	total := ioTotal + zstdDecTotal + xdrDecTotal + xdrEncTotal
	fmt.Printf("\n%d ledgers (CGO libzstd)\n", numLedgers)
	fmt.Printf("  Read file I/O:            %6.2fs  (%4.1f%%)  %6.1f µs/ledger\n",
		ioTotal.Seconds(), pct(ioTotal, total), float64(ioTotal.Microseconds())/float64(numLedgers))
	fmt.Printf("  Zstd decompress (CGO):    %6.2fs  (%4.1f%%)  %6.1f µs/ledger\n",
		zstdDecTotal.Seconds(), pct(zstdDecTotal, total), float64(zstdDecTotal.Microseconds())/float64(numLedgers))
	fmt.Printf("  XDR unmarshal:            %6.2fs  (%4.1f%%)  %6.1f µs/ledger\n",
		xdrDecTotal.Seconds(), pct(xdrDecTotal, total), float64(xdrDecTotal.Microseconds())/float64(numLedgers))
	fmt.Printf("  XDR marshal:              %6.2fs  (%4.1f%%)  %6.1f µs/ledger\n",
		xdrEncTotal.Seconds(), pct(xdrEncTotal, total), float64(xdrEncTotal.Microseconds())/float64(numLedgers))
	fmt.Printf("  Total:                    %6.2fs           %6.1f µs/ledger\n",
		total.Seconds(), float64(total.Microseconds())/float64(numLedgers))
}
