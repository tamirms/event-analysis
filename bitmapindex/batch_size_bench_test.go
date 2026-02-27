package bitmapindex

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkBatchSizeImpact measures how batch size affects point-lookup latency.
func BenchmarkBatchSizeImpact(b *testing.B) {
	const numKeys = 10000

	contracts := make([][]byte, numKeys)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint64(raw, uint64(i))
		contracts[i] = raw
	}

	rng := rand.New(rand.NewSource(42))
	const numQueries = 4096
	queryKeys := make([][]byte, numQueries)
	for i := range queryKeys {
		queryKeys[i] = contracts[rng.Intn(numKeys)]
	}

	for _, batchSize := range []int{1, 4, 16, 64, 128} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			dir := b.TempDir()
			mphfPath := filepath.Join(dir, "index.mphf")
			dataPath := filepath.Join(dir, "index.bitmaps")

			w := NewWriter(mphfPath, dataPath, WriterOptions{BatchSize: batchSize})
			for i, cid := range contracts {
				w.Add(makeTestEvent(cid), uint32(i))
			}

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := w.Finish(ctx); err != nil {
				b.Fatalf("Finish: %v", err)
			}

			info, _ := os.Stat(dataPath)
			b.ReportMetric(float64(info.Size())/1024, "packfile-KB")

			r := Open(mphfPath, dataPath)
			defer r.Close()

			b.ResetTimer()
			for i := range b.N {
				key := queryKeys[i%numQueries]
				bm, err := r.Lookup(FieldContractID, key)
				if err != nil {
					b.Fatalf("Lookup: %v", err)
				}
				if bm == nil {
					b.Fatal("unexpected nil bitmap")
				}
			}
		})
	}
}
