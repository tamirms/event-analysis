package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const fixtureDir = "testdata/fixtures"

// fixtureFormatVersion is bumped when the on-disk format changes (e.g., bitmapindex metadata).
// Fixtures auto-invalidate when the version in meta.json doesn't match.
const fixtureFormatVersion = 2

type fixtureMeta struct {
	SourceDataMtime  time.Time `json:"source_data_mtime"`
	SourceIndexMtime time.Time `json:"source_index_mtime"`
	TotalEvents      int       `json:"total_events"`
	TotalRawBytes    int       `json:"total_raw_bytes"`
	GeneratedAt      time.Time `json:"generated_at"`
	FormatVersion    int       `json:"format_version,omitempty"`
}

func ensureFixtureDir() error {
	return os.MkdirAll(fixtureDir, 0o755)
}

func metaPath() string {
	return filepath.Join(fixtureDir, "meta.json")
}

func loadMeta() (*fixtureMeta, error) {
	data, err := os.ReadFile(metaPath())
	if err != nil {
		return nil, err
	}
	var m fixtureMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func saveMeta(m *fixtureMeta) error {
	if err := ensureFixtureDir(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(metaPath(), data, 0o644)
}

func fixturesStale() bool {
	if os.Getenv("FORCE_REGEN") != "" {
		return true
	}
	meta, err := loadMeta()
	if err != nil {
		return true
	}
	dataStat, err := os.Stat("006016.data")
	if err != nil {
		return true
	}
	indexStat, err := os.Stat("006016.index")
	if err != nil {
		return true
	}
	if meta.FormatVersion != fixtureFormatVersion {
		return true
	}
	return !dataStat.ModTime().Equal(meta.SourceDataMtime) ||
		!indexStat.ModTime().Equal(meta.SourceIndexMtime)
}

func benchFixturesExist() bool {
	if _, err := os.Stat(filepath.Join(fixtureDir, "bench.events")); err != nil {
		return false
	}
	// RocksDB is a directory — check it's non-empty.
	entries, err := os.ReadDir(filepath.Join(fixtureDir, "rocks.db"))
	if err != nil || len(entries) == 0 {
		return false
	}
	return true
}

func bitmapBenchFixturesExist() bool {
	if _, err := os.Stat(filepath.Join(fixtureDir, "bitmapindex.mphf")); err != nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(fixtureDir, "bitmapindex.bitmaps")); err != nil {
		return false
	}
	entries, err := os.ReadDir(filepath.Join(fixtureDir, "bitmapindex.rocksdb"))
	if err != nil || len(entries) == 0 {
		return false
	}
	return true
}

func buildMeta() (*fixtureMeta, error) {
	dataStat, err := os.Stat("006016.data")
	if err != nil {
		return nil, fmt.Errorf("stat 006016.data: %w", err)
	}
	indexStat, err := os.Stat("006016.index")
	if err != nil {
		return nil, fmt.Errorf("stat 006016.index: %w", err)
	}
	return &fixtureMeta{
		SourceDataMtime:  dataStat.ModTime(),
		SourceIndexMtime: indexStat.ModTime(),
		TotalEvents:      len(allEvents),
		TotalRawBytes:    totalRawBytes,
		GeneratedAt:      time.Now(),
		FormatVersion:    fixtureFormatVersion,
	}, nil
}
