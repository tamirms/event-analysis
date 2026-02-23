package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/tamir/events-analysis/event"
)

// --- Shared data loading (used by all benchmarks) ---

var (
	dataOnce      sync.Once
	allEvents     [][]byte // each element is one binary-encoded event
	totalRawBytes int
	dataLoadErr   error
)

const eventsCachePath = "testdata/fixtures/events.bin"

func loadAllEvents() error {
	// Fast path: read from cached binary file (~2s) instead of
	// re-parsing source ledger files (~7 min).
	if !fixturesStale() {
		if err := loadEventsFromCache(); err == nil {
			truncateEvents()
			return nil
		}
	}

	if err := loadEventsFromSource(); err != nil {
		return err
	}
	// Cache for next run.
	if err := saveEventsCache(); err != nil {
		fmt.Printf("  warning: failed to cache events: %v\n", err)
	}
	truncateEvents()
	return nil
}

// truncateEvents limits allEvents to MAX_EVENTS if set, for faster iteration.
func truncateEvents() {
	if s := os.Getenv("MAX_EVENTS"); s != "" {
		if max, err := strconv.Atoi(s); err == nil && max > 0 && max < len(allEvents) {
			allEvents = allEvents[:max]
			totalRawBytes = 0
			for _, ev := range allEvents {
				totalRawBytes += len(ev)
			}
			fmt.Printf("  truncated to %d events, %s total raw bytes (MAX_EVENTS)\n", len(allEvents), fmtKB(float64(totalRawBytes)))
		}
	}
}

func loadEventsFromCache() error {
	f, err := os.Open(eventsCachePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Header: [4B count][4B totalRawBytes]
	var hdr [8]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return err
	}
	count := int(binary.LittleEndian.Uint32(hdr[0:]))
	totalRawBytes = int(binary.LittleEndian.Uint32(hdr[4:]))

	allEvents = make([][]byte, count)
	var lenBuf [4]byte
	for i := range count {
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			return err
		}
		n := int(binary.LittleEndian.Uint32(lenBuf[:]))
		ev := make([]byte, n)
		if _, err := io.ReadFull(f, ev); err != nil {
			return err
		}
		allEvents[i] = ev
	}

	fmt.Printf("  loaded %d events, %s total raw bytes (from cache)\n", len(allEvents), fmtKB(float64(totalRawBytes)))
	return nil
}

func saveEventsCache() error {
	if err := ensureFixtureDir(); err != nil {
		return err
	}
	f, err := os.Create(eventsCachePath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	// Header: [4B count][4B totalRawBytes]
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(allEvents)))
	binary.LittleEndian.PutUint32(hdr[4:], uint32(totalRawBytes))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}

	var lenBuf [4]byte
	for _, ev := range allEvents {
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(ev)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(ev); err != nil {
			return err
		}
	}
	return w.Flush()
}

func loadEventsFromSource() error {
	reader, err := NewChunkReader("006016.index", "006016.data")
	if err != nil {
		return fmt.Errorf("open chunk: %w", err)
	}
	defer reader.Close()

	numLedgers := reader.NumLedgers()
	fmt.Printf("loading events from %d ledgers...\n", numLedgers)

	flat := make([]byte, 0, 512*1024)
	var eventBuf []event.Event

	for i := range numLedgers {
		if i > 0 && i%2000 == 0 {
			fmt.Printf("  ledger %d/%d, events=%d\n", i, numLedgers, len(allEvents))
		}
		ledgerBytes, err := reader.ReadLedger(i)
		if err != nil {
			continue
		}
		eventBuf, err = ExtractEvents(ledgerBytes, eventBuf)
		if err != nil {
			continue
		}
		for idx := range eventBuf {
			flat = event.Marshal(flat[:0], &eventBuf[idx])
			eventCopy := make([]byte, len(flat))
			copy(eventCopy, flat)
			allEvents = append(allEvents, eventCopy)
			totalRawBytes += len(eventCopy)
		}
	}

	fmt.Printf("  loaded %d events, %s total raw bytes\n", len(allEvents), fmtKB(float64(totalRawBytes)))
	return nil
}
