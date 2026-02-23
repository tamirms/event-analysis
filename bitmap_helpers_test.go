package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/eventstore"
)

// field identifies which event field a key belongs to.
type field int

const (
	fieldContractID field = iota
	fieldTopic0
	fieldTopic1
	fieldTopic2
	fieldTopic3
)

// allFields enumerates every indexable field.
var allFields = []field{fieldContractID, fieldTopic0, fieldTopic1, fieldTopic2, fieldTopic3}

// extractKey returns the raw key bytes for the given field from a binary event.
// Returns nil if the field is not present in the event.
func extractKey(event []byte, f field) []byte {
	if len(event) < binaryHeaderSize {
		return nil
	}

	switch f {
	case fieldContractID:
		if event[2]&flagHasContractID == 0 {
			return nil
		}
		return event[4:36]

	case fieldTopic0, fieldTopic1, fieldTopic2, fieldTopic3:
		topicCount := int(event[3])
		idx := int(f) - int(fieldTopic0)
		if idx >= topicCount {
			return nil
		}
		off := binaryHeaderSize
		for i := 0; i < topicCount; i++ {
			if off+2 > len(event) {
				return nil
			}
			tLen := int(binary.BigEndian.Uint16(event[off:]))
			if i == idx {
				if off+2+tLen > len(event) {
					return nil
				}
				return event[off+2 : off+2+tLen]
			}
			off += 2 + tLen
		}
		return nil

	default:
		return nil
	}
}

// composeKey appends the field byte to key, producing a lookup key
// that disambiguates the same raw key across different logical fields.
func composeKey(key []byte, f field) []byte {
	out := make([]byte, len(key)+1)
	copy(out, key)
	out[len(key)] = byte(f)
	return out
}

// makeEvent builds a minimal binary event for testing.
// contractID: 32 bytes or nil, topics: list of topic byte slices.
func makeEvent(contractID []byte, topics [][]byte) []byte {
	const version = 0x02
	topicsSize := 0
	for _, t := range topics {
		topicsSize += 2 + len(t)
	}
	buf := make([]byte, binaryHeaderSize+topicsSize)

	buf[0] = version
	buf[1] = 1 // eventType = contract
	flags := byte(0)
	if contractID != nil {
		flags |= flagHasContractID
	}
	buf[2] = flags
	buf[3] = byte(len(topics))

	if contractID != nil && len(contractID) >= 32 {
		copy(buf[4:36], contractID[:32])
	}

	off := binaryHeaderSize
	for _, t := range topics {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(t)))
		copy(buf[off+2:], t)
		off += 2 + len(t)
	}

	return buf
}

// buildBitmapFromEventStore streams all events from the given eventstore and builds
// an MPHF+packfile bitmap index.
func buildBitmapFromEventStore(ctx context.Context, storePath, mphfPath, dataPath string) error {
	er, err := eventstore.Open(storePath)
	if err != nil {
		return fmt.Errorf("bitmapindex: open eventstore: %w", err)
	}
	defer er.Close()

	w := bitmapindex.NewWriter(bitmapindex.WriterOptions{CapacityHint: 600_000})

	ordinal := uint32(0)
	var buf [256]byte
	for event, err := range er.ReadEvents(0, er.EventCount()) {
		if err != nil {
			return fmt.Errorf("bitmapindex: read event %d: %w", ordinal, err)
		}
		for _, f := range allFields {
			key := extractKey(event, f)
			if key == nil {
				continue
			}
			var composed []byte
			if len(key)+1 <= len(buf) {
				n := copy(buf[:], key)
				buf[n] = byte(f)
				composed = buf[:n+1]
			} else {
				composed = composeKey(key, f)
			}
			w.Add(ordinal, composed)
		}
		ordinal++
	}

	return w.Finish(ctx, mphfPath, dataPath)
}

// --- ExtractKey tests (moved from bitmapindex package) ---

func TestExtractKey(t *testing.T) {
	contractID := bytes.Repeat([]byte{0xAB}, 32)
	topic0 := []byte("transfer")
	topic1 := []byte("amount")
	topic2 := []byte("from_addr")
	topic3 := []byte("to_addr")

	event := makeEvent(contractID, [][]byte{topic0, topic1, topic2, topic3})

	tests := []struct {
		f    field
		want []byte
	}{
		{fieldContractID, contractID},
		{fieldTopic0, topic0},
		{fieldTopic1, topic1},
		{fieldTopic2, topic2},
		{fieldTopic3, topic3},
	}
	for _, tt := range tests {
		got := extractKey(event, tt.f)
		if !bytes.Equal(got, tt.want) {
			t.Errorf("extractKey(field=%d): got %x, want %x", tt.f, got, tt.want)
		}
	}

	// No contract ID.
	noContract := makeEvent(nil, [][]byte{topic0})
	if got := extractKey(noContract, fieldContractID); got != nil {
		t.Errorf("extractKey(no contract): got %x, want nil", got)
	}

	// Topic out of range.
	if got := extractKey(noContract, fieldTopic1); got != nil {
		t.Errorf("extractKey(topic1 out of range): got %x, want nil", got)
	}
}

func TestExtractKeyZeroTopics(t *testing.T) {
	event := makeEvent(bytes.Repeat([]byte{0x01}, 32), nil)
	for _, f := range []field{fieldTopic0, fieldTopic1, fieldTopic2, fieldTopic3} {
		if got := extractKey(event, f); got != nil {
			t.Errorf("field %d with 0 topics: got %x, want nil", f, got)
		}
	}
	// Contract ID should still work.
	if got := extractKey(event, fieldContractID); got == nil {
		t.Error("fieldContractID with 0 topics: got nil, want non-nil")
	}
}

func TestExtractKeyTruncatedEvent(t *testing.T) {
	for _, f := range []field{fieldContractID, fieldTopic0, fieldTopic1} {
		if got := extractKey(make([]byte, binaryHeaderSize-1), f); got != nil {
			t.Errorf("truncated event, field %d: got %x, want nil", f, got)
		}
	}
}

func TestExtractKeyInvalidField(t *testing.T) {
	event := makeEvent(bytes.Repeat([]byte{0x01}, 32), [][]byte{[]byte("t0")})
	if got := extractKey(event, field(99)); got != nil {
		t.Errorf("invalid field: got %x, want nil", got)
	}
}
