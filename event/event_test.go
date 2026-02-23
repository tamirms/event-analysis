package event

import (
	"bytes"
	"testing"
	"time"
)

// makeEvent builds a minimal Event for testing.
func makeEvent(contractID []byte, topics [][]byte) Event {
	return Event{
		ContractID:     contractID,
		Topics:         topics,
		TxHash:         bytes.Repeat([]byte{0xCC}, 32),
		EventType:      TypeContract,
		DataBytes:      []byte("test-data"),
		LedgerClosedAt: time.Unix(1700000000, 0).UTC(),
		Successful:     true,
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	contractID := bytes.Repeat([]byte{0xAB}, 32)
	topics := [][]byte{
		[]byte("transfer"),
		[]byte("amount"),
		[]byte("from_addr"),
		[]byte("to_addr"),
	}

	ev := makeEvent(contractID, topics)
	data := Marshal(nil, &ev)

	var got Event
	if err := Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if !bytes.Equal(got.ContractID, contractID) {
		t.Errorf("ContractID: got %x, want %x", got.ContractID, contractID)
	}
	if got.EventType != TypeContract {
		t.Errorf("EventType: got %d, want %d", got.EventType, TypeContract)
	}
	if !got.Successful {
		t.Error("Successful: got false, want true")
	}
	if !bytes.Equal(got.TxHash, ev.TxHash) {
		t.Errorf("TxHash mismatch")
	}
	if !got.LedgerClosedAt.Equal(ev.LedgerClosedAt) {
		t.Errorf("LedgerClosedAt: got %v, want %v", got.LedgerClosedAt, ev.LedgerClosedAt)
	}
	if !bytes.Equal(got.DataBytes, ev.DataBytes) {
		t.Errorf("DataBytes: got %x, want %x", got.DataBytes, ev.DataBytes)
	}
	if len(got.Topics) != len(topics) {
		t.Fatalf("Topics len: got %d, want %d", len(got.Topics), len(topics))
	}
	for i := range topics {
		if !bytes.Equal(got.Topics[i], topics[i]) {
			t.Errorf("Topic[%d]: got %x, want %x", i, got.Topics[i], topics[i])
		}
	}

	// Ingestion-only fields should be zeroed.
	if got.LedgerSequence != 0 || got.TransactionIndex != 0 || got.OperationIndex != 0 || got.EventIndex != 0 {
		t.Error("ingestion-only fields not zeroed")
	}
}

func TestUnmarshalNoContractID(t *testing.T) {
	ev := makeEvent(nil, [][]byte{[]byte("topic0")})
	data := Marshal(nil, &ev)

	var got Event
	if err := Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ContractID != nil {
		t.Errorf("ContractID: got %x, want nil", got.ContractID)
	}
	if len(got.Topics) != 1 || !bytes.Equal(got.Topics[0], []byte("topic0")) {
		t.Errorf("Topics: got %v, want [[topic0]]", got.Topics)
	}
}

func TestUnmarshalZeroTopics(t *testing.T) {
	ev := makeEvent(bytes.Repeat([]byte{0x01}, 32), nil)
	data := Marshal(nil, &ev)

	var got Event
	if err := Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Topics) != 0 {
		t.Errorf("Topics: got %d topics, want 0", len(got.Topics))
	}
	if got.ContractID == nil {
		t.Error("ContractID should not be nil")
	}
}

func TestUnmarshalTruncated(t *testing.T) {
	var ev Event
	err := Unmarshal(make([]byte, HeaderSize-1), &ev)
	if err == nil {
		t.Fatal("expected error for truncated input")
	}
}

func TestUnmarshalTopicBoundaries(t *testing.T) {
	topics := [][]byte{
		bytes.Repeat([]byte{0x01}, 50),
		bytes.Repeat([]byte{0x02}, 100),
		bytes.Repeat([]byte{0x03}, 1),
	}
	ev := makeEvent(nil, topics)
	data := Marshal(nil, &ev)

	var got Event
	if err := Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Topics) != 3 {
		t.Fatalf("Topics len: got %d, want 3", len(got.Topics))
	}
	for i := range topics {
		if !bytes.Equal(got.Topics[i], topics[i]) {
			t.Errorf("Topic[%d]: length got %d, want %d", i, len(got.Topics[i]), len(topics[i]))
		}
	}
}
