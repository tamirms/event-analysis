package main

import (
	"encoding/binary"
	"slices"
	"time"
)

const (
	binaryFormatVersion = 0x02
	binaryHeaderSize    = 80

	flagHasContractID = 0x01
	flagSuccessful    = 0x02
)

// IngestEvent represents an event captured during ingestion.
type IngestEvent struct {
	LedgerSequence   uint32
	TransactionIndex uint32
	OperationIndex   uint16
	EventIndex       uint16
	ContractID       []byte   // 32 bytes if present
	Topics           [][]byte // pre-marshaled topic XDR
	TxHash           []byte   // 32 bytes
	EventType        int      // 0=system, 1=contract, 2=diagnostic
	DataBytes        []byte   // pre-marshaled SCVal
	LedgerClosedAt   time.Time
	Successful       bool
}

// AppendBinaryEvent encodes an IngestEvent directly into dst, avoiding a separate allocation.
func AppendBinaryEvent(dst []byte, event *IngestEvent) []byte {
	topicsSize := 0
	for _, topic := range event.Topics {
		topicsSize += 2 + len(topic)
	}
	needed := binaryHeaderSize + topicsSize + len(event.DataBytes)

	start := len(dst)
	dst = slices.Grow(dst, needed)
	dst = dst[:start+needed]
	buf := dst[start:]

	// Zero the fixed header (contract_id/tx_hash fields may be absent)
	clear(buf[:binaryHeaderSize])

	buf[0] = binaryFormatVersion
	buf[1] = byte(event.EventType)

	flags := byte(0)
	if len(event.ContractID) > 0 {
		flags |= flagHasContractID
	}
	if event.Successful {
		flags |= flagSuccessful
	}
	buf[2] = flags
	buf[3] = byte(len(event.Topics))

	if len(event.ContractID) >= 32 {
		copy(buf[4:36], event.ContractID[:32])
	}
	if len(event.TxHash) >= 32 {
		copy(buf[36:68], event.TxHash[:32])
	}

	binary.BigEndian.PutUint64(buf[68:76], uint64(event.LedgerClosedAt.Unix()))
	binary.BigEndian.PutUint32(buf[76:80], uint32(len(event.DataBytes)))

	off := binaryHeaderSize
	for _, topic := range event.Topics {
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(topic)))
		copy(buf[off+2:], topic)
		off += 2 + len(topic)
	}
	copy(buf[off:], event.DataBytes)

	return dst
}
