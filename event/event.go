package event

import (
	"encoding/binary"
	"fmt"
	"slices"
	"time"
)

// Format constants for the binary event encoding.
const (
	HeaderSize        = 80
	FormatVersion     = 0x02
	FlagHasContractID = 0x01
	FlagSuccessful    = 0x02
)

// Type identifies the kind of contract event.
type Type int

const (
	TypeSystem     Type = 0 // matches xdr.ContractEventTypeSystem
	TypeContract   Type = 1 // matches xdr.ContractEventTypeContract
	TypeDiagnostic Type = 2 // matches xdr.ContractEventTypeDiagnostic
)

// Event represents an event captured during ingestion.
type Event struct {
	// Populated by both Marshal/Unmarshal and ExtractEvents.
	ContractID     []byte   // 32 bytes if present
	Topics         [][]byte // pre-marshaled topic XDR
	TxHash         []byte   // 32 bytes
	EventType      Type
	DataBytes      []byte // pre-marshaled SCVal
	LedgerClosedAt time.Time
	Successful     bool

	// Populated only by ExtractEvents (NOT in binary format).
	// Unmarshal zeroes these fields.
	LedgerSequence   uint32
	TransactionIndex uint32
	OperationIndex   uint16
	EventIndex       uint16
}

// Marshal appends the binary encoding of ev to dst and returns the extended slice.
func Marshal(dst []byte, ev *Event) []byte {
	topicsSize := 0
	for _, topic := range ev.Topics {
		topicsSize += 2 + len(topic)
	}
	needed := HeaderSize + topicsSize + len(ev.DataBytes)

	start := len(dst)
	dst = slices.Grow(dst, needed)
	dst = dst[:start+needed]
	buf := dst[start:]

	// Zero the fixed header (contract_id/tx_hash fields may be absent)
	clear(buf[:HeaderSize])

	buf[0] = FormatVersion
	buf[1] = byte(ev.EventType)

	flags := byte(0)
	if len(ev.ContractID) >= 32 {
		flags |= FlagHasContractID
	}
	if ev.Successful {
		flags |= FlagSuccessful
	}
	buf[2] = flags
	buf[3] = byte(len(ev.Topics))

	if len(ev.ContractID) >= 32 {
		copy(buf[4:36], ev.ContractID[:32])
	}
	if len(ev.TxHash) >= 32 {
		copy(buf[36:68], ev.TxHash[:32])
	}

	binary.BigEndian.PutUint64(buf[68:76], uint64(ev.LedgerClosedAt.Unix()))
	binary.BigEndian.PutUint32(buf[76:80], uint32(len(ev.DataBytes)))

	off := HeaderSize
	for _, topic := range ev.Topics {
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(topic)))
		copy(buf[off+2:], topic)
		off += 2 + len(topic)
	}
	copy(buf[off:], ev.DataBytes)

	return dst
}

// Unmarshal parses a binary event into ev. Zero-copy for byte-slice fields:
// ContractID, Topics, TxHash, DataBytes become sub-slices of data.
// Caller must not modify data while ev is in use.
// Reuses ev.Topics backing array to avoid allocation.
//
// Binary layout parsed:
//
//	[0]:    version (0x02)
//	[1]:    eventType
//	[2]:    flags (FlagHasContractID, FlagSuccessful)
//	[3]:    topic count
//	[4:36]: contractID (if flag set)
//	[36:68]: txHash
//	[68:76]: ledgerClosedAt (big-endian unix timestamp)
//	[76:80]: dataBytes length (big-endian)
//	[80:]:  topics (each: 2B length + data), then dataBytes
func Unmarshal(data []byte, ev *Event) error {
	if len(data) < HeaderSize {
		return fmt.Errorf("event: data too short: %d bytes (want >= %d)", len(data), HeaderSize)
	}

	if data[0] != FormatVersion {
		return fmt.Errorf("event: unsupported version %d (want %d)", data[0], FormatVersion)
	}

	ev.EventType = Type(data[1])
	flags := data[2]
	topicCount := int(data[3])

	if flags&FlagHasContractID != 0 {
		ev.ContractID = data[4:36]
	} else {
		ev.ContractID = nil
	}

	ev.TxHash = data[36:68]
	ev.LedgerClosedAt = time.Unix(int64(binary.BigEndian.Uint64(data[68:76])), 0).UTC()
	ev.Successful = flags&FlagSuccessful != 0
	dataBytesLen := int(binary.BigEndian.Uint32(data[76:80]))

	// Parse topics.
	ev.Topics = ev.Topics[:0]
	off := HeaderSize
	for i := range topicCount {
		if off+2 > len(data) {
			return fmt.Errorf("event: truncated topic length at topic %d (offset %d)", i, off)
		}
		tLen := int(binary.BigEndian.Uint16(data[off:]))
		if off+2+tLen > len(data) {
			return fmt.Errorf("event: truncated topic data at topic %d (offset %d, len %d)", i, off, tLen)
		}
		ev.Topics = append(ev.Topics, data[off+2:off+2+tLen])
		off += 2 + tLen
	}

	// Parse dataBytes.
	if off+dataBytesLen > len(data) {
		return fmt.Errorf("event: truncated data bytes (offset %d, len %d, total %d)", off, dataBytesLen, len(data))
	}
	ev.DataBytes = data[off : off+dataBytesLen]

	// Zero ingestion-only fields.
	ev.LedgerSequence = 0
	ev.TransactionIndex = 0
	ev.OperationIndex = 0
	ev.EventIndex = 0

	return nil
}
