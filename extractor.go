package main

import (
	"fmt"
	"io"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/tamir/events-analysis/event"
)

const MainnetPassphrase = "Public Global Stellar Network ; September 2015"

// ExtractEvents extracts non-diagnostic events from decompressed LedgerCloseMeta XDR bytes.
// The events slice is reused across calls — pass the previous result back in to avoid allocations.
// The returned slice is only valid until the next call.
func ExtractEvents(ledgerXDR []byte, events []event.Event) ([]event.Event, error) {
	events = events[:0]

	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(ledgerXDR); err != nil {
		return events, fmt.Errorf("failed to unmarshal LedgerCloseMeta: %w", err)
	}

	if lcm.CountTransactions() == 0 {
		return events, nil
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(MainnetPassphrase, lcm)
	if err != nil {
		return events, fmt.Errorf("failed to create transaction reader for ledger %d: %w", lcm.LedgerSequence(), err)
	}
	defer txReader.Close()

	ledgerSeq := lcm.LedgerSequence()
	ledgerCloseTime := time.Unix(lcm.LedgerCloseTime(), 0).UTC()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return events, fmt.Errorf("failed to read transaction: %w", err)
		}

		txSuccessful := tx.Result.Successful()
		txHash := tx.Hash[:]

		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			return events, fmt.Errorf("failed to get transaction events: %w", err)
		}

		// Transaction-level events
		for eventIndex, ev := range txEvents.TransactionEvents {
			if ev.Event.Type == xdr.ContractEventTypeDiagnostic {
				continue
			}
			parsed, err := makeEventFromXDR(ev.Event, txHash, ledgerSeq, uint32(tx.Index), 0xFFFF, uint16(eventIndex), ledgerCloseTime, txSuccessful)
			if err != nil {
				return events, err
			}
			events = append(events, parsed)
		}

		// Operation-level events
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIndex, ev := range opEvents {
				if ev.Type == xdr.ContractEventTypeDiagnostic {
					continue
				}
				parsed, err := makeEventFromXDR(ev, txHash, ledgerSeq, uint32(tx.Index), uint16(opIndex), uint16(eventIndex), ledgerCloseTime, txSuccessful)
				if err != nil {
					return events, err
				}
				events = append(events, parsed)
			}
		}
	}

	return events, nil
}

func makeEventFromXDR(xdrEvent xdr.ContractEvent, txHash []byte,
	ledgerSeq uint32, txIdx uint32, opIdx uint16, eventIdx uint16,
	closeTime time.Time, successful bool) (event.Event, error) {

	var contractID []byte
	var topics [][]byte
	var dataBytes []byte

	if xdrEvent.ContractId != nil {
		contractID = xdrEvent.ContractId[:]
	}
	if xdrEvent.Body.V == 0 {
		body := xdrEvent.Body.MustV0()
		for _, topic := range body.Topics {
			topicBytes, err := topic.MarshalBinary()
			if err != nil {
				return event.Event{}, fmt.Errorf("failed to marshal topic: %w", err)
			}
			topics = append(topics, topicBytes)
		}
		var err error
		dataBytes, err = body.Data.MarshalBinary()
		if err != nil {
			return event.Event{}, fmt.Errorf("failed to marshal event data: %w", err)
		}
	}

	return event.Event{
		LedgerSequence:   ledgerSeq,
		TransactionIndex: txIdx,
		OperationIndex:   opIdx,
		EventIndex:       eventIdx,
		ContractID:       contractID,
		Topics:           topics,
		TxHash:           txHash,
		EventType:        event.Type(xdrEvent.Type),
		DataBytes:        dataBytes,
		LedgerClosedAt:   closeTime,
		Successful:       successful,
	}, nil
}
