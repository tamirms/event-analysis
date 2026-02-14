package main

import (
	"fmt"
	"io"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const MainnetPassphrase = "Public Global Stellar Network ; September 2015"

// ExtractEvents extracts non-diagnostic events from decompressed LedgerCloseMeta XDR bytes.
// The events slice is reused across calls — pass the previous result back in to avoid allocations.
// The returned slice is only valid until the next call.
func ExtractEvents(ledgerXDR []byte, events []IngestEvent) ([]IngestEvent, error) {
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

			var contractID []byte
			var topics [][]byte
			var dataBytes []byte

			if ev.Event.ContractId != nil {
				contractID = ev.Event.ContractId[:]
			}
			if ev.Event.Body.V == 0 {
				body := ev.Event.Body.MustV0()
				for _, topic := range body.Topics {
					topicBytes, _ := topic.MarshalBinary()
					topics = append(topics, topicBytes)
				}
				dataBytes, _ = body.Data.MarshalBinary()
			}

			events = append(events, IngestEvent{
				LedgerSequence:   ledgerSeq,
				TransactionIndex: uint32(tx.Index),
				OperationIndex:   0xFFFF,
				EventIndex:       uint16(eventIndex),
				ContractID:       contractID,
				Topics:           topics,
				TxHash:           txHash,
				EventType:        int(ev.Event.Type),
				DataBytes:        dataBytes,
				LedgerClosedAt:   ledgerCloseTime,
				Successful:       txSuccessful,
			})
		}

		// Operation-level events
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIndex, ev := range opEvents {
				if ev.Type == xdr.ContractEventTypeDiagnostic {
					continue
				}

				var contractID []byte
				var topics [][]byte
				var dataBytes []byte

				if ev.ContractId != nil {
					contractID = ev.ContractId[:]
				}
				if ev.Body.V == 0 {
					body := ev.Body.MustV0()
					for _, topic := range body.Topics {
						topicBytes, _ := topic.MarshalBinary()
						topics = append(topics, topicBytes)
					}
					dataBytes, _ = body.Data.MarshalBinary()
				}

				events = append(events, IngestEvent{
					LedgerSequence:   ledgerSeq,
					TransactionIndex: uint32(tx.Index),
					OperationIndex:   uint16(opIndex),
					EventIndex:       uint16(eventIndex),
					ContractID:       contractID,
					Topics:           topics,
					TxHash:           txHash,
					EventType:        int(ev.Type),
					DataBytes:        dataBytes,
					LedgerClosedAt:   ledgerCloseTime,
					Successful:       txSuccessful,
				})
			}
		}
	}

	return events, nil
}
