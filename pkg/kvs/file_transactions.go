package kvs

import (
	"context"
	"encoding/gob"
	"io"
	"os"
	_ "time"

	"github.com/isnastish/kvs/pkg/log"
)

// TODO: Use encoding/decoding for the chat as well to encode/decode messages, not raw bytes
// https://stackoverflow.com/questions/35845596/is-a-struct-actually-copied-between-goroutines-if-sent-over-a-golang-channel
// Package for encoding/decoding data sent over the network (to be explored)
// https://pkg.go.dev/encoding/gob

type FileTxnLogger struct {
	id       uint64
	filePath string
	file     *os.File
	events   chan<- Event
	errors   <-chan error
	enc      *gob.Encoder
	dec      *gob.Decoder
}

func NewFileTxnLogger(filePath string) (*FileTxnLogger, error) {
	gob.Register(map[string]string{})

	// Seek method cannot be used on files created with O_APPEND file,
	// we would have to advance the seek pointer manually
	// Since we always read the file first, readEvents will advance the seek ptr,
	// and then write will happen at the right location, so we can omit os.O_APPEND flag
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}

	return &FileTxnLogger{
		filePath: filePath,
		file:     file,
		enc:      gob.NewEncoder(file),
		dec:      gob.NewDecoder(file),
	}, nil
}

func (l *FileTxnLogger) WaitForPendingTransactions() {

}

func (l *FileTxnLogger) Close() {
	defer l.file.Close()
}

// func (l *FileTxnLogger) WriteTransaction(txnType TxnType, storage StorageType, key string, value interface{}) {
// 	l.events <- Event{storageType: storage, txnType: txnType, key: key, value: value, timestamp: time.Now()}
// }

func (l *FileTxnLogger) ProcessTransactions(shutdownContext context.Context) {
	events := make(chan Event, 16)
	errors := make(chan error, 1)

	l.events = events
	l.errors = errors

	encodeEvent := func(event *Event) bool {
		if err := l.enc.Encode(event); err != nil {
			log.Logger.Error("Failed to encode an event %v", err)
			errors <- err
			return false
		}
		return true
	}

	for {
		select {
		case event := <-events:
			event.id = l.id
			if !encodeEvent(&event) {
				return
			}
			l.id++

			// NOTE: Since we encoding and decoding the data when doing file transactions,
			// we don't need to have string representations for storage types, even for logging
			log.Logger.Info("Wrote event: Event{id: %d, t: %s, key: %s, value: %v, timestamp: %v}",
				event.id, event.txnType, event.key, event.value, event.timestamp)

		case <-shutdownContext.Done():
			log.Logger.Info("Finishing writing pending events")
			// If the server terminated, we have to write all the pending events,
			// otherwise the events might get lost, which will be imposible to replay them.
			if len(events) != 0 {
				for event := range events {
					event.id = l.id
					if !encodeEvent(&event) {
						return
					}
					l.id++
				}
			}
			return
		}
	}
}

func (l *FileTxnLogger) ReadEvents() (<-chan Event, <-chan error) {
	events := make(chan Event)
	errors := make(chan error, 1)

	go func() {
		// The receiver still will be able to read from closed channels
		// but not write to them.
		defer close(events)
		defer close(errors)

		for {
			event := Event{}
			err := l.dec.Decode(&event)
			if err != nil && err != io.EOF {
				log.Logger.Error("Error while decoding event %v", err)
				errors <- err
				break
			}
			if err == io.EOF {
				errors <- io.EOF
				break
			}
			events <- event
		}
	}()

	return events, errors
}
