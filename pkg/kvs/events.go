package kvs

import (
	"time"
)

const maxEventCount = ^uint64(0)

type Event struct {
	// storageType StorageType
	id        uint64
	txnType   TxnType
	key       string
	value     interface{}
	timestamp time.Time
}
