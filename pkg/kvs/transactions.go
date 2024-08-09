package kvs

import (
	"context"
)

type TxnType string

// const (
// 	txnPut    TxnType = "put"
// 	txnGet    TxnType = "get"
// 	txnDel    TxnType = "delete"
// 	txnIncr   TxnType = "incr"
// 	txnIncrBy TxnType = "incrby"
// )

type TxnLogger interface {
	// TODO: Make the value optional
	// WriteTransaction(txnType TxnType, storageType StorageType, key string, value interface{})
	ProcessTransactions(ctx context.Context)
	ReadEvents() (<-chan Event, <-chan error)
	Close()
	// NOTE: This function is yet to be implemented.
	// The idea behind it is to wait for all pending transactions to complete before closing the transaction logger.
	WaitForPendingTransactions()
}
