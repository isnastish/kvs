package apitypes

import (
	"fmt"
	"time"
)

type TransactionType int32

// This has to be kept in sync with transaction types defined in a .proto file.
// The order has to be exactly the same.
const (
	TransactionPut TransactionType = iota
	TransactionGet
	TransactionDel
	TransactionIncr
	TransactionIncrBy
)

type TransactionStorageType int32

// NOTE: Rename to DataType?
// This has to be kept in sync with storage types defined in a .proto file.
// The order has to match exactly to what we have in proto file.
const (
	StorageInt TransactionStorageType = iota
	StorageUint
	StorageFloat
	StorageString
	StorageMap
)

type Transaction struct {
	StorageType TransactionStorageType
	TxnType     TransactionType
	Timestamp   time.Time
	Key         string
	Data        interface{}
	// NOTE: The reason we have a separation between key and a data,
	// instead of representing everything as a single map,
	// is because data is optional and could be nil
	// We could use map[string]interface{} as well.
}

var (
	StorageTypeName = map[int32]string{
		0: "StorageInt",
		1: "StorageUint",
		2: "StorageFloat",
		3: "StorageStr",
		4: "StorageMap",
	}
	StorageTypeValue = map[string]int32{
		"StorageInt":   0,
		"StorageUint":  1,
		"StorageFloat": 2,
		"StorageStr":   3,
		"StorageMap":   4,
	}
)

var (
	TransactionTypeName = map[int32]string{
		0: "TransactionPut",
		1: "TransactionGet",
		2: "TransactionDel",
		3: "TransactionIncr",
		4: "TransactionIncrBy",
	}
	TransactionTypeValue = map[string]int32{
		"TransactionPut":    0,
		"TransactionGet":    1,
		"TransactionDel":    2,
		"TransactionIncr":   3,
		"TransactionIncrBy": 4,
	}
)

func (t *Transaction) String() string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("storage type: %s, transaction type: %s, timestamp: %s, key: %s, data: %v",
		StorageTypeName[int32(t.StorageType)], TransactionTypeName[int32(t.TxnType)], t.Timestamp.Format(time.DateTime),
		t.Key, t.Data)
}
