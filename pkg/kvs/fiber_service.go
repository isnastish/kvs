// Implementation of the key-value storage service using fiber.
package kvs

import "github.com/isnastish/kvs/pkg/apitypes"

type FiberService struct {
	storage map[apitypes.TransactionStorageType]Storage
}

func NewFiberService() *FiberService {
	return nil
}
