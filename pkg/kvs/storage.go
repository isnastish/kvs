package kvs

import (
	"sync"

	"github.com/pingcap/errors"

	"github.com/isnastish/kvs/pkg/apitypes"
)

type Storage interface {
	Put(key string, cmd *CmdResult) *CmdResult
	Get(key string, cmd *CmdResult) *CmdResult
	Del(key string, CmdResult *CmdResult) *CmdResult
}

type baseStorage struct {
	*sync.RWMutex
}

type IntStorage struct {
	baseStorage
	data map[string]int32
}

type UintStorage struct {
	baseStorage
	data map[string]uint32
}

type FloatStorage struct {
	baseStorage
	data map[string]float32
}

type StrStorage struct {
	baseStorage
	data map[string]string
}

type MapStorage struct {
	baseStorage
	data map[string]map[string]string
}

type CmdResult struct {
	storageType apitypes.TransactionStorageType
	args        []interface{}
	result      interface{}
	err         error
}

func newCmdResult(args ...interface{}) *CmdResult {
	return &CmdResult{
		args: args,
	}
}

func newMapStorage() *MapStorage {
	return &MapStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]map[string]string),
	}
}

func newIntStorage() *IntStorage {
	return &IntStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]int32),
	}
}

func newUintStorage() *UintStorage {
	return &UintStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]uint32),
	}
}

func newFloatStorage() *FloatStorage {
	return &FloatStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]float32),
	}
}

func newStrStorage() *StrStorage {
	return &StrStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]string),
	}
}

func (s *MapStorage) Put(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(map[string]string)
	s.Unlock()
	cmd.storageType = apitypes.StorageMap
	return cmd
}

func (s *MapStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.storageType = apitypes.StorageMap
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *MapStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, deleted := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = apitypes.StorageMap
	cmd.result = deleted
	return cmd
}

func (s *StrStorage) Put(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(string)
	s.Unlock()
	cmd.storageType = apitypes.StorageString
	return cmd
}

func (s *StrStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.storageType = apitypes.StorageString
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *StrStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, deleted := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = apitypes.StorageString
	cmd.result = deleted
	return cmd
}

func (s *IntStorage) Put(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(int32)
	s.Unlock()
	cmd.storageType = apitypes.StorageInt
	return cmd
}

func (s *IntStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.storageType = apitypes.StorageInt
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *IntStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, deleted := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = apitypes.StorageInt
	cmd.result = deleted
	return cmd
}

func (s *IntStorage) Incr(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	defer s.Unlock()
	cmd.storageType = apitypes.StorageInt
	val, exists := s.data[hashKey]
	// If the value doesn't exist, we should created a new entry with the specified key,
	// and set the value to zero
	if !exists {
		s.data[hashKey] = 1
		cmd.result = 0
		return cmd
	}
	cmd.result = val
	s.data[hashKey] = (val + 1)
	return cmd
}

func (s *IntStorage) IncrBy(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	defer s.Unlock()
	cmd.storageType = apitypes.StorageInt
	val, exists := s.data[hashKey]
	// The same applies to IncrBy, if the hashKey doesn't exist,
	// we should create a new one and set the value to cmd.val,
	// but return the result as zero
	if !exists {
		s.data[hashKey] = cmd.args[0].(int32)
		cmd.result = 0
		return cmd
	}
	cmd.result = val
	s.data[hashKey] = (val + cmd.args[0].(int32))
	return cmd
}

func (s *FloatStorage) Put(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = float32(cmd.args[0].(float32))
	s.Unlock()
	cmd.storageType = apitypes.StorageFloat
	return cmd
}

func (s *FloatStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	val, exists := s.data[hashKey]
	s.Unlock()
	cmd.storageType = apitypes.StorageFloat
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *FloatStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, deleted := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = apitypes.StorageFloat
	cmd.result = deleted
	return cmd
}

func (s *UintStorage) Put(key string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[key] = cmd.args[0].(uint32)
	s.Unlock()
	cmd.storageType = apitypes.StorageUint
	return cmd
}

func (s *UintStorage) Get(key string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[key]
	s.RUnlock()
	cmd.storageType = apitypes.StorageUint
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", key)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *UintStorage) Del(key string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, deleted := s.data[key]
	delete(s.data, key)
	s.Unlock()
	cmd.storageType = apitypes.StorageUint
	cmd.result = deleted
	return cmd
}
