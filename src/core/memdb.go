package core

import (
	"fmt"
	"strconv"
	"sync"
)

type MemDB struct {
	mutex    sync.RWMutex
	keyspace map[string]string
	// TODO: extend this data structure to support more features
}

func (db *MemDB) Init() {
	db.keyspace = make(map[string]string)
	db.mutex = sync.RWMutex{}
}

func (db *MemDB) CountKeys(keys []string) int {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	cnt := 0
	for _, key := range keys {
		if _, ok := db.keyspace[key]; ok {
			cnt++
		}
	}
	return cnt
}

func (db *MemDB) Get(key string) *string {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	val, ok := db.keyspace[key]
	if !ok {
		return nil
	}
	return &val
}

func (db *MemDB) Set(key string, value string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.keyspace[key] = value
}

func (db *MemDB) Delete(key string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	_, ok := db.keyspace[key]
	if !ok {
		return
	}

	delete(db.keyspace, key)
}

func (db *MemDB) Incr(key string, delta int) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	val, ok := db.keyspace[key]
	if !ok {
		db.keyspace[key] = strconv.Itoa(delta)
		return nil
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return fmt.Errorf("INCR expects a number, got %s", val)
	}
	db.keyspace[key] = strconv.Itoa(i + delta)
	return nil
}
