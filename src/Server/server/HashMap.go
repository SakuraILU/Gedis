package server

import (
	"fmt"
	"sort"
	"sync"
)

type kvMap struct {
	Kvs  map[string]value
	Lock sync.RWMutex
}

type value struct {
	Data interface{}
}

type HashMap struct {
	maps []kvMap
	size uint32
}

func NewHashMap(size uint32) *HashMap {
	hashmap := &HashMap{
		maps: make([]kvMap, size),
		size: size,
	}
	for i := 0; i < int(size); i++ {
		hashmap.maps[i].Kvs = make(map[string]value)
	}
	return hashmap
}

func (this *HashMap) Get(key string) (interface{}, error) {
	idx := this.key2idx(key)
	val, ok := this.maps[idx].Kvs[key]
	var err error
	if !ok {
		err = fmt.Errorf("key %s is not found", key)
	}
	return val.Data, err
}

func (this *HashMap) Put(key string, val interface{}) {
	idx := this.key2idx(key)
	this.maps[idx].Kvs[key] = value{Data: val}
}

func (this *HashMap) Del(key string) (err error) {
	idx := this.key2idx(key)
	_, ok := this.maps[idx].Kvs[key]
	if !ok {
		err = fmt.Errorf("key %s is not found", key)
		return
	}
	delete(this.maps[idx].Kvs, key)
	return
}

func (this *HashMap) Lock(key string, write bool) {
	idx := this.key2idx(key)
	if write {
		this.maps[idx].Lock.Lock()
	} else {
		this.maps[idx].Lock.RLock()
	}
}

func (this *HashMap) Unlock(key string, write bool) {
	idx := this.key2idx(key)
	if write {
		this.maps[idx].Lock.Unlock()
	} else {
		this.maps[idx].Lock.RUnlock()
	}
}

func (this *HashMap) Locks(keys []string, write bool) {
	idxset := make(map[uint32]interface{})
	for _, key := range keys {
		idx := this.key2idx(key)
		// 如果idx 不在idxs中，就加入idxs
		_, ok := idxset[idx]
		if !ok {
			idxset[idx] = struct{}{}
		}
	}
	// 将idxs中的idx按照从小到大的顺序排序
	idxs_sorted := make([]uint32, 0, len(idxset))
	for idx, _ := range idxset {
		idxs_sorted = append(idxs_sorted, idx)
	}
	sort.Slice(idxs_sorted, func(i, j int) bool {
		return idxs_sorted[i] < idxs_sorted[j]
	})
	// 按照idxs_sorted中的顺序加锁
	for _, idx := range idxs_sorted {
		if write {
			this.maps[idx].Lock.Lock()
		} else {
			this.maps[idx].Lock.RLock()
		}
	}
}

func (this *HashMap) Unlocks(keys []string, write bool) {
	idxset := make(map[uint32]interface{})
	for _, key := range keys {
		idx := this.key2idx(key)
		// 如果idx 不在idxs中，就加入idxs
		_, ok := idxset[idx]
		if !ok {
			idxset[idx] = struct{}{}
		}
	}
	// 将idxs中的idx按照从小到大的顺序排序
	idxs_sorted := make([]uint32, 0, len(idxset))
	// fmt.Println("idxs_sorted: ")
	for idx, _ := range idxset {
		// fmt.Printf("idx: %d ", idx)
		idxs_sorted = append(idxs_sorted, idx)
	}

	sort.Slice(idxs_sorted, func(i, j int) bool {
		return idxs_sorted[i] < idxs_sorted[j]
	})

	for _, idx := range idxs_sorted {
		if write {
			this.maps[idx].Lock.Unlock()
		} else {
			this.maps[idx].Lock.RUnlock()
		}
	}
}

func (this *HashMap) key2idx(key string) uint32 {
	var code uint32 = 1
	for _, ch := range key {
		code = code*31 + uint32(ch)
	}
	return code % this.size
}
