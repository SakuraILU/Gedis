package server

import (
	"fmt"
	"gedis/src/Server/siface"
	"math"
	"sort"
	"sync"
	"time"
)

type kvMap struct {
	Kvs  map[string]value
	Lock sync.RWMutex
}

type value struct {
	Data  interface{}
	TTLat int64
}

type HashMap struct {
	maps []kvMap
	size uint32

	exit_chan      chan bool
	ttl_check_time uint32
}

func NewHashMap(size uint32) *HashMap {
	hashmap := &HashMap{
		maps: make([]kvMap, size),
		size: size,

		exit_chan:      make(chan bool),
		ttl_check_time: 5,
	}

	for i := 0; i < int(size); i++ {
		hashmap.maps[i].Kvs = make(map[string]value)
	}
	return hashmap
}

func (this *HashMap) key2idx(key string) uint32 {
	var code uint32 = 1
	for _, ch := range key {
		code = code*31 + uint32(ch)
	}
	return code % this.size
}

func (this *HashMap) Get(key string) (interface{}, error) {
	idx := this.key2idx(key)
	val, ok := this.maps[idx].Kvs[key]
	var err error
	if !ok {
		err = fmt.Errorf("key %s is not found", key)
	}
	if time.Now().Unix() > val.TTLat {
		err = fmt.Errorf("key %s is not found", key)
		// important bug fix
		// Get is locked by read lock, modify map is not allowed,
		// thus don't delete key here, just leave it to expire goroutine TtlMonitor()
		// delete(this.maps[idx].Kvs, key)
	}
	return val.Data, err
}

func (this *HashMap) GetString(key string) (string, error) {
	val, err := this.Get(key)
	if err != nil {
		return "", fmt.Errorf("(nil)")
	}
	if _, ok := val.(string); !ok {
		return "", fmt.Errorf("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return val.(string), nil
}

func (this *HashMap) GetList(key string, create bool) ([]string, error) {
	val, err := this.Get(key)
	if err != nil {
		if create {
			return []string{}, nil
		} else {
			return nil, fmt.Errorf("(nil)")
		}
	}
	if _, ok := val.([]string); !ok {
		return nil, fmt.Errorf("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return val.([]string), nil
}

func (this *HashMap) GetZset(key string, create bool) (siface.IAVLTree, error) {
	val, err := this.Get(key)
	if err != nil {
		if create {
			return NewAvlTree(), nil
		} else {
			return nil, fmt.Errorf("(nil)")
		}
	}
	if _, ok := val.(siface.IAVLTree); !ok {
		return nil, fmt.Errorf("(error) WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return val.(siface.IAVLTree), nil
}

func (this *HashMap) Put(key string, val interface{}) {
	idx := this.key2idx(key)
	this.maps[idx].Kvs[key] = value{Data: val, TTLat: math.MaxInt64}
}

func (this *HashMap) Del(key string) (err error) {
	idx := this.key2idx(key)
	val, ok := this.maps[idx].Kvs[key]
	if !ok {
		err = fmt.Errorf("key %s is not found", key)
		return
	}
	if time.Now().Unix() > val.TTLat {
		err = fmt.Errorf("key %s is not found", key)
		delete(this.maps[idx].Kvs, key)
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
	for idx := range idxset {
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
	for idx := range idxset {
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

func (this *HashMap) SetTTL(key string, ttl int64) (err error) {
	idx := this.key2idx(key)
	val, ok := this.maps[idx].Kvs[key]
	if !ok {
		err = fmt.Errorf("key %s is not found", key)
		return
	}
	if time.Now().Unix() > val.TTLat {
		err = fmt.Errorf("key %s is not found", key)
		delete(this.maps[idx].Kvs, key)
		return
	}

	val.TTLat = time.Now().Unix() + ttl
	this.maps[idx].Kvs[key] = val
	return
}

func (this *HashMap) GetTTL(key string) (ttl int64, err error) {
	idx := this.key2idx(key)
	val, ok := this.maps[idx].Kvs[key]
	if !ok {
		err = fmt.Errorf("key %s is not found", key)
		return
	}
	// persistent
	if val.TTLat == math.MaxInt64 {
		return EXPIRE_FOREVER, nil
	}
	// expired
	if time.Now().Unix() > val.TTLat {
		err = fmt.Errorf("key %s is not found", key)
		delete(this.maps[idx].Kvs, key)
		return
	}
	// not expired
	ttl = val.TTLat - time.Now().Unix()
	return
}

func (this *HashMap) Persist(key string) (err error) {
	idx := this.key2idx(key)
	val, ok := this.maps[idx].Kvs[key]
	if !ok {
		err = fmt.Errorf("key %s is not found", key)
		return
	}
	if time.Now().Unix() > val.TTLat {
		err = fmt.Errorf("key %s is not found", key)
		delete(this.maps[idx].Kvs, key)
		return
	}
	val.TTLat = math.MaxInt64
	this.maps[idx].Kvs[key] = val
	return
}

func (this *HashMap) StartTtlMonitor() {
	ticker := time.Tick(time.Duration(this.ttl_check_time) * time.Second)
	for {
		select {
		case <-ticker:
			for i := 0; i < int(this.size); i++ {
				this.maps[i].Lock.Lock()
				for k, v := range this.maps[i].Kvs {
					if time.Now().Unix() > v.TTLat {
						delete(this.maps[i].Kvs, k)
					}
				}
				this.maps[i].Lock.Unlock()
			}
		case <-this.exit_chan:
			return
		}
	}
}

func (this *HashMap) StopTtlMonitor() {
	this.exit_chan <- true
}

func (this *HashMap) Foreach(f func(key string, val interface{}, TTLat int64)) {
	for idx := 0; idx < int(this.size); idx++ {
		this.maps[idx].Lock.RLock()
		for k, v := range this.maps[idx].Kvs {
			if time.Now().Unix() > v.TTLat {
				continue // use RLock...so can't delete this key, just ignore
			}
			f(k, v.Data, v.TTLat)
		}
		this.maps[idx].Lock.RUnlock()
	}
}

// func (this *HashMap) FindWithLock(pattern string) (keys []string, err error) {
// 	for idx := 0; idx < int(this.size); idx++ {
// 		this.maps[idx].Lock.RLock()
// 		for k, v := range this.maps[idx].Kvs {
// 			if time.Now().Unix() > v.TTLat {
// 				continue // use RLock...so can't delete this key, just ignore
// 			}
// 			if ismatch, _ := filepath.Match(pattern, k); ismatch {
// 				keys = append(keys, k)
// 			}
// 		}
// 		this.maps[idx].Lock.RUnlock()
// 	}
// 	return
// }
