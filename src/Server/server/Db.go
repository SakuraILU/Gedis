package server

import (
	"fmt"
	"gedis/src/Server/siface"
	"strconv"
)

type Db struct {
	name    string
	hashmap siface.IHashMap
}

func NewDb(name string) *Db {
	return &Db{
		name:    name,
		hashmap: NewHashMap(256),
	}
}

func (this *Db) Open() error {
	go this.hashmap.TtlMonitor()
	return nil
}

func (this *Db) Close() error {
	return nil
}

func (this *Db) Set(args [][]byte) (err error) {
	if len(args) != 2 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'set' command")
		return
	}
	key := string(args[0])
	val := string(args[1])
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	this.hashmap.Put(key, val)
	// assert this.hashmap.Get(key) == val
	// value, err := this.hashmap.Get(key)
	// if err != nil || value.(string) != val {
	// 	panic(fmt.Sprintf("Set failed, key: %s, val: %s", key, val))
	// }
	return
}

func (this *Db) Get(args [][]byte) (val []byte, err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'get' command")
		return
	}
	key := string(args[0])
	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)
	ival, err := this.hashmap.Get(key)
	if err != nil {
		return
	}
	sval, ok := ival.(string)
	if !ok {
		err = fmt.Errorf("(error) ERR wrong get type")
		return
	}
	val = []byte(sval)
	return
}

func (this *Db) Del(args [][]byte) (err error) {
	if len(args) < 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'del' command")
		return
	}

	keys := make([]string, 0)
	for _, key := range args {
		keys = append(keys, string(key))
	}
	this.hashmap.Locks(keys, true)
	defer this.hashmap.Unlocks(keys, true)

	dnum := 0 // how many keys are deleted successfully
	for _, key := range keys {
		if err = this.hashmap.Del(key); err == nil {
			dnum++
		}
	}
	err = fmt.Errorf("%d", dnum)
	return
}

func (this *Db) Mset(args [][]byte) (err error) {
	if len(args)%2 != 0 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'mset' command")
		return
	}

	keys := make([]string, 0)
	vals := make([]string, 0)
	for i, arg := range args {
		if i%2 == 0 {
			keys = append(keys, string(arg))
		} else {
			vals = append(vals, string(arg))
		}
	}

	this.hashmap.Locks(keys, true)
	defer this.hashmap.Unlocks(keys, true)
	for i := 0; i < len(keys); i++ {
		this.hashmap.Put(keys[i], vals[i])
	}
	return
}

func (this *Db) Lpush(args [][]byte) (err error) {
	if len(args) < 2 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'lpush' command")
		return
	}

	key := string(args[0])

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	val, err := this.hashmap.Get(key)
	if err != nil {
		val = make([]string, 0)
		err = nil
	} else if _, ok := val.([]string); !ok {
		err = fmt.Errorf("(error) ERR wrong key type")
	}
	lst := val.([]string)
	for _, key := range args[1:] {
		lst = append([]string{string(key)}, lst...)
	}
	this.hashmap.Put(key, lst)
	return
}

func (this *Db) Rpush(args [][]byte) (err error) {
	if len(args) < 2 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'lpush' command")
		return
	}

	key := string(args[0])

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	val, err := this.hashmap.Get(key)
	if err != nil {
		val = make([]string, 0)
		err = nil
	} else if _, ok := val.([]string); !ok {
		err = fmt.Errorf("(error) ERR wrong key type")
	}
	lst := val.([]string)
	for _, key := range args[1:] {
		lst = append(lst, string(key))
	}
	this.hashmap.Put(key, lst)
	return
}

func (this *Db) Lpop(args [][]byte) (elem []byte, err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'lpop' command")
		return
	}
	key := string(args[0])

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	val, err := this.hashmap.Get(key)
	if err != nil {
		val = make([]string, 0)
		err = nil
	} else if _, ok := val.([]string); !ok {
		err = fmt.Errorf("(error) ERR wrong key type")
	}

	lst := val.([]string)
	if len(lst) == 0 {
		err = fmt.Errorf("(nil)")
		return
	}

	elem = []byte(lst[0])
	lst = lst[1:]
	this.hashmap.Put(key, lst)
	return
}

func (this *Db) Rpop(args [][]byte) (elem []byte, err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'lpop' command")
		return
	}
	key := string(args[0])

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	val, err := this.hashmap.Get(key)
	if err != nil {
		val = make([]string, 0)
		err = nil
	} else if _, ok := val.([]string); !ok {
		err = fmt.Errorf("(error) ERR wrong key type")
	}

	lst := val.([]string)
	if len(lst) == 0 {
		err = fmt.Errorf("(nil)")
		return
	}

	elem = []byte(lst[len(lst)-1])
	lst = lst[0 : len(lst)-1]
	this.hashmap.Put(key, lst)
	return
}

func (this *Db) constrainIndex(index int, length int) int {
	if index < 0 {
		index = length + index
	}
	if index >= length {
		index = length - 1
	}
	if index < 0 {
		index = 0
	}
	return index
}

func (this *Db) Lrange(args [][]byte) (elems [][]byte, err error) {
	if len(args) != 3 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'lrange' command")
		return
	}
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		err = fmt.Errorf("(error) ERR wrong start or end")
		return
	}

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	val, err := this.hashmap.Get(key)
	if err != nil {
		return
	} else if _, ok := val.([]string); !ok {
		err = fmt.Errorf("(error) ERR wrong key type")
		return
	}
	lst := val.([]string)

	start = this.constrainIndex(start, len(lst))
	end = this.constrainIndex(end, len(lst))

	elems = make([][]byte, end-start+1)
	for i := start; i <= end; i++ {
		elems[i-start] = []byte(lst[i])
	}
	return
}

func (this *Db) Llen(args [][]byte) (length uint32, err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'llen' command")
		return
	}
	key := string(args[0])

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	val, err := this.hashmap.Get(key)
	if err != nil {
		return
	} else if _, ok := val.([]string); !ok {
		err = fmt.Errorf("(error) ERR wrong key type")
		return
	}
	length = uint32(len(val.([]string)))
	return
}

func (this *Db) Lindex(args [][]byte) (elem []byte, err error) {
	if len(args) != 2 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'lindex' command")
		return
	}
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		err = fmt.Errorf("(error) ERR index is not an integer")
		return
	}

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	val, err := this.hashmap.Get(key)
	if err != nil {
		return
	} else if _, ok := val.([]string); !ok {
		err = fmt.Errorf("(error) ERR wrong key type")
		return
	}

	lst := val.([]string)
	elem = []byte(lst[index])

	return
}

func (this *Db) Expire(args [][]byte) (err error) {
	if len(args) != 2 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'mset' command")
		return
	}
	key := string(args[0])
	ttl, err := strconv.Atoi(string(args[1]))
	if err != nil {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'expire' command")
		return
	}
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	this.hashmap.SetTTL(key, int64(ttl))
	return
}

func (this *Db) Persist(args [][]byte) (err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'persist' command")
		return
	}
	key := string(args[0])

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	err = this.hashmap.Persist(key)
	return
}

func (this *Db) TTL(args [][]byte) (ttl int64, err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'ttl' command")
		return
	}
	key := string(args[0])

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)
	ttl, err = this.hashmap.GetTTL(key)
	return
}

func (this *Db) Keys(args [][]byte) (keys []string, err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'keys' command")
		return
	}
	pattern := string(args[0])
	keys, err = this.hashmap.FindWithLock(pattern)
	return
}
