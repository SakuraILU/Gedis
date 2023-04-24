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

func (this *Db) Open(string) error {
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
	value, err := this.hashmap.Get(key)
	if err != nil || value.(string) != val {
		panic(fmt.Sprintf("Set failed, key: %s, val: %s", key, val))
	}
	return
}

func (this *Db) Get(args [][]byte) (val interface{}, err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'get' command")
		return
	}
	key := string(args[0])
	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)
	val, err = this.hashmap.Get(key)
	return
}

func (this *Db) Del(args [][]byte) (err error) {
	if len(args) != 1 {
		err = fmt.Errorf("(error) ERR wrong number of arguments for 'del' command")
		return
	}

	key := string(args[0])
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	err = this.hashmap.Del(key)

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
