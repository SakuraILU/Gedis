package server

import (
	"gedis/src/Server/siface"
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
	return nil
}

func (this *Db) Close() error {
	return nil
}

func (this *Db) Set(args [][]byte) {
	if len(args) != 2 {
		panic("Invalid SET command")
	}
	key := string(args[0])
	val := string(args[1])
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	this.hashmap.Put(key, val)
}

func (this *Db) Get(args [][]byte) (val interface{}, err error) {
	if len(args) != 1 {
		panic("Invalid GET command")
	}
	key := string(args[0])
	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)
	val, err = this.hashmap.Get(key)
	return
}

func (this *Db) Del(args [][]byte) (err error) {
	if len(args) != 1 {
		panic("Invalid DEL command")
	}

	key := string(args[0])
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	err = this.hashmap.Del(key)

	return
}
