package server

import (
	"fmt"
	"gedis/src/Server/siface"
	"strconv"
)

const (
	EXPIRE_FOREVER = -1
	EXPIRE_NONE    = -2
)

type Db struct {
	name    string
	hashmap siface.IHashMap

	handler map[string](func([]string) []string)

	exit_chan chan bool
}

func NewDb(name string) *Db {
	return &Db{
		name:    name,
		hashmap: NewHashMap(256),
	}
}

func (this *Db) Open() error {
	this.handler = make(map[string](func([]string) []string))
	this.handler["SET"] = this.set
	this.handler["GET"] = this.get
	this.handler["DEL"] = this.del
	this.handler["MSET"] = this.mset
	this.handler["EXPIRE"] = this.expire
	this.handler["PERSIST"] = this.persist
	this.handler["TTL"] = this.ttl
	this.handler["KEYS"] = this.keys
	this.handler["LPUSH"] = this.lpush
	this.handler["RPUSH"] = this.rpush
	this.handler["LPOP"] = this.lpop
	this.handler["RPOP"] = this.rpop
	this.handler["LLEN"] = this.llen
	this.handler["LINDEX"] = this.lindex
	this.handler["LRANGE"] = this.lrange

	go this.hashmap.TtlMonitor()

	return nil
}

func (this *Db) Close() error {
	this.hashmap.StopTtlMonitor()
	return nil
}

func (this *Db) Exec(command [][]byte) [][]byte {
	if len(command) == 0 {
		ret := make([][]byte, 0, 1)
		ret = append(ret, []byte("(error) ERR wrong number of arguments for 'exec' command"))
		return ret
	}

	cmd := string(command[0])
	args := make([]string, 0, len(command)-1)
	for _, arg := range command[1:] {
		args = append(args, string(arg))
	}

	if _, ok := this.handler[cmd]; !ok {
		ret := make([][]byte, 0, 1)
		ret = append(ret, []byte("(error) ERR unknown command '"+cmd+"'"))
		return ret
	}
	res := this.handler[cmd](args)

	ret := make([][]byte, 0, len(res))
	for _, r := range res {
		ret = append(ret, []byte(r))
	}
	return ret
}

func (this *Db) set(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 2 {
		res[0] = "(error) ERR wrong number of arguments for 'set' command"
		return
	}
	key := args[0]
	val := args[1]
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	this.hashmap.Put(key, val)

	// assert this.hashmap.Get(key) == val
	// value, err := this.hashmap.Get(key)
	// if err != nil || value.(string) != val {
	// 	panic(fmt.Sprintf("Set failed, key: %s, val: %s", key, val))
	// }

	res[0] = "OK"
	return
}

func (this *Db) get(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'get' command"
		return
	}

	key := args[0]
	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)
	val, err := this.hashmap.GetString(key)
	if err != nil {
		res[0] = err.Error()
		return
	}
	res[0] = val
	return
}

func (this *Db) del(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) < 1 {
		res[0] = "(error) ERR wrong number of arguments for 'del' command"
		return
	}

	keys := args
	this.hashmap.Locks(keys, true)
	defer this.hashmap.Unlocks(keys, true)

	dnum := 0 // how many keys are deleted successfully
	for _, key := range keys {
		if err := this.hashmap.Del(key); err == nil {
			dnum++
		}
	}
	res[0] = fmt.Sprintf("(integer) %d", dnum)
	return
}

func (this *Db) mset(args []string) (res []string) {
	res = make([]string, 1)

	if len(args)%2 != 0 {
		res[0] = "(error) ERR wrong number of arguments for 'mset' command"
		return
	}

	keys := make([]string, 0)
	vals := make([]string, 0)
	for i, arg := range args {
		if i%2 == 0 {
			keys = append(keys, arg)
		} else {
			vals = append(vals, arg)
		}
	}

	this.hashmap.Locks(keys, true)
	defer this.hashmap.Unlocks(keys, true)

	for i := 0; i < len(keys); i++ {
		this.hashmap.Put(keys[i], vals[i])
	}

	res[0] = "OK"
	return
}

func (this *Db) expire(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 2 {
		res[0] = "(error) ERR wrong number of arguments for 'expire' command"
		return
	}
	key := args[0]
	ttl, err := strconv.Atoi(args[1])
	if err != nil {
		res[0] = "(error) ERR wrong number of arguments for 'expire' command"
		return
	}
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)
	this.hashmap.SetTTL(key, int64(ttl))

	res[0] = "(integer) 1"
	return
}

func (this *Db) persist(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'persist' command"
		return
	}
	key := args[0]

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	err := this.hashmap.Persist(key)
	if err != nil {
		res[0] = "(error) ERR wrong number of arguments for 'persist' command"
	}
	res[0] = "(integer) 1"
	return
}

func (this *Db) ttl(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'ttl' command"
		return
	}

	key := args[0]
	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	ttl, err := this.hashmap.GetTTL(key)
	if err != nil {
		res[0] = fmt.Sprintf("(integer) %d", EXPIRE_NONE)
		return
	}
	if ttl == EXPIRE_FOREVER {
		res[0] = fmt.Sprintf("(integer) %d", EXPIRE_FOREVER)
		return
	}

	res[0] = fmt.Sprintf("(integer) %d", ttl)
	return
}

func (this *Db) keys(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'keys' command"
		return
	}
	pattern := args[0]
	keys, err := this.hashmap.FindWithLock(pattern)
	if err != nil {
		res[0] = "(error) ERR wrong number of arguments for 'keys' command"
		return
	}
	res = make([]string, len(keys))
	for i, key := range keys {
		res[i] = key
	}
	return
}

func (this *Db) lpush(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) < 2 {
		res[0] = "(error) ERR wrong number of arguments for 'lpush' command"
		return
	}

	key := args[0]

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	lst, err := this.hashmap.GetList(key, true)
	if err != nil {
		res[0] = err.Error()
		return
	}

	for _, key := range args[1:] {
		lst = append([]string{key}, lst...)
	}
	this.hashmap.Put(key, lst)

	res[0] = fmt.Sprintf("(integer) %d", len(lst))
	return
}

func (this *Db) rpush(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) < 2 {
		res[0] = "(error) ERR wrong number of arguments for 'lpush' command"
		return
	}

	key := args[0]

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	lst, err := this.hashmap.GetList(key, true)
	if err != nil {
		res[0] = err.Error()
		return
	}

	lst = append(lst, args[1:]...)
	this.hashmap.Put(key, lst)

	res[0] = fmt.Sprintf("(integer) %d", len(lst))
	return
}

func (this *Db) lpop(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'lpop' command"
		return
	}
	key := args[0]

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	lst, err := this.hashmap.GetList(key, false)
	if err != nil {
		res[0] = err.Error()
		return
	}
	// empty list
	if len(lst) == 0 {
		res[0] = "(nil)"
		return
	}

	res[0] = lst[0]
	lst = lst[1:]
	this.hashmap.Put(key, lst)

	return
}

func (this *Db) rpop(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'rpop' command"
		return
	}
	key := args[0]

	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	lst, err := this.hashmap.GetList(key, false)
	if err != nil {
		res[0] = err.Error()
		return
	}
	// empty list
	if len(lst) == 0 {
		res[0] = "(nil)"
		return
	}

	res[0] = lst[len(lst)-1]
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

func (this *Db) lrange(args []string) (res []string) {
	res = make([]string, 0)
	if len(args) != 3 {
		res[0] = "(error) ERR wrong number of arguments for 'lrange' command"
		return
	}
	key := args[0]
	start, err := strconv.Atoi(args[1])
	end, err := strconv.Atoi(args[2])
	if err != nil {
		err = fmt.Errorf("(error) ERR value is not an integer or out of range")
		return
	}

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	lst, err := this.hashmap.GetList(key, false)
	if err != nil {
		res = append(res, err.Error())
		return
	}

	start = this.constrainIndex(start, len(lst))
	end = this.constrainIndex(end, len(lst))
	for i := start; i <= end; i++ {
		res = append(res, lst[i])
	}
	return
}

func (this *Db) llen(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'llen' command"
		return
	}
	key := args[0]

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	lst, err := this.hashmap.GetList(key, false)
	if err != nil {
		res[0] = err.Error()
		return
	}

	res[0] = fmt.Sprintf("(integer) %d", len(lst))
	return
}

func (this *Db) lindex(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 2 {
		res[0] = "(error) ERR wrong number of arguments for 'lindex' command"
		return
	}
	key := args[0]

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	lst, err := this.hashmap.GetList(key, false)
	if err != nil {
		res[0] = err.Error()
		return
	}

	index, err := strconv.Atoi(args[1])
	if err != nil {
		res[0] = "(error) ERR index is not an integer"
		return
	}

	index = this.constrainIndex(index, len(lst))
	res[0] = lst[index]
	return
}
