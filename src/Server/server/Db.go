package server

import (
	"bufio"
	"fmt"
	"gedis/src/Server/siface"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	EXPIRE_FOREVER = -1
	EXPIRE_NONE    = -2
)

type Db struct {
	name    string
	hashmap siface.IHashMap

	handler map[string](func([]string) []string)

	cmd_file_packer *CmdFilePack
	cmd_chan        chan []string
	f_lock          sync.RWMutex
	exit_chan       chan bool
	wg              sync.WaitGroup
}

func NewDb(name string) *Db {
	return &Db{
		name:    name,
		hashmap: NewHashMap(256),

		cmd_file_packer: NewCmdFilePack(),
		cmd_chan:        make(chan []string, 256),
		f_lock:          sync.RWMutex{},
		exit_chan:       make(chan bool),
		wg:              sync.WaitGroup{},
	}
}

func (this *Db) persist2File() {
	fd, err := os.OpenFile(filepath.Join("database", fmt.Sprintf("db_%s", this.name)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("[PERSISIT]: database %s is not find, create an empty one\n", this.name)
	}

	writer := bufio.NewWriter(fd)

	persist_cmd := func(cmd []string) {
		cmdline := this.cmd_file_packer.SerializeCmd(cmd)
		if cmdline != "" {
			this.f_lock.Lock()
			_, err := writer.WriteString(cmdline)
			this.f_lock.Unlock()
			if err != nil {
				return
			}
		}
	}

exit:
	for {
		select {
		case cmd := <-this.cmd_chan:
			persist_cmd(cmd)
		case <-this.exit_chan:
			break exit
		}
	}

	// important bug fix:
	// handle all the left cmds in channel and flush into file
	// 1. channel is buffered, when received msg in exit_chan, there may be some cmds still left in channel which is not handled
	// 2. bufio... creates a buffer in Memory, bufio.WriteString will flush into file at some time he likes...may be full or "\n"? i don't know.
	// but anyway, when exit, we must call bufio.Flush() to forcely flush everything in buffer into file. Otherwise some cmds in buffer will lost
	for {
		select {
		case cmd := <-this.cmd_chan:
			persist_cmd(cmd)
		default:
			// no more msg in cmd_chan, all cmds are handled over
			writer.Flush()
			fd.Close()
			this.wg.Done()
			return
		}
	}
}

func (this *Db) recoveryFromFile() {
	fd, err := os.OpenFile(filepath.Join("database", fmt.Sprintf("db_%s", this.name)), os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("[RECOVER]: database %s is not find\n, create an empty one", this.name)
	}

	reader := bufio.NewReader(fd)
	for {
		cmdline, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		cmd := this.cmd_file_packer.UnserializeCmd(cmdline)

		this.handler[cmd[0]](cmd[1:])
	}
	fd.Close()
}

func (this *Db) Open() error {
	this.handler = make(map[string](func([]string) []string))
	// string
	this.handler["SET"] = this.set
	this.handler["GET"] = this.get
	this.handler["DEL"] = this.del
	this.handler["MSET"] = this.mset
	this.handler["EXPIRE"] = this.expire
	this.handler["PERSIST"] = this.persist
	this.handler["TTL"] = this.ttl
	this.handler["KEYS"] = this.keys
	// list
	this.handler["LPUSH"] = this.lpush
	this.handler["RPUSH"] = this.rpush
	this.handler["LPOP"] = this.lpop
	this.handler["RPOP"] = this.rpop
	this.handler["LLEN"] = this.llen
	this.handler["LINDEX"] = this.lindex
	this.handler["LRANGE"] = this.lrange
	// zset
	this.handler["ZADD"] = this.zadd
	this.handler["ZREM"] = this.zrem
	this.handler["ZCARD"] = this.zcard
	this.handler["ZRANGE"] = this.zrange
	this.handler["ZRANGEBYSCORE"] = this.zrangebyscore
	this.handler["ZCOUNT"] = this.zcount
	this.handler["ZRANK"] = this.zrank
	this.handler["ZSCORE"] = this.zscore
	// TODO: hashmap
	// TODO: set

	// open database
	this.recoveryFromFile()
	// read and execute cmds in database

	go this.hashmap.TtlMonitor()

	this.wg.Add(1)
	go this.persist2File()

	return nil
}

func (this *Db) Close() error {
	this.exit_chan <- true
	// wait persist2File exits compeletly:
	// 1. handle all cmds in channel
	// 2. flushes bufio into file
	this.wg.Wait()

	this.hashmap.StopTtlMonitor()

	close(this.cmd_chan)
	close(this.exit_chan)

	return nil
}

func (this *Db) Exec(bcmd [][]byte) [][]byte {
	if len(bcmd) == 0 {
		ret := make([][]byte, 0, 1)
		ret = append(ret, []byte("(error) ERR wrong number of arguments for 'exec' command"))
		return ret
	}

	// [][]byte -> []string (cmd and args)
	cmd := make([]string, 0, len(bcmd))
	for _, v := range bcmd {
		cmd = append(cmd, string(v))
	}

	// find handler by cmd and execute
	if _, ok := this.handler[cmd[0]]; !ok {
		ret := make([][]byte, 0, 1)
		ret = append(ret, []byte("(error) ERR unknown command '"+cmd[0]+"'"))
		return ret
	}
	// execute
	res := this.handler[cmd[0]](cmd[1:])
	// write into cmd channel for persistance
	this.cmd_chan <- cmd

	// []string -> [][]byte
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
	if err != nil {
		res[0] = "(error) ERR value is not an integer or out of range"
		return
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		res[0] = "(error) ERR value is not an integer or out of range"
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
	for i := start; i < end; i++ {
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
	if index < 0 || index >= len(lst) {
		res[0] = "(nil)"
	} else {
		res[0] = lst[index]
	}
	return
}

func (this *Db) zadd(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		res[0] = "(error) ERR wrong number of arguments for 'zadd' command"
		return
	}

	key := args[0]
	score_keys := args[1:]
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	zset, err := this.hashmap.GetZset(key, true)
	if err != nil {
		res[0] = err.Error()
		return
	}

	// check score validation
	for i := 0; i < len(score_keys); i = i + 2 {
		_, err := strconv.ParseFloat(score_keys[i], 64)
		if err != nil {
			res[0] = "(error) ERR value is not a valid float"
			return
		}
	}

	anum := 0
	for i := 0; i < len(score_keys); i = i + 2 {
		score, _ := strconv.ParseFloat(score_keys[i], 64)
		zset.Add(score, score_keys[i+1])
		anum++
	}
	this.hashmap.Put(key, zset)

	res[0] = fmt.Sprintf("(integer) %d", anum)
	return
}

func (this *Db) zrem(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) < 2 {
		res[0] = "(error) ERR wrong number of arguments for 'zrem' command"
		return
	}
	key := args[0]
	this.hashmap.Lock(key, true)
	defer this.hashmap.Unlock(key, true)

	zset, err := this.hashmap.GetZset(key, true)
	if err != nil {
		res[0] = err.Error()
		return
	}

	dnum := 0
	keys2rm := args[1:]
	for _, key := range keys2rm {
		if err := zset.Remove(key); err == nil {
			dnum++
		}
	}

	this.hashmap.Put(key, zset)

	res[0] = fmt.Sprintf("(integer) %d", dnum)
	return
}

func (this *Db) zcard(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 1 {
		res[0] = "(error) ERR wrong number of arguments for 'zcard' command"
		return
	}

	key := args[0]
	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	zset, err := this.hashmap.GetZset(key, false)
	if err != nil {
		res[0] = err.Error()
		return
	}

	res[0] = fmt.Sprintf("(integer) %d", zset.GetSize())
	return
}

func (this *Db) zrange(args []string) (res []string) {
	res = make([]string, 0)
	if len(args) != 3 {
		res = append(res, "(error) ERR wrong number of arguments for 'zrange' command")
		return
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		res = append(res, "(error) ERR value is not an integer or out of range")
		return
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		res = append(res, "(error) ERR value is not an integer or out of range")
		return
	}

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	zset, err := this.hashmap.GetZset(key, false)
	if err != nil {
		res = append(res, err.Error())
		return
	}

	start = this.constrainIndex(start, int(zset.GetSize()))
	end = this.constrainIndex(end, int(zset.GetSize()))
	entries := zset.GetRangeByRank(uint32(start), uint32(end))
	if len(entries) == 0 {
		res = append(res, "(empty array)")
	}
	for _, entry := range entries {
		res = append(res, entry.Key)
	}

	return
}

func (this *Db) zrangebyscore(args []string) (res []string) {
	res = make([]string, 0)
	if len(args) != 3 {
		res = append(res, "(error) ERR wrong number of arguments for 'zrangebyscore' command")
		return
	}

	key := args[0]
	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	zset, err := this.hashmap.GetZset(key, false)
	if err != nil {
		res = append(res, err.Error())
		return
	}

	start, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		res = append(res, "(error) ERR value is not an float or out of range")
		return
	}
	end, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		res = append(res, "(error) ERR value is not an float or out of range")
		return
	}

	entries := zset.GetRangeByScore(start, end)
	if len(entries) == 0 {
		res = append(res, "(empty array)")
	}
	for _, entry := range entries {
		res = append(res, entry.Key)
	}

	return
}

func (this *Db) zcount(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 3 {
		res[0] = "(error) ERR wrong number of arguments for 'zcount' command"
		return
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		res[0] = "(error) ERR value is not an integer or out of range"
		return
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		res[0] = "(error) ERR value is not an integer or out of range"
		return
	}

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	zset, err := this.hashmap.GetZset(key, false)
	if err != nil {
		res = append(res, err.Error())
		return
	}

	start = this.constrainIndex(start, int(zset.GetSize()))
	end = this.constrainIndex(end, int(zset.GetSize()))
	entries := zset.GetRangeByRank(uint32(start), uint32(end))

	res[0] = fmt.Sprintf("(integer) %d", len(entries))

	return
}

func (this *Db) zrank(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 2 {
		res[0] = "(error) ERR wrong number of arguments for 'zrank' command"
		return
	}

	key := args[0]
	member := args[1]

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	zset, err := this.hashmap.GetZset(key, false)
	if err != nil {
		res = append(res, err.Error())
		return
	}

	rank, err := zset.GetRank(member)
	if err != nil {
		res[0] = "(nil)"
		return
	}

	res[0] = fmt.Sprintf("(integer) %d", rank)

	return
}

func (this *Db) zscore(args []string) (res []string) {
	res = make([]string, 1)
	if len(args) != 2 {
		res[0] = "(error) ERR wrong number of arguments for 'zscore' command"
		return
	}

	key := args[0]
	member := args[1]

	this.hashmap.Lock(key, false)
	defer this.hashmap.Unlock(key, false)

	zset, err := this.hashmap.GetZset(key, false)
	if err != nil {
		res = append(res, err.Error())
		return
	}

	score, err := zset.GetScore(member)
	if err != nil {
		res[0] = "(nil)"
		return
	}

	// The -1 as the third parameter tells the strconv.FormatFloat() to print the fewest digits necessary to accurately represent the float.
	// see: https://stackoverflow.com/questions/31289409/format-a-float-to-n-decimal-places-and-no-trailing-zeros
	res[0] = fmt.Sprintf("(float) %s", strconv.FormatFloat(score, 'f', -1, 64))

	return
}
