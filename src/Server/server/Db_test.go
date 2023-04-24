package server_test

import (
	"gedis/src/Server/server"
	"strconv"
	"sync"
	"testing"
)

// single goroutine Set/GET/DEL test
func TestDb(t *testing.T) {
	db := server.NewDb("test")
	db.Open("")
	// SET command
	cmd := [][]byte{[]byte("SET"), []byte("key"), []byte("value")}
	db.Set(cmd[1:])
	// GET command
	cmd = [][]byte{[]byte("GET"), []byte("key")}
	val, err := db.Get(cmd[1:])
	if err != nil {
		t.Error("TestDb failed")
	}
	if val.(string) != "value" {
		t.Error("TestDb failed")
	}
	// DEL command
	cmd = [][]byte{[]byte("DEL"), []byte("key")}
	err = db.Del(cmd[1:])
	if err != nil {
		t.Error("TestDb failed")
	}
	// GET command
	cmd = [][]byte{[]byte("GET"), []byte("key")}
	_, err = db.Get(cmd[1:])
	if err == nil {
		t.Error("TestDb failed")
	}
}

// multi goroutine SET/GET test
func TestDb2(t *testing.T) {
	// 生成一些SET命令
	num := 10000
	cmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		cmds[i] = [][]byte{[]byte("SET"), []byte("key" + strconv.Itoa(i)), []byte("value" + strconv.Itoa(i))}
	}
	// 生成一些GET命令
	getCmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		getCmds[i] = [][]byte{[]byte("GET"), []byte("key" + strconv.Itoa(i))}
	}

	// 多个goroutine并发SET/GET
	db := server.NewDb("test")
	db.Open("")
	for i := 0; i < num; i++ {
		go db.Set(cmds[i][1:])
		go func(i int) {
			for {
				val, err := db.Get(getCmds[i][1:])
				if err == nil {
					if val.(string) != "value"+strconv.Itoa(i) {
						t.Error("TestDb2 failed")
					}
					break
				}
			}
		}(i)
	}
}

// multi goroutine SET/DEL test
func TestDb3(t *testing.T) {
	// 生成一些SET命令
	num := 100000
	num_remain := 10000
	cmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		cmds[i] = [][]byte{[]byte("SET"), []byte("key" + strconv.Itoa(i)), []byte("value" + strconv.Itoa(i))}
	}
	// 生成一些DEL命令
	delCmds := make([][][]byte, num)
	for i := num_remain; i < num; i++ {
		delCmds[i] = [][]byte{[]byte("DEL"), []byte("key" + strconv.Itoa(i))}
	}

	// 多个goroutine并发SET/DEL
	db := server.NewDb("test")
	db.Open("")

	wg := sync.WaitGroup{}
	wg.Add(num + (num - num_remain))
	for i := 0; i < num; i++ {
		go func(i int) {
			db.Set(cmds[i][1:])
			wg.Done()
		}(i)

	}
	for i := num_remain; i < num; i++ {
		go func(i int) {
			db.Del(delCmds[i][1:])
			wg.Done()
		}(i)
	}
	wg.Wait()
	// 检查0～num_remain-1的key是否存在
	for i := 0; i < num_remain; i++ {
		cmd := [][]byte{[]byte("GET"), []byte("key" + strconv.Itoa(i))}
		val, err := db.Get(cmd[1:])
		if err != nil {
			t.Error("TestDb3 failed")
		}
		if val.(string) != "value"+strconv.Itoa(i) {
			t.Error("TestDb3 failed")
		}
	}
}
