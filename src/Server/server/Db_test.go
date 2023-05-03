package server_test

import (
	"fmt"
	"gedis/src/Server/server"
	"strconv"
	"sync"
	"testing"
)

// single goroutine Set/GET/DEL test
func TestDb(t *testing.T) {
	db := server.NewDb("test")
	db.Open()
	// SET command
	cmd := [][]byte{[]byte("SET"), []byte("key"), []byte("value")}
	db.Exec(cmd)
	// GET command
	cmd = [][]byte{[]byte("GET"), []byte("key")}
	res := db.Exec(cmd)
	if string(res[0]) != "value" {
		t.Error("TestDb failed")
	}
	// DEL command
	cmd = [][]byte{[]byte("DEL"), []byte("key")}
	res = db.Exec(cmd)
	if string(res[0]) != "1" {
		t.Error("TestDb failed")
	}
	// GET command
	cmd = [][]byte{[]byte("GET"), []byte("key")}
	res = db.Exec(cmd)
	if string(res[0]) != "(nil)" {
		t.Error("TestDb failed")
	}
}

// multi goroutine SET/GET test
func TestDb2(t *testing.T) {
	// 生成一些SET命令
	num := 10000
	setcmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		setcmds[i] = [][]byte{[]byte("SET"), []byte("key" + strconv.Itoa(i)), []byte("value" + strconv.Itoa(i))}
	}
	// 生成一些GET命令
	getCmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		getCmds[i] = [][]byte{[]byte("GET"), []byte("key" + strconv.Itoa(i))}
	}

	// 多个goroutine并发SET/GET
	db := server.NewDb("test")
	db.Open()
	for i := 0; i < num; i++ {
		go db.Exec(setcmds[i])
		go func(i int) {
			for {
				val := db.Exec(getCmds[i])
				if string(val[0]) == "value"+strconv.Itoa(i) {
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
	setcmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		setcmds[i] = [][]byte{[]byte("SET"), []byte("key" + strconv.Itoa(i)), []byte("value" + strconv.Itoa(i))}
	}
	// 生成一些DEL命令
	delCmds := make([][][]byte, num)
	for i := num_remain; i < num; i++ {
		delCmds[i] = [][]byte{[]byte("DEL"), []byte("key" + strconv.Itoa(i))}
	}

	// 多个goroutine并发SET/DEL
	db := server.NewDb("test")
	db.Open()

	wg := sync.WaitGroup{}
	wg.Add(num + (num - num_remain))
	for i := 0; i < num; i++ {
		go func(i int) {
			db.Exec(setcmds[i])
			wg.Done()
		}(i)

		if i >= num_remain {
			go func(i int) {
				for {
					val := db.Exec(delCmds[i])
					if string(val[0]) == "1" {
						break
					}
				}
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	// 检查0～num_remain-1的key是否存在
	for i := 0; i < num_remain; i++ {
		cmd := [][]byte{[]byte("GET"), []byte("key" + strconv.Itoa(i))}
		res := db.Exec(cmd)
		if string(res[0]) != "value"+strconv.Itoa(i) {
			t.Error("TestDb3 failed")
		}
	}
	// 检查num_remain～num-1的key是否不存在
	for i := num_remain; i < num; i++ {
		cmd := [][]byte{[]byte("GET"), []byte("key" + strconv.Itoa(i))}
		res := db.Exec(cmd)
		if string(res[0]) != "(nil)" {
			t.Error("TestDb3 failed")
		}
	}
}

// multi goroutine lpush/lpop test
func TestDb4(t *testing.T) {
	// 生成一些LPUSH命令
	num := 100000
	lpushcmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		lpushcmds[i] = [][]byte{[]byte("LPUSH"), []byte("key"), []byte("value" + strconv.Itoa(i))}
	}
	// 生成一些LPOP命令
	lpopCmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		lpopCmds[i] = [][]byte{[]byte("LPOP"), []byte("key")}
	}

	// 多个goroutine并发LPUSH/LPOP
	db := server.NewDb("test")
	db.Open()
	for i := 0; i < num; i++ {
		go db.Exec(lpushcmds[i])
		go func(i int) {
			for {
				val := db.Exec(lpopCmds[i])
				fmt.Printf("%s\n", val[0])
				if string(val[0]) != "(nil)" {
					break
				}
			}
		}(i)
	}
}

// multi goroutine lpush/lpop and lrange test
func TestDb5(t *testing.T) {
	// 生成一些LPUSH命令
	num := 10000
	lpushcmds := make([][][]byte, num)
	for i := 0; i < num; i++ {
		lpushcmds[i] = [][]byte{[]byte("LPUSH"), []byte("key"), []byte("value" + strconv.Itoa(i))}
	}
	// 生成LPOP命令
	pnum := 1000
	lpopCmd := [][]byte{[]byte("LPOP"), []byte("key")}

	// 生成LRANGE命令
	lrangeCmd := [][]byte{[]byte("LRANGE"), []byte("key"), []byte("0"), []byte("-1")}

	// 多个goroutine并发LPUSH/LPOP
	wg := sync.WaitGroup{}
	wg.Add(num + pnum)

	db := server.NewDb("test")
	db.Open()
	for i := 0; i < num; i++ {
		go func(i int) {
			db.Exec(lpushcmds[i])
			wg.Done()
		}(i)
	}

	// 多个goroutine并发LPOP
	for i := 0; i < pnum; i++ {
		go func(i int) {
			for {
				val := db.Exec(lpopCmd)
				if string(val[0]) != "(nil)" {
					break
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	// check length use llen cmd, llen == num
	cmd := [][]byte{[]byte("LLEN"), []byte("key")}
	res := db.Exec(cmd)
	if string(res[0]) != fmt.Sprintf("(integer) %d", num-pnum) {
		t.Error("TestDb5 failed")
	}

	// 检查LRANGE
	lrnum := 1000
	wg.Add(lrnum)
	for i := 0; i < lrnum; i++ {
		go func(i int) {
			res := db.Exec(lrangeCmd)
			if len(res) != num-pnum {
				t.Error("TestDb5 failed")
			}
		}(i)
	}
}
