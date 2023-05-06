package server_test

import (
	"bufio"
	"fmt"
	"gedis/src/Server/server"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func generate_rcmds(rnum int) [][]string {
	// 根据随机数生成一些redis的读命令
	// rcmds: []string{"GET", "TTL", "KEYS",
	// 		"LLEN", "LINDEX", "LRANGE",
	// 		"ZCARD", "ZRANGE", "ZCOUNT", "ZRANK", "ZSCORE"},
	rcmds := []string{"GET", "TTL", "KEYS",
		"LLEN", "LINDEX", "LRANGE",
		"ZCARD", "ZRANGE", "ZCOUNT"}
	r_commands := make([][]string, 0)
	for i := 0; i < rnum; i++ {
		n := rand.Intn(len(rcmds))
		switch n {
		case 0:
			// GET
			r_commands = append(r_commands, []string{"GET", "key" + fmt.Sprint(i)})
		case 1:
			// TTL
			r_commands = append(r_commands, []string{"TTL", "key" + fmt.Sprint(i)})
		case 2:
			// KEYS
			r_commands = append(r_commands, []string{"KEYS", "key" + fmt.Sprint(i)})
		case 3:
			// LLEN
			r_commands = append(r_commands, []string{"LLEN", "key" + fmt.Sprint(i)})
		case 4:
			// LINDEX
			r_commands = append(r_commands, []string{"LINDEX", "key" + fmt.Sprint(i), "0"})
		case 5:
			// LRANGE
			r_commands = append(r_commands, []string{"LRANGE", "key" + fmt.Sprint(i), "0", "-1"})
		case 6:
			// ZCARD
			r_commands = append(r_commands, []string{"ZCARD", "key" + fmt.Sprint(i)})
		case 7:
			// ZRANGE
			r_commands = append(r_commands, []string{"ZRANGE", "key" + fmt.Sprint(i), "0", "-1"})
		case 8:
			// ZCOUNT
			r_commands = append(r_commands, []string{"ZCOUNT", "key" + fmt.Sprint(i)})
		}
	}
	return r_commands
}

func generate_wcmds(wnum int) [][]string {
	// 根据随机数生成一些redis的写命令
	wcmds := []string{"SET", "DEL", "LPUSH", "RPUSH", "LPOP"}
	w_commands := make([][]string, 0)
	for i := 0; i < wnum; i++ {
		n := rand.Intn(len(wcmds))
		switch n {
		case 0:
			// SET
			w_commands = append(w_commands, []string{"SET", "key" + fmt.Sprint(i), "value" + fmt.Sprint(i)})
		case 1:
			// DEL
			w_commands = append(w_commands, []string{"DEL", "key" + fmt.Sprint(i)})
		case 2:
			// LPUSH
			w_commands = append(w_commands, []string{"LPUSH", "key" + fmt.Sprint(i), "value" + fmt.Sprint(i)})
		case 3:
			// RPUSH
			w_commands = append(w_commands, []string{"RPUSH", "key" + fmt.Sprint(i), "value" + fmt.Sprint(i)})
		case 4:
			// LPOP
			w_commands = append(w_commands, []string{"LPOP", "key" + fmt.Sprint(i)})
		case 5:
			// RPOP
			w_commands = append(w_commands, []string{"RPOP", "key" + fmt.Sprint(i)})
		case 6:
			// ZADD
			w_commands = append(w_commands, []string{"ZADD", "key" + fmt.Sprint(i), "value" + fmt.Sprint(i)})
		case 7:
			// ZREM
			w_commands = append(w_commands, []string{"ZREM", "key" + fmt.Sprint(i), "value" + fmt.Sprint(i)})
		}
	}
	return w_commands
}

// 测试序列化和反序列化
func TestCFP1(t *testing.T) {
	// set seed unix time
	rand.Seed(time.Now().Unix())
	r_commands := generate_rcmds(1000)
	w_commands := generate_wcmds(1000)

	cmdfilepack := server.NewCmdFilePack()

	// r_commands will return nil when serialize
	for _, v := range r_commands {
		buf := cmdfilepack.SerializeCmd(v)
		if buf != "" {
			t.Error("SerializeCmd failed")
		}
	}

	// w_commands will return []byte when serialize
	for _, v := range w_commands {
		buf := cmdfilepack.SerializeCmd(v)
		if buf == "" {
			t.Error("SerializeCmd failed")
		}
		cmd := cmdfilepack.UnserializeCmd(buf)
		if len(cmd) != len(v) {
			t.Error("UnserializeCmd failed")
		}
		for i, v := range cmd {
			if v != cmd[i] {
				t.Error("UnserializeCmd failed")
			}
		}
	}

}

// 测试Db.Exec() 时是否正确写入写命令到文件db_[database name], 再一行一行读取命令进行检查
func TestCFP2(t *testing.T) {
	// clear file
	os.Remove(filepath.Join("database", "db_wcmd_test"))

	// set seed unix time
	rand.Seed(time.Now().Unix())
	w_commands := generate_wcmds(1000)

	cmdfilepack := server.NewCmdFilePack()
	name := "wcmd_test"
	db := server.NewDb(name)
	db.Open()

	// db execute write commands
	for _, v := range w_commands {
		// []string -> [][]byte
		cmd := make([][]byte, 0)
		for _, v := range v {
			cmd = append(cmd, []byte(v))
		}
		db.Exec(cmd)
	}
	db.Close()

	// read commands from file
	// open file
	file, err := os.Open(filepath.Join("database", fmt.Sprintf("db_%s", name)))
	if err != nil {
		t.Error("open file failed")
	}
	defer file.Close()
	// read file line by line, every line is a command serialized
	reader := bufio.NewReader(file)
	num := 0
	for ; ; num++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		cmd := cmdfilepack.UnserializeCmd(line)
		// assert cmd len
		if len(cmd) != len(w_commands[num]) {
			t.Error("UnserializeCmd failed")
		}
		// assert cmd content is consistent with w_commands
		for j, v := range cmd {
			if v != w_commands[num][j] {
				t.Error("UnserializeCmd failed")
			}
		}
	}

	// check num
	if num != len(w_commands) {
		t.Error("TestCFP2 failed")
	}
}

// 测试Db.Exec() 时是否正确过滤掉读命令
func TestCFP3(t *testing.T) {
	// clear file
	os.Remove(filepath.Join("database", "db_rcmd_test"))

	// set seed unix time
	rand.Seed(time.Now().Unix())
	rcmds := generate_rcmds(1000)
	cmdfilepack := server.NewCmdFilePack()
	name := "rcmd_test"
	db := server.NewDb(name)
	db.Open()

	// db execute read commands
	for _, v := range rcmds {
		// []string -> [][]byte
		cmd := make([][]byte, 0)
		for _, v := range v {
			cmd = append(cmd, []byte(v))
		}
		db.Exec(cmd)
	}

	// file should be empty...all rcommands should be filtered
	file, err := os.Open(filepath.Join("database", fmt.Sprintf("db_%s", name)))
	if err != nil {
		t.Error("open file failed")
	}
	defer file.Close()
	// read file line by line, every line is a command serialized
	reader := bufio.NewReader(file)
	for i := 0; ; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		cmd := cmdfilepack.UnserializeCmd(line)
		// assert cmd len
		if len(cmd) != 0 {
			t.Error("UnserializeCmd failed")
		}
	}

}

// mixed test r/w
func TestCFP4(t *testing.T) {
	// clear file
	os.Remove(filepath.Join("database", "db_mixed_test"))

	wcmds := generate_wcmds(1000)
	rcmds := generate_rcmds(1000)
	cmds := make([][]string, 0)
	cmds = append(cmds, wcmds...)
	cmds = append(cmds, rcmds...)
	// shuffle cmds
	for i := 0; i < len(cmds); i++ {
		j := rand.Intn(i + 1)
		cmds[i], cmds[j] = cmds[j], cmds[i]
	}

	name := "mixed_test"
	db := server.NewDb(name)
	db.Open()

	// db execute read commands
	for _, v := range cmds {
		// []string -> [][]byte
		cmd := make([][]byte, 0)
		for _, v := range v {
			cmd = append(cmd, []byte(v))
		}
		db.Exec(cmd)
	}
	db.Close()

	// wcmds should be written to file, while rcmds should be filtered
	// thus, check wcmds
	file, err := os.Open(filepath.Join("database", fmt.Sprintf("db_%s", name)))
	if err != nil {
		t.Error("open file failed")
	}
	defer file.Close()
	// read file line by line, every line is a command serialized
	reader := bufio.NewReader(file)
	num := 0
	for ; ; num++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			break
		}
	}
	// check num
	if num != len(wcmds) {
		fmt.Printf("len %d\n", num)
		t.Error("mixed test failed")
	}
}

// concurrent write...
func TestCFP5(t *testing.T) {
	gnum := 100
	wnum := 1000
	// clear file
	name := "concurrent_test"
	os.Remove(filepath.Join("database", "db_concurrent_test"))
	db := server.NewDb("concurrent_test")
	db.Open()

	wg := sync.WaitGroup{}
	wg.Add(gnum)
	// set seed unix time
	rand.Seed(time.Now().Unix())
	for i := 0; i < gnum; i++ {
		go func() {
			rcmds := generate_rcmds(1000)
			wcmds := generate_wcmds(1000)
			cmds := make([][]string, 0)
			cmds = append(cmds, wcmds...)
			cmds = append(cmds, rcmds...)
			// shuffle cmds
			for i := 0; i < len(cmds); i++ {
				j := rand.Intn(i + 1)
				cmds[i], cmds[j] = cmds[j], cmds[i]
			}
			for _, v := range cmds {
				// []string -> [][]byte
				cmd := make([][]byte, 0)
				for _, v := range v {
					cmd = append(cmd, []byte(v))
				}
				db.Exec(cmd)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	db.Close()

	// wcmds should be written to file, while rcmds should be filtered
	// check num of wcmds, should be gnum * wnum
	file, err := os.Open(filepath.Join("database", fmt.Sprintf("db_%s", name)))
	if err != nil {
		t.Error("open file failed")
	}
	defer file.Close()
	// read file line by line, every line is a command serialized
	reader := bufio.NewReader(file)
	num := 0
	for ; ; num++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			break
		}
	}
	// check num
	if num != gnum*wnum {
		fmt.Printf("len %d\n", num)
		t.Error("concurrent test failed")
	}
}
