package server

import (
	"bufio"
	"fmt"
	"gedis/src/Server/siface"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
)

const (
	EXPIRE_FOREVER = -1
	EXPIRE_NONE    = -2
)

type Db struct {
	name string

	engine siface.IEngine

	cmd_file_packer *CmdFilePack
	cmd_chan        chan []string

	f_lock     sync.RWMutex
	fd         *os.File
	writer     *bufio.Writer
	rewrite_wg sync.WaitGroup
	exit_chan  chan bool
	exit_wg    sync.WaitGroup
}

func NewDb(name string) *Db {
	return &Db{
		name: name,

		engine: NewEngine(),

		cmd_file_packer: NewCmdFilePack(),
		cmd_chan:        make(chan []string, 256),
		f_lock:          sync.RWMutex{},
		rewrite_wg:      sync.WaitGroup{},
		exit_chan:       make(chan bool),
		exit_wg:         sync.WaitGroup{},
	}
}

func (this *Db) Open() error {
	// start engine first and then we can execute cmds for recovery
	this.engine.Start()
	// open database
	this.recoverDb()
	// start persist
	this.persistReset()
	this.exit_wg.Add(1) // used for wait persistDb exits compeletly
	go this.persistDb()

	return nil
}

func (this *Db) Close() error {
	this.exit_chan <- true
	// wait persist2File exits compeletly:
	// 1. handle all cmds in channel
	// 2. flushes bufio into file
	this.exit_wg.Wait()

	this.engine.Stop()

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

	// execute cmd
	res := this.engine.Handle(cmd)
	// write into cmd channel for persistance
	this.cmd_chan <- cmd

	// []string -> [][]byte
	ret := make([][]byte, 0, len(res))
	for _, r := range res {
		ret = append(ret, []byte(r))
	}
	return ret
}

func (this *Db) persistDb() {
	repersist_cnt := 0

	persist_cmd := func(cmd []string) {
		cmdline := this.cmd_file_packer.SerializeCmd(cmd)
		if cmdline != "" {
			this.f_lock.Lock()
			_, err := this.writer.WriteString(cmdline)
			this.f_lock.Unlock()
			if err != nil {
				return
			}
			repersist_cnt++
			if repersist_cnt > 5 {
				this.rewrite_wg.Wait()
				go this.reWriteDb()
				this.rewrite_wg.Add(1)
				repersist_cnt = 0
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
			this.writer.Flush()
			this.fd.Close()
			this.exit_wg.Done()
			return
		}
	}
}

func (this *Db) recoverDb() {
	fd, err := os.OpenFile(filepath.Join("database", fmt.Sprintf("db_%s", this.name)), os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("[RECOVER]: database %s is not find\n, create an empty one", this.name)
	}
	defer fd.Close()

	reader := bufio.NewReader(fd)
	for {
		cmdline, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		// excuete the cmd
		this.engine.Handle(this.cmd_file_packer.UnserializeCmd(cmdline))
	}
}

func (this *Db) persistReset() {
	fd, err := os.OpenFile(filepath.Join("database", fmt.Sprintf("db_%s", this.name)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("[PERSISIT]: database %s is not find, create an empty one\n", this.name)
	}
	this.fd = fd
	this.writer = bufio.NewWriter(fd)
}

func (this *Db) reWriteDb() {
	defer this.rewrite_wg.Done()
	// important bug fix: flush... bufio is so horrible...flush, flush and flush...
	// flush and get persist endline: cmd to recover is in dbfile[0: size)
	size := func() int64 {
		this.f_lock.Lock()
		defer this.f_lock.Unlock()

		this.writer.Flush()

		fd, err := os.OpenFile(filepath.Join("database", fmt.Sprintf("db_%s", this.name)), os.O_CREATE|os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("[RECOVER]: database %s is not find\n, create an empty one", this.name)
		}
		defer fd.Close()

		finfo, err := fd.Stat()
		if err != nil {
			return 0
		}
		return finfo.Size()
	}()
	if size == 0 {
		return
	}

	// read cmds in dbfile[0, size) and recover in a tmp engine [engine execute cmds for rewriting]
	fd, err := os.OpenFile(filepath.Join("database", fmt.Sprintf("db_%s", this.name)), os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("[RECOVER]: database %s is not find\n, create an empty one", this.name)
	}
	reader := bufio.NewReader(fd)

	engine := NewEngine()
	engine.Start()
	defer engine.Stop()

	remain := size
	for remain > 0 {
		cmdline, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		engine.Handle(this.cmd_file_packer.UnserializeCmd(cmdline))
		remain -= int64(len(cmdline))
	}

	// persist into a temp file
	tmp_fd, err := ioutil.TempFile("database", "tmpaof")
	if err != nil {
		fmt.Println("fail to create tmp file...repersist stop")
		fd.Close()
		return
	}

	writer := bufio.NewWriter(tmp_fd)
	// traverse all keys and persist key and generate cmds to persist (key, value)s according to value.(type)
	engine.Foreach(func(key string, val interface{}, TTL int64) {
		cmd := this.entry2cmd(key, val, TTL)
		writer.WriteString(this.cmd_file_packer.SerializeCmd(cmd))
	})

	// during replay and write keys into dbfile, f_lock is not hold by rewrite goroutine, thus persist continues...
	// so we need to write the rest cmds in src dbfile into tmp dbfile for consistence, here need to stop persist
	this.f_lock.Lock()
	defer this.f_lock.Unlock()

	_, err = io.Copy(writer, reader)
	if err != nil {
		fmt.Println("fail to copy new cmds in src dbfile into tmp dbfile...repersist stop")
		fd.Close()
		tmp_fd.Close()
		os.Remove(tmp_fd.Name()) // remember to remove tmp file
		return
	}

	fd.Close()
	writer.Flush()
	tmp_fd.Close()

	err = os.Rename(tmp_fd.Name(), fd.Name())
	if err != nil {
		fmt.Println("fail to rename tmp aof to aof...repersisit stop")
		return
	}

	// importent... aof is a new file now, should reset this.fd to the new file and this.fd to the new this.fd...
	this.persistReset()
}

func (this *Db) entry2cmd(key string, val interface{}, TTL int64) (cmd []string) {
	cmd = make([]string, 0)
	switch val := val.(type) {
	case string:
		cmd = append(cmd, "SET", key, val)
	case []string:
		cmd = append(cmd, "RPUSH", key)
		cmd = append(cmd, val...)
	case siface.IAVLTree:
		cmd = append(cmd, "ZADD", key)
		entries := val.GetRangeByRank(0, math.MaxUint32)
		for _, entry := range entries {
			cmd = append(cmd, fmt.Sprint(entry.Score), entry.Key)
		}
	default:
		fmt.Printf("invalid...")
	}
	return
}
