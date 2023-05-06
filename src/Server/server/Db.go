package server

import (
	"bufio"
	"fmt"
	"gedis/src/Server/siface"
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
	f_lock          sync.RWMutex
	exit_chan       chan bool
	wg              sync.WaitGroup
}

func NewDb(name string) *Db {
	return &Db{
		name: name,

		engine: NewEngine(),

		cmd_file_packer: NewCmdFilePack(),
		cmd_chan:        make(chan []string, 256),
		f_lock:          sync.RWMutex{},
		exit_chan:       make(chan bool),
		wg:              sync.WaitGroup{},
	}
}

func (this *Db) persistDb() {
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

func (this *Db) recoverDb() {
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

		this.engine.Handle(cmd)
	}
	fd.Close()
}

func (this *Db) Open() error {
	// start engine first and then we can execute cmds for recovery
	this.engine.Start()
	// open database
	this.recoverDb()
	// read and execute cmds in database

	this.wg.Add(1) // used for wait persistDb exits compeletly
	go this.persistDb()

	return nil
}

func (this *Db) Close() error {
	this.exit_chan <- true
	// wait persist2File exits compeletly:
	// 1. handle all cmds in channel
	// 2. flushes bufio into file
	this.wg.Wait()

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
