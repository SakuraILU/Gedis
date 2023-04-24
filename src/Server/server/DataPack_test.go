package server_test

import (
	"gedis/src/Server/server"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

var wg sync.WaitGroup

func startServer(cmds [][][]byte) {
	// 简单的tcp server, 用于测试DataPack.go
	// 1. 监听端口
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		panic(err.Error())
	}
	wg.Done()
	// 2. 接收客户端连接
	conn, err := listener.Accept()
	if err != nil {
		panic(err.Error())
	}
	// 3. 使用Datapack中的UnpackMsg读取客户端数据
	receiver := server.NewDataPack(conn)
	for _, cmd := range cmds {
		msg := receiver.UnpackMsg()
		// assert msg == cmd
		for i, v := range msg {
			if string(v) != string(cmd[i]) {
				panic("msg != cmd")
			}
		}
	}
}

func startclient(cmds [][][]byte) {
	// 连接服务器
	conn, err := net.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		panic(err.Error())
	}
	// 使用Datapack中的PackMsg打包数据
	for _, cmd := range cmds {
		server.NewDataPack(conn).PackMsg(cmd)
		// 稍微等待一下，否则数据过多可能socket会丢失？cmds较多时加上1ms延迟就正确了
		time.Sleep(time.Millisecond * 1)
	}
}

func Test(testing *testing.T) {
	// 生成num个cmds，随机的SET/GET/MSET/MGET/DEL/EXPIRE
	cmds := make([][][]byte, 0)
	num := 1000
	for i := 0; i < num; i++ {
		cmd := make([][]byte, 0)
		// 根据随机数生成cmd
		// 0: SET
		// 1: GET
		// 2: MSET
		// 3: MGET
		// 4: DEL
		// 5: EXPIRE
		rnum := rand.Intn(6)
		if rnum == 0 {
			cmd = append(cmd, []byte("SET"))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i)))
			cmd = append(cmd, []byte("myvalue"+strconv.Itoa(i)))
		} else if rnum == 1 {
			cmd = append(cmd, []byte("GET"))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i)))
		} else if rnum == 2 {
			cmd = append(cmd, []byte("MSET"))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i)))
			cmd = append(cmd, []byte("myvalue"+strconv.Itoa(i)))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i+1)))
			cmd = append(cmd, []byte("myvalue"+strconv.Itoa(i+1)))
		} else if rnum == 3 {
			cmd = append(cmd, []byte("MGET"))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i)))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i+1)))
		} else if rnum == 4 {
			cmd = append(cmd, []byte("DEL"))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i)))
		} else if rnum == 5 {
			cmd = append(cmd, []byte("EXPIRE"))
			cmd = append(cmd, []byte("mykey"+strconv.Itoa(i)))
			cmd = append(cmd, []byte("10"))
		}
		cmds = append(cmds, cmd)
	}

	wg.Add(1)
	// 启动server
	go startServer(cmds)
	wg.Wait()
	// 启动client
	startclient(cmds)
}
