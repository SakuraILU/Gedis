package server_test

import (
	"gedis/src/Server/server"
	"gedis/src/zinx/znet"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

var wg sync.WaitGroup

func startServer(cmds [][][]byte) {
	// 简单的tcp server, 用于测试CmdPack.go
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
	msg_pack := znet.NewDataPack()
	cmd_pack := server.NewCmdPack()
	for _, cmd := range cmds {
		// 3. 读取客户端数据
		header := make([]byte, msg_pack.GetHeadLen())
		_, err := conn.Read(header)
		if err != nil {
			panic(err.Error())
		}
		// 4. 解包msg
		msg, err := msg_pack.UnpackHead(header)
		if err != nil {
			panic(err.Error())
		}
		// 5. 读取数据
		_, err = conn.Read(msg.GetMsgData())
		if err != nil {
			panic(err.Error())
		}
		// 6. 解包msg的data里的cmd
		cmds_unpacked := cmd_pack.UnpackCmd(msg.GetMsgData())
		// assert msg == cmd
		for i, v := range cmds_unpacked {
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
	msg_pack := znet.NewDataPack()
	cmd_pack := server.NewCmdPack()
	for _, cmd := range cmds {
		// 1. 打包cmd
		cmd_packed := cmd_pack.PackCmd(cmd)
		// 2. 打包msg
		msg := znet.NewMessage(0, cmd_packed)
		msg_packed, err := msg_pack.Pack(msg)
		if err != nil {
			panic(err.Error())
		}
		conn.Write(msg_packed)
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
