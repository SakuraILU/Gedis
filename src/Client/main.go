package main

import (
	"gedis/src/Server/server"
	"net"
)

func main() {
	// 连接服务器
	conn, err := net.Dial("tcp", "127.0.0.1:8888")
	if err != nil {
		panic(err.Error())
	}
	// 使用Datapack中的PackMsg打包数据
	// RESP 二进制安全的文本协议
	msg := [][]byte{
		[]byte("MSET"),
		[]byte("mykey"),
		[]byte("myvalue"),
		[]byte("mykey2"),
		[]byte("myvalue2"),
	}
	server.NewDataPack(conn).PackMsg(msg)
}
