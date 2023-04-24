package main

import (
	"gedis/src/Server/server"
	"net"
)

func main() {
	// 简单的tcp server, 用于测试DataPack.go
	// 1. 监听端口
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
	if err != nil {
		panic(err.Error())
	}
	// 2. 接收客户端连接
	conn, err := listener.Accept()
	if err != nil {
		panic(err.Error())
	}
	// 3. 使用Datapack中的UnpackMsg读取客户端数据
	msg := server.NewDataPack(conn).UnpackMsg()
	// 4. 打印客户端数据
	for _, v := range msg {
		println(string(v))
	}
}
