package main

import (
	"gedis/src/Server/server"
	"gedis/src/zinx/ziface"
	"gedis/src/zinx/znet"
)

func main() {
	db := server.NewDb("db1")
	gedis_server := znet.NewServer()
	gedis_server.AddRounter(0, server.NewSetRouter())
	gedis_server.SetOnConnStart(func(conn ziface.IConnection) {
		conn.SetProperty("db", db)
	})

	gedis_server.Serve()
}
