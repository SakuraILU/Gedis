package main

import (
	"gedis/src/Server/server"
	"gedis/src/zinx/ziface"
	"gedis/src/zinx/znet"
)

func main() {
	db_mgr := server.NewDbManager()
	gedis_server := znet.NewServer()
	gedis_server.AddRounter(0, server.NewSetRouter())
	gedis_server.AddRounter(1, server.NewDbRouter(db_mgr))
	gedis_server.SetOnConnStart(func(conn ziface.IConnection) {
		conn.SetProperty("db", db_mgr.GetDb(0))
	})

	gedis_server.Serve()
}
