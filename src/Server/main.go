package main

import (
	"gedis/src/Server/server"
	"gedis/src/zinx/ziface"
	"gedis/src/zinx/znet"
)

func main() {
	db_mgr := server.NewDbManager()
	db_mgr.Start()
	defer db_mgr.Stop()

	gedis_server := znet.NewServer()
	gedis_server.AddRounter(0, server.NewDbRouter())
	gedis_server.AddRounter(1, server.NewDbSelectRouter(db_mgr))
	gedis_server.SetOnConnStart(func(conn ziface.IConnection) {
		conn.SetProperty("db", db_mgr.GetDb(0))
	})

	gedis_server.Serve()
}
