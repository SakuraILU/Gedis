package server

import (
	"gedis/src/Server/siface"
	"gedis/src/zinx/ziface"
	"gedis/src/zinx/znet"
)

type DbRouter struct {
	znet.BaseRounter
	cmd_packer siface.ICmdPack
}

func NewDbRouter() *DbRouter {
	return &DbRouter{
		cmd_packer: NewCmdPack(),
	}
}

func (this *DbRouter) Handle(req ziface.IRequest) {
	conn := req.GetConn()
	buf := req.GetData()
	idb, err := conn.GetProperty("db")
	if err != nil {
		panic("db is not found")
	}
	db := idb.(siface.IDb)

	command := this.cmd_packer.UnpackCmd(buf)

	res := db.Exec(command)

	resp := this.cmd_packer.PackCmd(res)
	conn.SendMsg(0, resp)
}
