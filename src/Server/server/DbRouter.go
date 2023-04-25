package server

import (
	"fmt"
	"gedis/src/Server/siface"
	"gedis/src/zinx/ziface"
	"gedis/src/zinx/znet"
	"strconv"
)

type DbRouter struct {
	znet.BaseRounter
	db_mgr     *DbManager
	cmd_packer siface.ICmdPack
}

func NewDbRouter(db_mgr *DbManager) *DbRouter {
	return &DbRouter{
		db_mgr:     db_mgr,
		cmd_packer: NewCmdPack(),
	}
}

func (this *DbRouter) Handle(req ziface.IRequest) {
	conn := req.GetConn()
	buf := req.GetData()

	cmd_arg := this.cmd_packer.UnpackCmd(buf)
	cmd := string(cmd_arg[0])
	args := cmd_arg[1:]

	resp := this.cmd_packer.PackCmd([][]byte{[]byte("OK")})
	switch cmd {
	case "SELECT":
		if len(args) != 1 {
			str := "(error) ERR wrong number of arguments for 'keys' command"
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(str)})
			conn.SendMsg(0, resp)
			return
		}
		id, err := strconv.Atoi(string(args[0]))
		if err != nil {
			str := "(error) ERR wrong number of arguments for 'keys' command"
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(str)})
			conn.SendMsg(0, resp)
			return
		}
		if id < 0 || id >= 16 {
			str := "ERR DB index is out of range"
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(str)})
			conn.SendMsg(0, resp)
			return
		}
		conn.SetProperty("db", this.db_mgr.GetDb(uint32(id)))
		resp = this.cmd_packer.PackCmd([][]byte{[]byte(fmt.Sprint(id))})
		conn.SendMsg(1, resp)
	default:
		resp = this.cmd_packer.PackCmd([][]byte{[]byte("Unspported command")})
		conn.SendMsg(0, resp)
	}

}
