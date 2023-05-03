package server

import (
	"fmt"
	"gedis/src/Server/siface"
	"gedis/src/zinx/ziface"
	"gedis/src/zinx/znet"
)

type ListRouter struct {
	znet.BaseRounter
	cmd_packer siface.ICmdPack
}

func NewListRouter() *ListRouter {
	return &ListRouter{
		cmd_packer: NewCmdPack(),
	}
}

func (this *ListRouter) Handle(req ziface.IRequest) {
	conn := req.GetConn()
	data := req.GetData()

	idb, err := conn.GetProperty("db")
	if err != nil {
		panic("db is not found")
	}
	db := idb.(siface.IDb)

	cmds := this.cmd_packer.UnpackCmd(data)
	cmd := string(cmds[0])
	args := cmds[1:]
	resp := this.cmd_packer.PackCmd([][]byte{[]byte("OK")})
	switch cmd {
	case "LPUSH":
		if err := db.Lpush(args); err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		}
		conn.SendMsg(0, resp)
	case "RPUSH":
		if err := db.Rpush(args); err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		}
		conn.SendMsg(0, resp)
	case "LRANGE":
		val, err := db.Lrange(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		} else {
			resp = this.cmd_packer.PackCmd(val)
		}
		conn.SendMsg(0, resp)
	case "LPOP":
		elem, err := db.Lpop(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		} else {
			resp = this.cmd_packer.PackCmd([][]byte{elem})
		}
		conn.SendMsg(0, resp)
	case "RPOP":
		elem, err := db.Rpop(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		} else {
			resp = this.cmd_packer.PackCmd([][]byte{elem})
		}
		conn.SendMsg(0, resp)
	case "LLEN":
		len, err := db.Llen(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		} else {
			slen := fmt.Sprint(len)
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(slen)})
		}
		conn.SendMsg(0, resp)
	case "LINDEX":
		elem, err := db.Lindex(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		} else {
			resp = this.cmd_packer.PackCmd([][]byte{elem})
		}
		conn.SendMsg(0, resp)
	default:
		resp = this.cmd_packer.PackCmd([][]byte{[]byte("Unspported command")})
		conn.SendMsg(0, resp)
	}
}
