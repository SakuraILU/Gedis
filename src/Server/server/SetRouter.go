package server

import (
	"fmt"
	"gedis/src/Server/siface"
	"gedis/src/zinx/ziface"
	"gedis/src/zinx/znet"
)

type SetRouter struct {
	znet.BaseRounter
	cmd_packer siface.ICmdPack
}

func NewSetRouter() *SetRouter {
	return &SetRouter{
		cmd_packer: NewCmdPack(),
	}
}

func (this *SetRouter) Handle(req ziface.IRequest) {
	conn := req.GetConn()
	buf := req.GetData()
	idb, err := conn.GetProperty("db")
	if err != nil {
		panic("db is not found")
	}
	db := idb.(siface.IDb)

	cmd_arg := this.cmd_packer.UnpackCmd(buf)
	cmd := string(cmd_arg[0])
	args := cmd_arg[1:]

	resp := this.cmd_packer.PackCmd([][]byte{[]byte("OK")})
	switch cmd {
	case "SET":
		err := db.Set(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		}
		conn.SendMsg(0, resp)
	case "GET":
		val, err := db.Get(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		} else {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(val.(string))})
		}
		conn.SendMsg(0, resp)
	case "DEL":
		if err := db.Del(args); err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		}
		conn.SendMsg(0, resp)
	case "MSET":
		if err := db.Mset(args); err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		}
		conn.SendMsg(0, resp)
	case "EXPIRE":
		if err := db.Expire(args); err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		}
		conn.SendMsg(0, resp)
	case "TTL":
		ttl, err := db.TTL(args)
		if err != nil {
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(err.Error())})
		} else {
			sttl := fmt.Sprint(ttl)
			resp = this.cmd_packer.PackCmd([][]byte{[]byte(sttl)})
		}
		conn.SendMsg(0, resp)
	}
}
