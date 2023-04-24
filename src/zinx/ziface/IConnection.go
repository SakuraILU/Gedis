package ziface

import (
	"net"
)

type IConnection interface {
	Start()
	Stop()
	StopPassive()

	GetConnID() uint32
	RemoteAddr() net.Addr
	SendMsg(id uint32, data []byte) error

	GetRouterManager() IRouterManager

	//设置链接属性
	SetProperty(key string, value interface{})
	//获取链接属性
	GetProperty(key string) (interface{}, error)
	//移除链接属性
	RemoveProperty(key string)
}
