package znet

import (
	"errors"
	"fmt"
	"gedis/src/zinx/ziface"
)

type RouterManager struct {
	routers map[uint32]ziface.IRouter
}

func NewRouterManager() (rt_manager *RouterManager) {
	rt_manager = &RouterManager{
		routers: make(map[uint32]ziface.IRouter),
	}
	return
}

func (this *RouterManager) AddRouter(msg_id uint32, router ziface.IRouter) {
	if _, ok := this.routers[msg_id]; ok {
		panic(fmt.Sprintf("Router id %d already exist\n", msg_id))
	}
	this.routers[msg_id] = router
}

func (this *RouterManager) ExecHandler(request ziface.IRequest) (err error) {
	msg_id := request.GetMsgId()
	router, ok := this.routers[msg_id]
	if !ok {
		err = errors.New(fmt.Sprintf("Unsupported msg id %d", msg_id))
		return
	}
	router.PreHandle(request)
	router.Handle(request)
	router.PostHandle(request)
	return
}
