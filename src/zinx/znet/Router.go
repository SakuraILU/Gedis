package znet

import "gedis/src/zinx/ziface"

type BaseRounter struct{}

func (this *BaseRounter) PreHandle(ziface.IRequest)  {}
func (this *BaseRounter) Handle(ziface.IRequest)     {}
func (this *BaseRounter) PostHandle(ziface.IRequest) {}
