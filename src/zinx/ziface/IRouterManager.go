package ziface

type IRouterManager interface {
	AddRouter(msg_id uint32, router IRouter)
	ExecHandler(IRequest) error
}
