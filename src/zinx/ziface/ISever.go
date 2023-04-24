package ziface

type IServer interface {
	// start Server
	Start()
	// stop Server
	Stop()
	// run Server
	Serve()

	// add a rounter
	AddRounter(uint32, IRouter)
	// get routerManager
	GetRouterManager() IRouterManager
	// get work pool
	GetWorkPool() IWorkPool
	// get connection manager
	GetConnectionManager() IConnectionManager

	// set onConnStartCallback
	SetOnConnStart(func(IConnection))
	// set onConnStopCallback
	SetOnConnStop(func(IConnection))
	// set onConnStartCallback
	GetOnConnStart() func(IConnection)
	// set onConnStopCallback
	GetOnConnStop() func(IConnection)
}
