package ziface

type IWorkPool interface {
	GetPoolSize() uint32
	GetTaskQueueSize() uint32
	AddRequest(IRequest)
	StartWorkPool()
}
