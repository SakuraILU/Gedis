package ziface

type IWroker interface {
	GetWorkerID() uint32
	GetTaskQueueSize() uint32
	AddRequest(IRequest)
	StartWork()
}
