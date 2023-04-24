package znet

import (
	"gedis/src/zinx/utils"
	"gedis/src/zinx/ziface"
)

type WorkPool struct {
	pool_size       uint32
	task_queue_size uint32
	works           []ziface.IWroker

	started bool
}

func NewWorkPool() (work_pool *WorkPool) {
	work_pool = &WorkPool{
		pool_size:       utils.Global_obj.PoolSize,
		task_queue_size: utils.Global_obj.TaskQueueSize,
		works:           make([]ziface.IWroker, utils.Global_obj.PoolSize),

		started: false,
	}

	return
}

func (this *WorkPool) GetPoolSize() uint32 {
	return this.pool_size
}
func (this *WorkPool) GetTaskQueueSize() uint32 {
	return this.task_queue_size
}

func (this *WorkPool) StartWorkPool() {
	if this.started {
		return
	}

	for id := 0; id < int(this.pool_size); id++ {
		this.works[id] = NewWorker(uint32(id))
		go this.works[id].StartWork()
	}
	this.started = true
}

func (this *WorkPool) AddRequest(request ziface.IRequest) {
	if !this.started {
		panic("Work pool is not started, while try to add request...")
	}
	id := request.GetConn().GetConnID() % this.pool_size
	this.works[id].AddRequest(request)
}
