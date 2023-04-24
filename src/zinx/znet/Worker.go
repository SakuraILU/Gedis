package znet

import (
	"gedis/src/zinx/utils"
	"gedis/src/zinx/ziface"
)

type Worker struct {
	id              uint32
	task_queue_size uint32
	task_queue      chan ziface.IRequest
}

func NewWorker(id uint32) (worker *Worker) {
	worker = &Worker{
		id:              id,
		task_queue_size: utils.Global_obj.TaskQueueSize,
		task_queue:      make(chan ziface.IRequest, utils.Global_obj.TaskQueueSize),
	}
	return
}

func (this *Worker) GetWorkerID() uint32 {
	return this.id
}

func (this *Worker) GetTaskQueueSize() uint32 {
	return this.task_queue_size
}

func (this *Worker) AddRequest(request ziface.IRequest) {
	// fmt.Printf("Add request (msg id %d, con id %d) to work %d\n", request.GetMsgId(), request.GetConn().GetConnID(), this.id)
	this.task_queue <- request
}

func (this *Worker) StartWork() {
	for {
		select {
		case request := <-this.task_queue:
			request.GetConn().GetRouterManager().ExecHandler(request)
		}
	}
}
