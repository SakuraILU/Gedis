package znet

import (
	"errors"
	"gedis/src/zinx/ziface"
	"sync"
)

type ConnectionManager struct {
	conns map[uint32]ziface.IConnection
	lock  sync.RWMutex
}

func NewConnectionManager() (cm *ConnectionManager) {
	cm = &ConnectionManager{
		conns: make(map[uint32]ziface.IConnection),
		lock:  sync.RWMutex{},
	}
	return
}

func (this *ConnectionManager) GetConn(id uint32) (conn ziface.IConnection, err error) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	conn, ok := this.conns[id]
	if !ok {
		err = errors.New("connection not found")
	}

	return
}

func (this *ConnectionManager) Add(conn ziface.IConnection) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.conns[conn.GetConnID()] = conn
}

// not only remove the element in the map, but also stop this connection to free its socket and other resources
func (this *ConnectionManager) Remove(conn ziface.IConnection) {
	this.lock.Lock()
	defer this.lock.Unlock()

	delete(this.conns, conn.GetConnID())
	// defer conn.Stop()
}

// not only clear the map, but also Stop all the connections to free sockets and other resources
func (this *ConnectionManager) ClearAll() {
	this.lock.Lock()
	defer this.lock.Unlock()

	for id, conn := range this.conns {
		delete(this.conns, id)
		go conn.Stop()
	}
}

func (this *ConnectionManager) Size() uint32 {
	this.lock.RLock()
	defer this.lock.RUnlock()

	return uint32(len(this.conns))
}
