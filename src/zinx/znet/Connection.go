package znet

import (
	"errors"
	"fmt"
	"gedis/src/zinx/ziface"
	"io"
	"net"
	"sync"
)

type Connection struct {
	id         uint32
	conn       net.Conn
	is_closed  bool
	close_lock sync.Mutex

	server       ziface.IServer
	rt_manager   ziface.IRouterManager
	work_pool    ziface.IWorkPool
	conn_manager ziface.IConnectionManager

	data_pack ziface.IDataPack
	msg_chan  chan []byte
	exit_chan chan bool

	onConnStart func(ziface.IConnection)
	onConnStop  func(ziface.IConnection)

	properties map[string]interface{}
	prop_lock  sync.RWMutex
}

func NewConnection(id uint32, conn net.Conn, server ziface.IServer) (connection *Connection) {
	data_pack := NewDataPack()

	connection = &Connection{
		id:        id,
		conn:      conn,
		is_closed: false,

		server:       server,
		rt_manager:   server.GetRouterManager(),
		work_pool:    server.GetWorkPool(),
		conn_manager: server.GetConnectionManager(),

		data_pack: data_pack,
		msg_chan:  make(chan []byte, 3),
		exit_chan: make(chan bool, 2),

		onConnStart: server.GetOnConnStart(),
		onConnStop:  server.GetOnConnStop(),

		properties: make(map[string]interface{}),
		prop_lock:  sync.RWMutex{},
	}

	return
}

func (this *Connection) unpackMsg() (msg ziface.IMessage, err error) {

	buf := make([]byte, this.data_pack.GetHeadLen())

	// // ReadFull reads exactly len(buf) bytes from r into buf...避免了TCP包的帧头段可能不完整的问题，more safer
	_, err = io.ReadFull(this.conn, buf) // _, err = this.conn.Read(buf)

	if err != nil {
		return
	}
	msg, err = this.data_pack.UnpackHead(buf)
	if err != nil {
		return
	}

	// ReadFull reads exactly len(buf) bytes from r into buf...避免了TCP包的数据段可能不完整的问题, more safer
	_, err = io.ReadFull(this.conn, msg.GetMsgData()) // this.conn.Read(msg.GetMsgData())
	// _, err = io.ReadFull(this.conn, msg.GetMsgData())
	if err != nil {
		return
	}

	return
}

func (this *Connection) Writer() {
end:
	for {
		select {
		case <-this.exit_chan:
			break end
		case buf := <-this.msg_chan:
			if _, err := this.conn.Write(buf); err != nil {
				break end
			}
		}
	}

	// fmt.Printf("writer of connection %d is exit...", this.id)
}

func (this *Connection) Start() {
	defer this.Stop()

	// start goes out a Writer，then serves as Reader itself
	go this.Writer()

	// on start callback, before reading from client...
	this.onConnStart(this)

	for {
		msg, err := this.unpackMsg()
		// fmt.Println("loop...")
		if errors.Is(err, io.EOF) {
			fmt.Printf("Connection %d is ended by client\n", this.id)
			break
		} else if errors.Is(err, net.ErrClosed) {
			fmt.Printf("Connection %d is ended by sever\n", this.id)
			break
		} else if err != nil {
			fmt.Println(err.Error())
			continue
		}

		request := NewRequest(this, msg)

		// 读写分离后，读不需要handler的数据了，因此无需等待handler，把handler给go出去
		go this.work_pool.AddRequest(request)
	}

}

func (this *Connection) Stop() {
	this.close_lock.Lock()
	defer this.close_lock.Unlock()

	if this.is_closed {
		return
	}
	fmt.Printf("Connection %d is stopped[Reader]\n", this.id)

	// on stop callback, before end this connection...
	this.onConnStop(this)

	// tell writer that reader is ready to exit...
	// otherwise writer may still use the closed tcp connection and cause error
	this.exit_chan <- true

	this.conn.Close()
	this.is_closed = true
	close(this.msg_chan)
	close(this.exit_chan)

	this.conn_manager.Remove(this)
}

func (this *Connection) StopPassive() {
	defer this.Stop()
	defer this.conn_manager.Remove(this)
}

func (this *Connection) GetConnID() (id uint32) {
	id = this.id
	return
}

func (this *Connection) RemoteAddr() (addr net.Addr) {
	addr = this.conn.RemoteAddr()
	return
}

func (this *Connection) SendMsg(id uint32, data []byte) (err error) {
	msg := NewMessage(id, data)
	buf, err := this.data_pack.Pack(msg)
	if err != nil {
		return
	}
	this.msg_chan <- buf
	return
}

func (this *Connection) GetRouterManager() ziface.IRouterManager {
	return this.rt_manager
}

func (this *Connection) SetProperty(key string, value interface{}) {
	this.prop_lock.Lock()
	defer this.prop_lock.Unlock()
	this.properties[key] = value
}

func (this *Connection) GetProperty(key string) (value interface{}, err error) {
	this.prop_lock.RLock()
	defer this.prop_lock.RUnlock()

	value, ok := this.properties[key]
	if !ok {
		err = errors.New(fmt.Sprintf("propery %s not found", key))
	}
	return
}

func (this *Connection) RemoveProperty(key string) {
	this.prop_lock.Lock()
	defer this.prop_lock.Unlock()

	delete(this.properties, key)
}
