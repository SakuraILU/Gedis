package znet

import (
	"fmt"
	"gedis/src/zinx/utils"
	"gedis/src/zinx/ziface"
	"net"
	"os"
	"os/signal"
)

// implement of IServer interface, the Server mode
type Server struct {
	name string

	ip_version string
	ip         string
	port       uint32

	rt_manager   ziface.IRouterManager
	work_pool    ziface.IWorkPool
	conn_manager ziface.IConnectionManager

	onConnStart func(ziface.IConnection)
	onConnStop  func(ziface.IConnection)
}

func NewServer() (server *Server) {
	server = &Server{
		name:         utils.Global_obj.Name,
		ip_version:   "tcp4",
		ip:           utils.Global_obj.Ip,
		port:         utils.Global_obj.Port,
		rt_manager:   NewRouterManager(),
		work_pool:    NewWorkPool(),
		conn_manager: NewConnectionManager(),

		onConnStart: func(i ziface.IConnection) {},
		onConnStop:  func(i ziface.IConnection) {},
	}

	return
}

func (this *Server) Start() {
	this.work_pool.StartWorkPool()

	fmt.Printf("start server %s at (%s: %d)\n", this.name, this.ip, this.port)
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", this.ip, this.port))
	if err != nil {
		panic(err.Error())
	}

	var conn_id uint32 = 0
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		// fmt.Printf("size of connections is %d\n", this.conn_manager.Size())
		if this.conn_manager.Size() >= utils.Global_obj.MaxConnSize {
			conn.Close()
			continue
		}

		connection := NewConnection(conn_id, conn, this)
		this.conn_manager.Add(connection)
		go connection.Start()

		conn_id++
	}
}

func (this *Server) Stop() {
	this.conn_manager.ClearAll()
}

func (this *Server) Serve() {
	go this.Start()
	defer this.Stop()

	this.waitExitSig()
}

func (this *Server) AddRounter(msg_id uint32, router ziface.IRouter) {
	this.rt_manager.AddRouter(msg_id, router)
}

func (this *Server) GetRouterManager() ziface.IRouterManager {
	return this.rt_manager
}

func (this *Server) GetWorkPool() ziface.IWorkPool {
	return this.work_pool
}

func (this *Server) GetConnectionManager() ziface.IConnectionManager {
	return this.conn_manager
}

func (this *Server) SetOnConnStart(fun func(ziface.IConnection)) {
	this.onConnStart = fun
}
func (this *Server) SetOnConnStop(fun func(ziface.IConnection)) {
	this.onConnStop = fun
}

func (this *Server) GetOnConnStart() func(ziface.IConnection) {
	return this.onConnStart
}
func (this *Server) GetOnConnStop() func(ziface.IConnection) {
	return this.onConnStop
}

func (this *Server) waitExitSig() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Kill, os.Interrupt)

	for {
		select {
		case sig := <-c:
			{
				fmt.Printf("Got sig %s\n", sig.String())
				switch sig {
				case os.Kill, os.Interrupt:
					{
						return
					}
				default:
					{
						fmt.Printf("Got sig %s, use sigterm, sigkill or sigquit to quit the server\n", sig.String())
					}
				}
			}
		}
	}
}
