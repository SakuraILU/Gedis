package znet

import "gedis/src/zinx/ziface"

type Request struct {
	conn ziface.IConnection
	msg  ziface.IMessage
}

func NewRequest(conn ziface.IConnection, msg ziface.IMessage) (request *Request) {
	request = &Request{
		conn: conn,
		msg:  msg,
	}
	return
}

func (this *Request) GetConn() (conn ziface.IConnection) {
	conn = this.conn
	return
}

func (this *Request) GetData() (data []byte) {
	data = this.msg.GetMsgData()
	return
}

func (this *Request) GetDataLen() uint32 {
	return this.msg.GetDataLen()
}

func (this *Request) GetMsgId() uint32 {
	return this.msg.GetMsgID()
}
