package znet

import "gedis/src/zinx/ziface"

type Message struct {
	len  uint32
	id   uint32
	data []byte
}

func NewMessage(id uint32, data []byte) (msg ziface.IMessage) {
	msg = &Message{
		len:  uint32(len(data)),
		id:   id,
		data: data,
	}
	return
}

func (this *Message) GetMsgID() (id uint32) {
	id = this.id
	return
}

func (this *Message) GetMsgData() (data []byte) {
	data = this.data
	return
}

func (this *Message) GetDataLen() (len uint32) {
	len = this.len
	return
}

func (this *Message) SetMsgID(id uint32) {
	this.id = id
}

func (this *Message) SetMsgData(data []byte) {
	this.data = data
}

func (this *Message) SetDataLen(len uint32) {
	this.len = len
}
