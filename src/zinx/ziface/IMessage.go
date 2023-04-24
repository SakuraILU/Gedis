package ziface

type IMessage interface {
	GetMsgID() uint32
	GetMsgData() []byte
	GetDataLen() uint32

	SetDataLen(uint32)
	SetMsgID(uint32)
	SetMsgData([]byte)
}
