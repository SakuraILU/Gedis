package ziface

type IRequest interface {
	GetConn() IConnection
	GetDataLen() uint32
	GetMsgId() uint32
	GetData() []byte
}
