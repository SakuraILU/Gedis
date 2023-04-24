package ziface

type IConnectionManager interface {
	GetConn(uint32) (IConnection, error)
	Add(IConnection)
	Remove(IConnection)
	ClearAll()
	Size() uint32
}
