package ziface

type IDataPack interface {
	Pack(IMessage) ([]byte, error)
	UnpackHead([]byte) (IMessage, error)

	GetHeadLen() uint32
}
