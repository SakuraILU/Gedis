package siface

type IDataPack interface {
	PackMsg([][]byte)
	UnpackMsg() [][]byte
}
