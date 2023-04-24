package siface

type ICmdPack interface {
	PackCmd([][]byte) []byte
	UnpackCmd([]byte) [][]byte
}
