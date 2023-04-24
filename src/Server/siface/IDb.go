package siface

type IDb interface {
	Open(string) error
	Close() error
	Set([][]byte)
	Get([][]byte) (interface{}, error)
}
