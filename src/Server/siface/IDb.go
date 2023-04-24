package siface

type IDb interface {
	Open(string) error
	Close() error
	Set([][]byte) error
	Get([][]byte) (interface{}, error)
	Del([][]byte) error
	Mset([][]byte) error
	Expire([][]byte) error
	TTL([][]byte) (int64, error)
}
