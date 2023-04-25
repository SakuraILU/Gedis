package siface

type IDb interface {
	Open() error
	Close() error
	Set([][]byte) error
	Get([][]byte) (interface{}, error)
	Del([][]byte) error
	Mset([][]byte) error
	Expire([][]byte) error
	TTL([][]byte) (int64, error)
	Keys([][]byte) ([]string, error)
	Persist([][]byte) error
}
