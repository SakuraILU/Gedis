package siface

type IDb interface {
	Open() error
	Close() error
	Set([][]byte) error
	Get([][]byte) ([]byte, error)
	Del([][]byte) error
	Mset([][]byte) error

	Lpush([][]byte) error
	Rpush([][]byte) error
	Lpop([][]byte) ([]byte, error)
	Rpop([][]byte) ([]byte, error)
	Lrange([][]byte) ([][]byte, error)
	Llen([][]byte) (uint32, error)
	Lindex([][]byte) ([]byte, error)

	Expire([][]byte) error
	TTL([][]byte) (int64, error)
	Keys([][]byte) ([]string, error)
	Persist([][]byte) error
}
