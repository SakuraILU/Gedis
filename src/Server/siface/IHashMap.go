package siface

type IHashMap interface {
	Lock(key string, write bool)
	Unlock(key string, write bool)
	Locks(keys []string, write bool)
	Unlocks(keys []string, write bool)

	Put(key string, val interface{})
	Del(key string) error
	Get(key string) (val interface{}, err error)
	GetString(key string) (val string, err error)
	GetList(key string, create bool) (val []string, err error)
	FindWithLock(pattern string) (keys []string, err error)

	SetTTL(key string, time int64) error
	GetTTL(key string) (int64, error)
	Persist(key string) error
	TtlMonitor()
	StopTtlMonitor()
}
