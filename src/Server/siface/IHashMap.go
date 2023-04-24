package siface

type IHashMap interface {
	Get(key string) (val interface{}, err error)
	Put(key string, val interface{})
	Del(key string) error
	Lock(key string, write bool)
	Unlock(key string, write bool)
	Locks(keys []string, write bool)
	Unlocks(keys []string, write bool)

	SetTTL(key string, time int64) error
	GetTTL(key string) (int64, error)
	TtlMonitor()
}
