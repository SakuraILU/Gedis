package siface

type IHashMap interface {
	Get(key string) (val interface{}, err error)
	Put(key string, val interface{})
	Del(key string) error
	Lock(key string, write bool)
	Unlock(key string, write bool)
	Locks(keys []string, write bool)
	Unlocks(keys []string, write bool)
}
