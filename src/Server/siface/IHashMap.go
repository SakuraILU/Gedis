package siface

type IHashMap interface {
	Get(key string) (val interface{}, err error)
	Put(key string, val interface{})
	Del(key string) error
	Lock(string, bool)
	Unlock(string, bool)
	Locks([]string, bool)
	Unlocks([]string, bool)
}
