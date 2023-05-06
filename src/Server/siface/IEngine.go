package siface

type IEngine interface {
	Start()
	Stop()

	Handle([]string) []string
	Foreach(func(key string, val interface{}, TTL int64))
}
