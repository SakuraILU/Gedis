package siface

type IEngine interface {
	Start()
	Stop()

	Handle([]string) []string
}
