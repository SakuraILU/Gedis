package siface

type IDbManager interface {
	Start()
	Stop()
	GetDb(id uint32)
}
