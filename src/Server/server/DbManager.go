package server

import "gedis/src/Server/siface"

type DbManager struct {
	dbs []siface.IDb
}

func NewDbManager() *DbManager {
	db_mgr := &DbManager{
		dbs: make([]siface.IDb, 16),
	}
	for i := 0; i < 16; i++ {
		db_mgr.dbs[i] = NewDb(string(i))
	}
	return db_mgr
}

func (this *DbManager) Start() {
	for _, db := range this.dbs {
		db.Open()
	}
}

func (this *DbManager) Stop() {
	for _, db := range this.dbs {
		db.Close()
	}
}

func (this *DbManager) GetDb(id uint32) siface.IDb {
	return this.dbs[id]
}
