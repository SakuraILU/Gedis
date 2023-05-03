package siface

type IDb interface {
	Open() error
	Close() error
	Exec([][]byte) [][]byte
}
