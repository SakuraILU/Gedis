package siface

type SetEntry struct {
	Score float64
	Key   string
}

type IAVLTree interface {
	Add(score float64, key string)
	Remove(key string) error
	GetSize() uint32

	GetScore(key string) (score float64, err error)
	GetRank(key string) (rank uint32, err error)
	GetRangeByRank(start, end uint32) (entries []SetEntry)
	GetRangeByScore(start, end float64) (entries []SetEntry)
}
