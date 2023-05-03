package siface

type SetEntry struct {
	Score float32
	Key   string
}

type IAVLTree interface {
	Add(score float32, key string)
	Remove(key string) error
	GetSize() uint32

	GetScore(key string) (score float32, err error)
	GetRank(key string) (rank uint32, err error)
	GetRangeByRank(start, end uint32) (keys []SetEntry, err error)
	GetRangeByScore(start, end float32) (keys []SetEntry, err error)
}
