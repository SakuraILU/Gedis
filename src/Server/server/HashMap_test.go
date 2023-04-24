package server_test

import (
	"fmt"
	"gedis/src/Server/server"
	"sync"
	"testing"
)

// 低压力测试，无并发读写
func Test1(t *testing.T) {
	m := server.NewHashMap(25)
	m.Put("key", "value")
	val, err := m.Get("key")
	if err != nil {
		t.Error("Test1 failed")
	}
	if val.(string) != "value" {
		t.Error("Test1 failed")
	}
}

// 低压力测试，插入和删除
func Test2(t *testing.T) {
	m := server.NewHashMap(25)
	m.Put("key1", "value1")
	m.Put("key2", "value2")
	val, err := m.Get("key1")
	if err != nil {
		t.Error("Test2 failed")
	}
	if val.(string) != "value1" {
		t.Error("Test2 failed")
	}
	m.Del("key2")
	val, err = m.Get("key2")
	if err == nil {
		t.Error("Test2 failed")
	}
	val, err = m.Get("key1")
	if err != nil {
		t.Error("Test2 failed")
	}
}

// 高压力测试，有并发读写
func Test3(t *testing.T) {
	m := server.NewHashMap(256)
	// 生成一些key-value pair
	k_vs := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("value%d", i)
		k_vs[key] = val
	}

	// 十万个并发读写协程
	for i := 0; i < 100000; i++ {
		// 不断地往map里面写入k-v对
		go func() {
			for k, v := range k_vs {
				m.Lock(k, true)
				m.Put(k, v)
				m.Unlock(k, true)
			}
		}()
		// 不断地从map里面读取k-v对，直到所有的k-v对都被读取才算成功
		go func() {
			// 深拷贝一份k-v对，用于后面的检查
			k_vs_cpy := make(map[string]string)
			for k, v := range k_vs {
				k_vs_cpy[k] = v
			}
			for len(k_vs_cpy) > 0 {
				for k, _ := range k_vs_cpy {
					m.Lock(k, false)
					val, err := m.Get(k)
					if err != nil {
						continue
					}
					if val.(string) != k_vs_cpy[k] {
						continue
					}
					m.Unlock(k, false)
					delete(k_vs_cpy, k)
				}
			}
		}()
	}
}

// 高压力测试，有并发，同时读写多个key
func Test4(t *testing.T) {
	m := server.NewHashMap(256)
	// 生成一些key-value pair
	k_vs := make(map[string]string)
	lock := sync.Mutex{}
	lock_cpy := sync.Mutex{}
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("value%d", i)
		k_vs[key] = val
	}

	for i := 0; i < 2000; i++ {
		// 不断地往map里面写入k-v对
		go func() {
			// 深拷贝一份k-v对，用于每次写随机选择10个
			k_vs_cpy := make(map[string]string)
			for k, v := range k_vs {
				k_vs_cpy[k] = v
			}
			for len(k_vs_cpy) > 0 {
				// 生成一些key
				keys := make([]string, 0, 10)
				for k, _ := range k_vs_cpy {
					keys = append(keys, k)
					if len(keys) == 10 {
						break
					}
				}
				m.Locks(keys, true)
				for _, k := range keys {
					m.Put(k, k_vs_cpy[k])
					lock.Lock()
					delete(k_vs_cpy, k)
					lock.Unlock()
				}
				m.Unlocks(keys, true)
			}
		}()
		// 不断地从map里面读取k-v对，直到所有的k-v对都被读取才算成功
		go func() {
			// 深拷贝一份k-v对，用于后面的检查
			k_vs_cpy := make(map[string]string)
			for k, v := range k_vs {
				k_vs_cpy[k] = v
			}
			for len(k_vs_cpy) > 0 {
				// 生成一些key
				keys := make([]string, 0, 10)
				for k, _ := range k_vs_cpy {
					keys = append(keys, k)
					if len(keys) == 10 {
						break
					}
				}
				m.Locks(keys, false)
				for _, k := range keys {
					val, err := m.Get(k)
					if err != nil {
						continue
					}
					if val.(string) != k_vs_cpy[k] {
						continue
					}
					lock_cpy.Lock()
					delete(k_vs_cpy, k)
					lock_cpy.Unlock()
				}
				m.Unlocks(keys, false)
			}
		}()
	}
}

// 高压力测试，并发写入和删除，最后检查删除是否正确
func Test5(t *testing.T) {
	pnum := 100   // put goroutine num
	dnum := 10000 // delete goroutine num

	m := server.NewHashMap(256)
	// 生成一些key-value pair
	k_vs := make(map[string]string)
	for i := 0; i < 200000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("value%d", i)
		k_vs[key] = val
	}

	// generate num subsets from k_vs，作为待删除的key，之后每个协程删一个子集
	k_vs_dels := make([]map[string]string, dnum)
	for i := 0; i < dnum; i++ {
		k_vs_dels[i] = make(map[string]string)
	}
	i := 0
	for k, v := range k_vs {
		k_vs_dels[i][k] = v
		i = (i + 1) % dnum
	}

	wg := sync.WaitGroup{}
	wg.Add(pnum + dnum - 1)
	// 高点并发，pnum个goroutine不断地往map里面写入所有的k-v对，当然事实的结果和一个goroutine写入是一样的
	for i := 0; i < pnum; i++ {
		go func() {
			// 深拷贝一份k-v对，用于每次写随机选择10个
			k_vs_cpy := make(map[string]string)
			for k, v := range k_vs {
				k_vs_cpy[k] = v
			}
			for len(k_vs_cpy) > 0 {
				// 生成一些key
				keys := make([]string, 0, 10)
				for k, _ := range k_vs_cpy {
					keys = append(keys, k)
					if len(keys) == 10 {
						break
					}
				}
				m.Locks(keys, true)
				for _, k := range keys {
					m.Put(k, k_vs_cpy[k])
					delete(k_vs_cpy, k)
				}
				m.Unlocks(keys, true)
			}
			wg.Done()
		}()
	}

	// (ndnum-1)个goroutine依次负责删除各个k_vs_dels中一个子集的k-v对，
	// 保留最后一个子集用于检查是否正确删除应该删除的k-v对并留下未删除的k-v对
	for id := 0; id < dnum-1; id++ {
		go func(id int) {
			k_vs_del := k_vs_dels[id]
			for len(k_vs_del) > 0 {
				// 生成一些key
				keys := make([]string, 0, 10)
				for k, _ := range k_vs_del {
					keys = append(keys, k)
					if len(keys) == 10 {
						break
					}
				}
				m.Locks(keys, true)
				for _, k := range keys {
					m.Del(k)
					delete(k_vs_del, k)
				}
				m.Unlocks(keys, true)
			}
			wg.Done()
		}(id)
	}
	wg.Wait()

	// 应该只有kv_dels[last]中的k-v对还在map中
	for k, v := range k_vs_dels[dnum-1] {
		val, err := m.Get(k)
		if err != nil {
			t.Error("Test5 failed")
		}
		if val.(string) != v {
			t.Error("Test5 failed")
		}
	}
}
