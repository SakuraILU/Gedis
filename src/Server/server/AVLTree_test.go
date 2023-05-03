package server_test

import (
	"fmt"
	"gedis/src/Server/server"
	"math/rand"
	"testing"
)

// test Add and GetScore
func TestAvl1(t *testing.T) {
	tree := server.NewAvlTree()
	// generate and add some (score, key) pair
	num := 1000
	for i := 0; i < num; i++ {
		tree.Add(float64(i), "key"+fmt.Sprint(i))
	}
	// get size
	if tree.GetSize() != uint32(num) {
		fmt.Println(tree.GetSize())
		t.Error("TestAvl1 failed")
	}
	// get score
	for i := 0; i < num; i++ {
		score, err := tree.GetScore("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl1 failed")
		}
		if score != float64(i) {
			t.Error("TestAvl1 failed")
		}
	}
}

// test Add, Remove and Get
func TestAvl2(t *testing.T) {
	tree := server.NewAvlTree()
	// generate and add some (score, key) pair
	num := 1000
	dnum := 300 // delete [0:dnum)
	for i := 0; i < num; i++ {
		tree.Add(float64(i), "key"+fmt.Sprint(i))
	}
	// remove a subset of (score, key) pair
	for i := 0; i < dnum; i++ {
		err := tree.Remove("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl2 failed")
		}
	}
	// get size
	if tree.GetSize() != uint32(num-dnum) {
		fmt.Printf("%d %d\n", tree.GetSize(), num-dnum)
		t.Error("TestAvl2 failed")
	}
	// get score
	for i := dnum; i < num; i++ {
		score, err := tree.GetScore("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl2 failed")
		}
		if score != float64(i) {
			t.Error("TestAvl2 failed")
		}
	}
	// get score of deleted key, should return error
	for i := 0; i < dnum; i++ {
		_, err := tree.GetScore("key" + fmt.Sprint(i))
		if err == nil {
			t.Error("TestAvl2 failed")
		}
	}

	// get score of non-exist key, should return error
	_, err := tree.GetScore("key" + fmt.Sprint(num))
	if err == nil {
		t.Error("TestAvl2 failed")
	}
}

// test Add, Remove and GetRank
func TestAvl3(t *testing.T) {
	tree := server.NewAvlTree()
	// generate and add some (score, key) pair
	num := 1000
	dnum := 300 // delete [0:dnum)
	for i := 0; i < num; i++ {
		tree.Add(float64(i), "key"+fmt.Sprint(i))
	}
	// remove a subset of (score, key) pair
	for i := 0; i < dnum; i++ {
		err := tree.Remove("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl3 failed")
		}
	}

	// get size
	if tree.GetSize() != uint32(num-dnum) {
		fmt.Printf("%d %d\n", tree.GetSize(), num-dnum)
		t.Error("TestAvl3 failed")
	}

	// get rank
	for i := dnum; i < num; i++ {
		rank, err := tree.GetRank("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl3 failed")
		}
		if rank != uint32(i-dnum) {
			fmt.Printf("%d %d\n", rank, i-dnum)
			t.Error("TestAvl3 failed")
		}
	}
	// get rank of deleted key, should return error
	for i := 0; i < dnum; i++ {
		_, err := tree.GetRank("key" + fmt.Sprint(i))
		if err == nil {
			t.Error("TestAvl3 failed")
		}
	}

	// get rank of non-exist key, should return error
	_, err := tree.GetRank("key" + fmt.Sprint(num))
	if err == nil {
		t.Error("TestAvl3 failed")
	}
}

// test Add, Remove and GetRangeByRank
func TestAvl4(t *testing.T) {
	tree := server.NewAvlTree()
	// generate and add some (score, key) pair
	num := 1000
	for i := 0; i < num; i++ {
		tree.Add(float64(i), "key"+fmt.Sprint(i))
	}
	// get range
	keys := tree.GetRangeByRank(0, uint32(num)+100) // test: larger than actual range limit..
	if len(keys) != num {
		t.Error("TestAvl4 failed")
	}
	for i := 0; i < num; i++ {
		fmt.Printf("%s %s\n", keys[i].Key, "key"+fmt.Sprint(i))
		if keys[i].Key != "key"+fmt.Sprint(i) {
			t.Error("TestAvl4 failed")
		}
	}
}

// test Add, Remove and GetRangeByScore
func TestAvl5(t *testing.T) {
	tree := server.NewAvlTree()
	// generate and add some (score, key) pair
	num := 1000
	for i := 0; i < num; i++ {
		tree.Add(float64(i)/float64(num), "key"+fmt.Sprint(i)) // rerange to [0,1]
	}
	// get range
	keys := tree.GetRangeByScore(0, 5) // test: larger than actual range limit..
	if len(keys) != num {
		t.Error("TestAvl5 failed")
	}
	for i := 0; i < num; i++ {
		if keys[i].Key != "key"+fmt.Sprint(i) {
			t.Error("TestAvl5 failed")
		}
	}
}

// test add, remove and then add again with different score
func TestAvl6(t *testing.T) {
	tree := server.NewAvlTree()
	// generate and add some (score, key) pair
	num := 1000
	for i := 0; i < num; i++ {
		tree.Add(float64(i), "key"+fmt.Sprint(i))
	}
	// get size
	if tree.GetSize() != uint32(num) {
		t.Error("TestAvl6 failed")
	}
	// get score
	for i := 0; i < num; i++ {
		score, err := tree.GetScore("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl6 failed")
		}
		if score != float64(i) {
			t.Error("TestAvl6 failed")
		}
	}

	// add again with different score
	for i := 0; i < num; i++ {
		tree.Add(2*float64(i), "key"+fmt.Sprint(i))
	}
	// get size
	if tree.GetSize() != uint32(num) {
		fmt.Printf("%d %d\n", tree.GetSize(), num)
		t.Error("TestAvl6 failed")
	}
	// get score
	for i := 0; i < num; i++ {
		score, err := tree.GetScore("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl6 failed")
		}
		if score != 2*float64(i) {
			t.Error("TestAvl6 failed")
		}
	}
}

// test add in disorder (randomly), then get range should be in order
func TestAvl7(t *testing.T) {
	tree := server.NewAvlTree()
	// generate and add some (score, key) pair
	num := 513
	dnum := 256
	// add in reverse order
	for i := num - 1; i >= 0; i-- {
		tree.Add(float64(i), "key"+fmt.Sprint(i))
	}
	// add in order
	for i := 0; i < num; i++ {
		tree.Add(float64(i), "key"+fmt.Sprint(i))
	}
	// add in disorder
	perm := rand.Perm(num)
	for i := 0; i < num; i++ {
		tree.Add(float64(perm[i]), "key"+fmt.Sprint(perm[i]))
	}
	// get range
	keys := tree.GetRangeByRank(0, uint32(num)+100) // test: larger than actual range limit..
	fmt.Printf("length of key is %d\n", len(keys))
	if len(keys) != num {
		t.Error("TestAvl7 failed")
	}
	for i := 0; i < num; i++ {
		if keys[i].Key != "key"+fmt.Sprint(i) {
			t.Error("TestAvl7 failed")
		}
	}

	// delete [0, dnum) keys]
	fmt.Printf("delete [0, %d) keys\n", dnum)
	for i := 0; i < dnum; i++ {
		tree.Remove("key" + fmt.Sprint(i))
	}
	// get range
	keys = tree.GetRangeByRank(0, uint32(num)+100) // test: larger than actual range limit..
	if len(keys) != num-dnum {
		fmt.Printf("len is %d", len(keys))
		t.Error("TestAvl7 failed")
	}
	for i := 0; i < num-dnum; i++ {
		if keys[i].Key != "key"+fmt.Sprint(i+dnum) {
			t.Error("TestAvl7 failed")
		}
	}

	// add [0, num] in disorder again
	perm = rand.Perm(num)
	for i := 0; i < num; i++ {
		tree.Add(float64(perm[i]), "key"+fmt.Sprint(perm[i]))
	}
	// get range
	keys = tree.GetRangeByRank(0, uint32(num)+100) // test: larger than actual range limit..
	if len(keys) != num {
		t.Error("TestAvl7 failed")
	}
	for i := 0; i < num; i++ {
		if keys[i].Key != "key"+fmt.Sprint(i) {
			t.Error("TestAvl7 failed")
		}
	}
}

// randomly delete and add, then get range should be in order
func TestAvl8(t *testing.T) {
	// generate and add some (score, key) pair randomly
	tree := server.NewAvlTree()

	num := 1000
	perm := rand.Perm(num)
	for i := 0; i < num; i++ {
		tree.Add(float64(perm[i]), "key"+fmt.Sprint(perm[i]))
	}
	// get size
	if tree.GetSize() != uint32(num) {
		t.Error("TestAvl8 failed")
	}
	// get score
	for i := 0; i < num; i++ {
		score, err := tree.GetScore("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl8 failed")
		}
		if score != float64(i) {
			t.Error("TestAvl8 failed")
		}
	}

	// remove some keys, randomly [42,3,56,2...]
	dnum := 200
	dpos := make([]int, 0)
	for i := 0; i < dnum; i++ {
		// no duplicate
		num := rand.Intn(num)
		for j := 0; j < len(dpos); j++ {
			if num == dpos[j] {
				num = rand.Intn(num)
				j = 0
			}
		}
		dpos = append(dpos, num)
	}
	for i := 0; i < dnum; i++ {
		tree.Remove("key" + fmt.Sprint(dpos[i]))
	}
	// get size
	if tree.GetSize() != uint32(num-dnum) {
		fmt.Printf("size is %d\n", tree.GetSize())
		t.Error("TestAvl8 failed")
	}
	// get range [0, num)
	keys := tree.GetRangeByRank(0, uint32(num)+100) // test: larger than actual range limit..
	if len(keys) != num-dnum {
		t.Error("TestAvl8 failed")
	}
	// check if keys are in order
	for i := 0; i < num-dnum-1; i++ {
		if keys[i].Score > keys[i+1].Score {
			t.Error("TestAvl8 failed")
		}
	}
	// get range by score should also be in order
	keys = tree.GetRangeByScore(float64(0), float64(num)+100) // test: larger than actual range limit..
	if len(keys) != num-dnum {
		t.Error("TestAvl8 failed")
	}
	// check if keys are in order
	for i := 0; i < num-dnum-1; i++ {
		if keys[i].Score > keys[i+1].Score {
			t.Error("TestAvl8 failed")
		}
	}

	// choose some keys in dpos to add back
	anum := 100
	apos := make([]int, 0)
	for i := 0; i < anum; i++ {
		// no duplicate
		num := rand.Intn(dnum)
		for j := 0; j < len(apos); j++ {
			if num == apos[j] {
				num = rand.Intn(dnum)
				j = 0
			}
		}
		apos = append(apos, num)
	}
	for i := 0; i < anum; i++ {
		tree.Add(float64(dpos[apos[i]]), "key"+fmt.Sprint(dpos[apos[i]]))
	}
	// get size
	if tree.GetSize() != uint32(num+anum-dnum) {
		t.Error("TestAvl8 failed")
	}
	// get range [0, num)
	keys = tree.GetRangeByRank(0, uint32(num)+100) // test: larger than actual range limit..
	if len(keys) != num+anum-dnum {
		t.Error("TestAvl8 failed")
	}
	// check if keys are in order
	for i := 0; i < num+anum-dnum-1; i++ {
		if keys[i].Score > keys[i+1].Score {
			t.Error("TestAvl8 failed")
		}
	}
}

// test add and partial get range and get range by score
func TestAvl9(t *testing.T) {
	// generate and add some (score, key) pair randomly
	tree := server.NewAvlTree()

	num := 1000
	perm := rand.Perm(num)
	for i := 0; i < num; i++ {
		tree.Add(float64(perm[i]), "key"+fmt.Sprint(perm[i]))
	}
	perm = rand.Perm(num) // random add again, for fun
	for i := 0; i < num; i++ {
		tree.Add(float64(perm[i]), "key"+fmt.Sprint(perm[i]))
	}
	// get size
	if tree.GetSize() != uint32(num) {
		t.Error("TestAvl9 failed")
	}
	// get score
	for i := 0; i < num; i++ {
		score, err := tree.GetScore("key" + fmt.Sprint(i))
		if err != nil {
			t.Error("TestAvl9 failed")
		}
		if score != float64(i) {
			t.Error("TestAvl9 failed")
		}
	}

	// get range [ar, br)
	ar := 200
	br := 400
	keys := tree.GetRangeByRank(uint32(ar), uint32(br))
	if len(keys) != br-ar+1 {
		t.Error("TestAvl9 failed")
	}
	// check if keys are in order
	for i := 0; i < br-ar-1; i++ {
		// fmt.Printf("score of key0 is %f, score of key2 is %f\n", keys[i].Score, keys[i+1].Score)
		if keys[i].Score > keys[i+1].Score {
			t.Error("TestAvl9 failed")
		}
	}
	// get range by score should also be in order
	keys = tree.GetRangeByScore(float64(ar), float64(br))
	if len(keys) != br-ar+1 {
		t.Error("TestAvl9 failed")
	}
	// check if keys are in order
	for i := 0; i < br-ar-1; i++ {
		fmt.Printf("score of key0 is %f, score of key2 is %f\n", keys[i].Score, keys[i+1].Score)
		if keys[i].Score > keys[i+1].Score {
			t.Error("TestAvl9 failed")
		}
	}
}
