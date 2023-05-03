package server

import (
	"fmt"
	"gedis/src/Server/siface"
	"math"
	"strings"
)

func max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

type avlNode struct {
	Score  float32
	Key    string
	Left   *avlNode
	Right  *avlNode
	Height int
	Size   uint32
}

func NewAvlNode(score float32, key string) *avlNode {
	return &avlNode{Score: score, Key: key, Left: nil, Right: nil, Height: 1, Size: 1}
}

func (this *avlNode) updateHeightAndSize() {
	if this.Right == nil && this.Left == nil {
		this.Height = 1
		this.Size = 1
	} else if this.Right == nil {
		this.Height = this.Left.Height + 1
		this.Size = this.Left.Size + 1
	} else if this.Left == nil {
		this.Height = this.Right.Height + 1
		this.Size = this.Right.Size + 1
	} else {
		this.Height = max(this.Left.Height, this.Right.Height) + 1
		this.Size = this.Left.Size + this.Right.Size + 1
	}
}

func (this *avlNode) diffChildsHeight() int {
	if this.Right == nil && this.Left == nil {
		return 0
	} else if this.Right == nil {
		return this.Left.Height
	} else if this.Left == nil {
		return -this.Right.Height
	} else {
		return this.Left.Height - this.Right.Height
	}
}

type AVLTree struct {
	root *avlNode
	size uint32
}

func NewAvlTree() (avl_tree *AVLTree) {
	return &AVLTree{root: nil, size: 0}
}

func (this *AVLTree) l_rotate(node *avlNode) *avlNode {
	if node.Right == nil {
		panic("right node is nil, l rotate failed")
	}

	r_node := node.Right
	node.Right = r_node.Left
	r_node.Left = node

	node.updateHeightAndSize()
	r_node.updateHeightAndSize()

	return r_node
}

func (this *AVLTree) r_rotate(node *avlNode) *avlNode {
	if node.Left == nil {
		panic("left node is nil, r rotate failed")
	}

	l_node := node.Left
	node.Left = l_node.Right
	l_node.Right = node

	node.updateHeightAndSize()
	l_node.updateHeightAndSize()

	return l_node
}

func (this *AVLTree) Add(score float32, key string) {
	if this.root == nil {
		this.root = &avlNode{Score: score, Key: key}
		return
	}

	// important bug fix
	// don't perform repeated key check in add(score float32, key string, curroot *avlNode) and try to delete the repeated key
	// because add() is traced by score, not key
	// the (score_old, key) and the new (score_new, key) may be in different branch
	// so the repeated key may not be found during the backtrace of add()
	//
	// check repeated key, if exists, delete it. this may be a little low efficient, but it's workable at least
	if _, err := this.GetScore(key); err == nil {
		err := this.Remove(key)
		if err != nil {
			panic(fmt.Sprintf("remove key %s failed", key))
		}
	}

	this.root = this.add(score, key, this.root)
}

func (this *AVLTree) balance(curroot *avlNode) *avlNode {
	defer func() {
		// assert the tree is balanced after balance()...for debug
		diff := curroot.diffChildsHeight()
		if diff <= -2 || diff >= 2 {
			panic(fmt.Sprintf("adjust fails... is not balanced at key %s with score %f, diff is %d", curroot.Key, curroot.Score, diff))
		}
	}()

	// update the height and size of curroot first, one of its subtree is changed when add/remove
	// make sure height is correct for the rotation operations
	curroot.updateHeightAndSize()

	if curroot.diffChildsHeight() >= 2 {
		l_node := curroot.Left
		// for add, diffheight shouldn't be zero...if zero, this add won't change subtree's height, won't be unbalanced
		// but for remove, it is possible...i.e. remove a node in the left tree can cause left tree's height be smaller, while right tree is maintained as before,
		// the height of right tree can be zero before of course.
		if l_node.diffChildsHeight() >= 0 {
			return this.r_rotate(curroot)
		} else {
			curroot.Left = this.l_rotate(l_node)
			return this.r_rotate(curroot)
		}
	} else if curroot.diffChildsHeight() <= -2 {
		r_node := curroot.Right
		if r_node.diffChildsHeight() <= 0 {
			return this.l_rotate(curroot)
		} else {
			curroot.Right = this.r_rotate(r_node)
			return this.l_rotate(curroot)
		}
	}

	return curroot
}

func (this *AVLTree) add(score float32, key string, curroot *avlNode) *avlNode {
	if curroot == nil {
		return NewAvlNode(score, key)
	}

	if strings.Compare(key, curroot.Key) == 0 {
		panic(fmt.Sprintf("key %s already exists, shouldn't reach here in func add()", key))
	}

	if score <= curroot.Score {
		curroot.Left = this.add(score, key, curroot.Left)
	} else if score > curroot.Score {
		curroot.Right = this.add(score, key, curroot.Right)
	}

	curroot = this.balance(curroot)
	return curroot
}

func (this *AVLTree) Remove(key string) (err error) {
	if this.root == nil {
		return fmt.Errorf("tree is empty")
	}
	this.root, err = this.remove(key, this.root)

	// assert remove successfully
	// if score, err := this.getScore(key, this.root); err == nil {
	// 	panic(fmt.Sprintf("remove %s failed, %s", key, err.Error()))
	// } else {
	// 	fmt.Printf("remove %s success, score is %f\n", key, score)
	// }
	return
}

func (this *AVLTree) getAndDelMin(curroot *avlNode) (newroot *avlNode, minroot *avlNode) {
	if curroot.Left == nil {
		return curroot.Right, curroot
	}

	curroot.Left, minroot = this.getAndDelMin(curroot.Left)
	return this.balance(curroot), minroot
}

func (this *AVLTree) remove(key string, curroot *avlNode) (newroot *avlNode, err error) {
	if curroot == nil {
		return nil, fmt.Errorf("key %s is not found", key)
	}

	if strings.Compare(key, curroot.Key) == 0 {
		if curroot.Right == nil {
			return curroot.Left, nil
		} else if curroot.Left == nil {
			return curroot.Right, nil
		} else {
			newroot, minroot := this.getAndDelMin(curroot.Right)
			curroot.Right = newroot

			curroot.Score = minroot.Score
			curroot.Key = minroot.Key

			return this.balance(curroot), nil
		}
	}

	if curroot.Left, err = this.remove(key, curroot.Left); err == nil {
		return this.balance(curroot), nil
	}

	if curroot.Right, err = this.remove(key, curroot.Right); err == nil {
		return this.balance(curroot), nil
	}

	return curroot, fmt.Errorf("key %s is not found", key)
}

func (this *AVLTree) GetSize() uint32 {
	if this.root == nil {
		return 0
	}

	return this.root.Size
}

func (this *AVLTree) GetScore(key string) (score float32, err error) {
	return this.getScore(key, this.root)
}

func (this *AVLTree) getScore(key string, curroot *avlNode) (score float32, err error) {
	if strings.Compare(key, curroot.Key) == 0 {
		return curroot.Score, nil
	}
	if curroot.Left != nil {
		if score, err = this.getScore(key, curroot.Left); err == nil {
			return score, err
		}
	}
	if curroot.Right != nil {
		if score, err = this.getScore(key, curroot.Right); err == nil {
			return score, err
		}
	}

	return -1, fmt.Errorf("%s is not found", key)
}

func (this *AVLTree) GetRank(key string) (rank uint32, err error) {
	if this.root == nil {
		return 0, fmt.Errorf("tree is empty")
	}
	return this.getRank(key, this.root)
}

func (this *AVLTree) getRank(key string, curroot *avlNode) (rank uint32, err error) {
	var currank uint32 = 0
	if curroot.Left != nil {
		currank = curroot.Left.Size
	}
	if strings.Compare(key, curroot.Key) == 0 {
		return currank, nil
	}

	if curroot.Left != nil {
		if rank, err = this.getRank(key, curroot.Left); err == nil {
			return rank, err
		}
	}
	if curroot.Right != nil {
		if rank, err = this.getRank(key, curroot.Right); err == nil {
			return currank + (rank + 1), err
		}
	}

	return math.MaxUint32, fmt.Errorf("%s is not found", key)
}

func (this *AVLTree) GetRangeByRank(start, end uint32) (entries []siface.SetEntry, err error) {
	if this.root == nil {
		return nil, fmt.Errorf("tree is empty")
	}

	entries = this.getRangeByRank(int(start), int(end), this.root)
	if len(entries) == 0 {
		err = fmt.Errorf("empty array")
	}
	return entries, err
}

func (this *AVLTree) getRangeByRank(start, end int, curroot *avlNode) (entries []siface.SetEntry) {
	// because subtract operation of index is needed, so start and end is typed as int
	if curroot == nil {
		return []siface.SetEntry{}
	}
	if start > end {
		return []siface.SetEntry{}
	}

	currank := 0
	if curroot.Left != nil {
		currank = int(curroot.Left.Size)
	}
	if start <= currank && end >= currank {
		entries = this.getRangeByRank(start, currank-1, curroot.Left)
		entries = append(entries, siface.SetEntry{Key: curroot.Key, Score: curroot.Score})
		entries = append(entries, this.getRangeByRank(0, end-(currank+1), curroot.Right)...)
	} else if end < currank {
		entries = this.getRangeByRank(start, end, curroot.Left)
	} else if start > currank {
		entries = this.getRangeByRank(start-(currank+1), end-(currank+1), curroot.Right)
	}
	return
}

func (this *AVLTree) GetRangeByScore(start, end float32) (entries []siface.SetEntry, err error) {
	if this.root == nil {
		return nil, fmt.Errorf("tree is empty")
	}

	entries = this.getRangeByScore(start, end, this.root)
	if len(entries) == 0 {
		err = fmt.Errorf("empty array")
	}
	return entries, err
}

func (this *AVLTree) getRangeByScore(start, end float32, curroot *avlNode) (entries []siface.SetEntry) {
	if curroot == nil {
		return []siface.SetEntry{}
	}
	if start > end {
		return []siface.SetEntry{}
	}

	// important bug fix:
	// rank is relative to the left most node. When searching the right subtree, left subtree and curnode is ignored,
	// so the rank of the right subtree is relative to the left most node (currank + 1 before, now it is 0 rank) of the right subtree, not the whole tree,
	// thus we need to subtract the rank of current node when searching the right subtree, pass [start- (currank + 1), end - (currank + 1)]
	// BUT
	// while score is fixed in each node, don't need to subtract any value when searching subtrees, just pass [start, end]

	curscore := curroot.Score
	if start <= curscore && end >= curscore {
		entries = this.getRangeByScore(start, curscore, curroot.Left)
		entries = append(entries, siface.SetEntry{Key: curroot.Key, Score: curroot.Score})
		entries = append(entries, this.getRangeByScore(curscore, end, curroot.Right)...)
	} else if end < curscore {
		entries = this.getRangeByScore(start, end, curroot.Left)
	} else if start > curscore {
		entries = this.getRangeByScore(start, end, curroot.Right)
	}
	return
}

// for debug...
func (this *AVLTree) Visualize() {
	if this.root == nil {
		fmt.Printf("tree is empty\n")
		return
	}

	//BFS, output the tree like [[root], [left, right], [left, right], ...]
	queue := make([]*avlNode, 0)
	queue = append(queue, this.root)
	fmt.Printf("[%s] ", this.root.Key)
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.Left != nil {
			queue = append(queue, cur.Left)
		}
		if cur.Right != nil {
			queue = append(queue, cur.Right)
		}
		// output [left.key, right.key], if nul, output null
		outstr := ""
		if cur.Left != nil {
			outstr += cur.Left.Key
		} else {
			outstr += "null"
		}
		outstr += ", "
		if cur.Right != nil {
			outstr += cur.Right.Key
		} else {
			outstr += "null"
		}
		fmt.Printf("[%s] ", outstr)
	}
	fmt.Printf("\n")
}
