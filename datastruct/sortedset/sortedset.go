package sortedset

import (
	"math/rand"
	"time"
)

const (
	maxLevel = 16 // 跳表的最大层数
)

// Element 是一个包含成员和分数的结构体
type Element struct {
	Member string  // 成员
	Score  float64 // 分数
}

// Level 是跳表中的一层，包含指向下一节点的指针和该层的跨度
type Level struct {
	forward *node // 指向下一节点的指针
	span    int64 // 当前层的跨度，表示从当前节点到下一个节点的距离
}

// node 是跳表中的节点数据结构，包含元素数据和多个层级的 Level 信息
type node struct {
	Element           // 继承 Element，包含成员和分数
	backward *node    // 指向前一个节点的指针
	level    []*Level // 当前节点的多个 Level
}

// SortedSet 是通过跳表实现的有序集合
type SortedSet struct {
	header *node // 跳表的头节点
	tail   *node // 跳表的尾节点
	length int64 // 跳表中元素的数量
	level  int   // 当前跳表的最大层数
}

// Make 创建一个新的有序集合实例
func Make() *SortedSet {
	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	sortedSet := &SortedSet{
		level: 1, // 初始设置跳表的层数为 1
	}

	// 初始化头节点
	sortedSet.header = &node{
		level: make([]*Level, maxLevel), // 为头节点初始化 maxLevel 个 Level
	}
	for i := 0; i < maxLevel; i++ {
		sortedSet.header.level[i] = &Level{} // 每一层初始化 Level
	}

	return sortedSet
}

// randomLevel 返回一个随机的跳表层数
func randomLevel() int {
	level := 1 // 初始层数为 1
	// 使用概率控制跳表层数，概率为 0.25
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
		// 如果超过最大层数，返回最大层数
		if level >= maxLevel {
			return maxLevel
		}
	}
	return level // 返回生成的层数
}

// insert 向跳表中插入一个新的节点
func (sortedSet *SortedSet) insert(member string, score float64) *node {
	update := make([]*node, maxLevel) // 存储每一层更新的节点
	rank := make([]int64, maxLevel)   // 存储每一层的排名

	// 找到插入位置
	currNode := sortedSet.header
	for i := sortedSet.level - 1; i >= 0; i-- {
		if i == sortedSet.level-1 {
			rank[i] = 0 // 对于最底层，初始化 rank 为 0
		} else {
			rank[i] = rank[i+1] // 更新上一层的 rank
		}

		// 在当前层遍历，找到小于给定分数的节点
		for currNode.level[i].forward != nil &&
			(currNode.level[i].forward.Score < score || // 如果当前节点分数小于插入的分数
				(currNode.level[i].forward.Score == score && // 如果分数相同，比较成员
					currNode.level[i].forward.Member < member)) {
			rank[i] += currNode.level[i].span    // 更新当前层的排名
			currNode = currNode.level[i].forward // 移动到下一个节点
		}
		update[i] = currNode // 记录更新节点的位置
	}

	// 生成一个随机层数
	level := randomLevel()
	// 如果随机生成的层数大于当前跳表的层数，扩展跳表的层数
	if level > sortedSet.level {
		for i := sortedSet.level; i < level; i++ {
			rank[i] = 0
			update[i] = sortedSet.header               // 更新每一层的更新节点
			update[i].level[i].span = sortedSet.length // 更新每一层的跨度
		}
		sortedSet.level = level // 更新跳表的层数
	}

	// 创建新的节点
	newNode := &node{
		Element: Element{
			Member: member,
			Score:  score,
		},
		level: make([]*Level, level), // 为新节点分配 level 层
	}

	// 初始化新节点的每一层
	for i := 0; i < level; i++ {
		newNode.level[i] = &Level{}
		// 更新新节点的指针和跨度
		newNode.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = newNode

		// 更新 update[i] 层的跨度
		newNode.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 增加尚未更新跨度的层级的跨度
	for i := level; i < sortedSet.level; i++ {
		update[i].level[i].span++
	}

	// 更新新节点的 backward 指针
	if update[0] == sortedSet.header {
		newNode.backward = nil // 如果更新节点是头节点，newNode 的 backward 为 nil
	} else {
		newNode.backward = update[0] // 否则，更新 backward 为更新节点
	}

	// 如果 newNode 后面有节点，更新后继节点的 backward 指针
	if newNode.level[0].forward != nil {
		newNode.level[0].forward.backward = newNode
	} else {
		// 如果新节点是尾节点，更新尾节点
		sortedSet.tail = newNode
	}

	// 增加跳表元素数量
	sortedSet.length++
	return newNode // 返回新插入的节点
}

// deleteNode 从有序集合中删除一个节点
func (sortedSet *SortedSet) deleteNode(node *node, update []*node) {
	// 遍历每一层，删除节点并更新跨度
	for i := 0; i < sortedSet.level; i++ {
		// 如果当前层的 forward 指向 node，表示该层需要更新
		if update[i].level[i].forward == node {
			// 更新跨度
			update[i].level[i].span += node.level[i].span - 1
			// 删除节点
			update[i].level[i].forward = node.level[i].forward
		} else {
			// 如果该层没有删除节点，则减少跨度
			update[i].level[i].span--
		}
	}

	// 更新节点的 backward 指针
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		// 如果节点是尾节点，更新尾节点
		sortedSet.tail = node.backward
	}

	// 更新跳表的层数
	for sortedSet.level > 1 && sortedSet.header.level[sortedSet.level-1].forward == nil {
		sortedSet.level--
	}

	// 减少跳表中的元素数量
	sortedSet.length--
}

// Remove 从有序集合中删除一个成员
func (sortedSet *SortedSet) Remove(member string) bool {
	update := make([]*node, maxLevel) // 用来存储更新节点的指针
	node := sortedSet.header

	// 从高层到低层找到目标节点的前驱节点
	for i := sortedSet.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && node.level[i].forward.Member < member {
			node = node.level[i].forward
		}
		update[i] = node // 记录每一层的前驱节点
	}

	// 如果目标节点存在且成员匹配，删除该节点
	node = node.level[0].forward
	if node != nil && node.Member == member {
		sortedSet.deleteNode(node, update)
		return true
	}

	return false // 如果成员不存在，返回 false
}

// Exists 检查成员是否存在于有序集合中
func (sortedSet *SortedSet) Exists(member string) bool {
	node := sortedSet.getByMember(member) // 查找成员节点
	return node != nil                    // 如果节点存在，返回 true，否则返回 false
}

// Add 向有序集合中添加或更新一个成员
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	// 查找成员是否已经存在
	existed := false
	update := make([]*node, maxLevel) // 用来存储更新节点的指针
	rank := make([]int64, maxLevel)   // 用来存储每一层的排名

	node := sortedSet.header
	// 从高层到低层查找插入位置
	for i := sortedSet.level - 1; i >= 0; i-- {
		if i == sortedSet.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		// 查找插入位置
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score || // 分数小于目标分数
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) { // 分数相同，按成员字典排序
			rank[i] += node.level[i].span
			node = node.level[i].forward
		}
		update[i] = node // 记录每一层的前驱节点
	}

	// 如果节点已存在，删除并重新插入
	node = node.level[0].forward
	if node != nil && node.Member == member {
		sortedSet.deleteNode(node, update)
		existed = true
	}

	// 插入新节点
	sortedSet.insert(member, score)
	return existed // 如果节点已存在，返回 true，否则返回 false
}

// GetRank 返回成员的排名
func (sortedSet *SortedSet) GetRank(member string, reverse bool) (int64, bool) {
	var rank int64 = 0
	node := sortedSet.header

	// 从高层到低层遍历，找到成员的排名
	for i := sortedSet.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Member < member) {
			rank += node.level[i].span
			node = node.level[i].forward
		}
	}

	// 查找成员节点
	node = node.level[0].forward
	if node != nil && node.Member == member {
		if reverse {
			// 如果是逆序，返回倒序排名
			return sortedSet.length - rank - 1, true
		}
		// 返回正序排名
		return rank, true
	}

	// 如果成员不存在，返回 false
	return 0, false
}

// GetScore 返回成员的分数
func (sortedSet *SortedSet) GetScore(member string) (float64, bool) {
	node := sortedSet.getByMember(member) // 查找成员节点
	if node != nil {
		return node.Score, true // 如果节点存在，返回分数
	}
	return 0, false // 如果节点不存在，返回 false
}

// GetByRank 根据排名返回成员
func (sortedSet *SortedSet) GetByRank(rank int64, reverse bool) (*Element, bool) {
	// 如果是逆序，调整排名
	if reverse {
		rank = sortedSet.length - rank - 1
	}

	// 校验排名是否合法
	if rank < 0 || rank >= sortedSet.length {
		return nil, false // 排名无效，返回 false
	}

	var i int64 = 0
	n := sortedSet.header

	// 从头节点开始扫描，找到对应排名的成员
	for i = 0; i < rank; {
		if n.level[0].forward == nil {
			// 如果没有找到对应的节点，返回 false
			return nil, false
		}

		i += n.level[0].span
		n = n.level[0].forward
	}

	// 返回成员的 Element
	return &Element{
		Member: n.Member,
		Score:  n.Score,
	}, true
}

// GetByScoreRange 根据分数范围返回成员
func (sortedSet *SortedSet) GetByScoreRange(min, max float64, offset, limit int64, reverse bool) []*Element {
	if reverse {
		// 如果是逆序，调用逆序查询函数
		return sortedSet.getByScoreRangeReverse(min, max, offset, limit)
	}
	// 正序查询
	return sortedSet.getByScoreRange(min, max, offset, limit)
}

// getByScoreRange 返回分数在指定范围内的成员，按分数从小到大排列
func (sortedSet *SortedSet) getByScoreRange(min, max float64, offset, limit int64) []*Element {
	// 从头节点开始
	n := sortedSet.header

	// 跳过所有分数小于 min 的节点
	for i := sortedSet.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil && n.level[i].forward.Score < min {
			n = n.level[i].forward
		}
	}

	// 移动到第一个分数 >= min 的节点
	n = n.level[0].forward

	// 跳过 offset 个节点
	for n != nil && offset > 0 {
		offset--
		n = n.level[0].forward
	}

	var result []*Element
	// 获取所有分数 <= max 的节点
	for n != nil && n.Score <= max && (limit < 0 || limit > 0) {
		// 将节点加入结果列表
		result = append(result, &Element{
			Member: n.Member,
			Score:  n.Score,
		})

		// 如果限制返回数量，减少 limit
		if limit > 0 {
			limit--
		}

		// 移动到下一个节点
		n = n.level[0].forward
	}

	// 返回符合条件的成员列表
	return result
}

// getByScoreRangeReverse 返回分数在指定范围内的成员，按分数从大到小排列
func (sortedSet *SortedSet) getByScoreRangeReverse(min, max float64, offset, limit int64) []*Element {
	var result []*Element

	// 从尾节点开始
	n := sortedSet.tail

	// 跳过所有分数大于 max 的节点
	for n != nil && n.Score > max {
		n = n.backward
	}

	// 跳过 offset 个节点
	for n != nil && offset > 0 {
		offset--
		n = n.backward
	}

	// 获取所有分数 >= min 的节点
	for n != nil && n.Score >= min && (limit < 0 || limit > 0) {
		// 将节点加入结果列表
		result = append(result, &Element{
			Member: n.Member,
			Score:  n.Score,
		})

		// 如果限制返回数量，减少 limit
		if limit > 0 {
			limit--
		}

		// 移动到前一个节点
		n = n.backward
	}

	// 返回符合条件的成员列表
	return result
}

// GetByLexRange 根据成员名的字典顺序返回指定范围内的成员
func (sortedSet *SortedSet) GetByLexRange(min, max string, offset, limit int64, reverse bool) []*Element {
	// 如果是逆序查询，调用逆序版本
	if reverse {
		return sortedSet.getByLexRangeReverse(min, max, offset, limit)
	}
	// 正序查询
	return sortedSet.getByLexRange(min, max, offset, limit)
}

// getByLexRange 返回成员名在指定字典范围内的成员，按字典顺序从小到大排列
func (sortedSet *SortedSet) getByLexRange(min, max string, offset, limit int64) []*Element {
	// 从头节点开始
	n := sortedSet.header

	// 跳过所有成员名小于 min 的节点
	for i := sortedSet.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil && n.level[i].forward.Member < min {
			n = n.level[i].forward
		}
	}

	// 移动到第一个成员名 >= min 的节点
	n = n.level[0].forward

	// 跳过 offset 个节点
	for n != nil && offset > 0 {
		offset--
		n = n.level[0].forward
	}

	var result []*Element
	// 获取所有成员名 <= max 的节点
	for n != nil && n.Member <= max && (limit < 0 || limit > 0) {
		// 将节点加入结果列表
		result = append(result, &Element{
			Member: n.Member,
			Score:  n.Score,
		})

		// 如果限制返回数量，减少 limit
		if limit > 0 {
			limit--
		}

		// 移动到下一个节点
		n = n.level[0].forward
	}

	// 返回符合条件的成员列表
	return result
}

// getByLexRangeReverse 返回成员名在指定字典范围内的成员，按字典顺序从大到小排列
func (sortedSet *SortedSet) getByLexRangeReverse(min, max string, offset, limit int64) []*Element {
	var result []*Element

	// 从尾节点开始
	n := sortedSet.tail

	// 跳过所有成员名大于 max 的节点
	for n != nil && n.Member > max {
		n = n.backward
	}

	// 跳过 offset 个节点
	for n != nil && offset > 0 {
		offset--
		n = n.backward
	}

	// 获取所有成员名 >= min 的节点
	for n != nil && n.Member >= min && (limit < 0 || limit > 0) {
		// 将节点加入结果列表
		result = append(result, &Element{
			Member: n.Member,
			Score:  n.Score,
		})

		// 如果限制返回数量，减少 limit
		if limit > 0 {
			limit--
		}

		// 移动到前一个节点
		n = n.backward
	}

	// 返回符合条件的成员列表
	return result
}

// Count 返回分数在指定范围内的元素个数
func (sortedSet *SortedSet) Count(min, max float64) int64 {
	// 调用 GetByScoreRange 获取分数在范围内的所有元素，计算并返回它们的个数
	return int64(len(sortedSet.GetByScoreRange(min, max, 0, -1, false)))
}

// RangeCount 返回成员名在指定字典范围内的元素个数
func (sortedSet *SortedSet) RangeCount(min, max string) int64 {
	// 调用 GetByLexRange 获取成员名在范围内的所有元素，计算并返回它们的个数
	return int64(len(sortedSet.GetByLexRange(min, max, 0, -1, false)))
}

// Len 返回有序集合中元素的总数
func (sortedSet *SortedSet) Len() int64 {
	// 返回集合的长度
	return sortedSet.length
}

// getByMember 根据成员名查找对应的节点
func (sortedSet *SortedSet) getByMember(member string) *node {
	n := sortedSet.header

	// 遍历跳过成员名小于给定值的节点
	for i := sortedSet.level - 1; i >= 0; i-- {
		for n.level[i].forward != nil && n.level[i].forward.Member < member {
			n = n.level[i].forward
		}
	}

	// 查找节点并返回
	n = n.level[0].forward
	if n != nil && n.Member == member {
		return n
	}

	// 如果找不到该节点，返回 nil
	return nil
}

// ForEach 遍历有序集合并对每个元素执行给定的函数
func (sortedSet *SortedSet) ForEach(fn func(element *Element) bool) {
	n := sortedSet.header.level[0].forward

	// 遍历每个元素，直到遍历完或者函数返回 false
	for n != nil {
		if !fn(&Element{
			Member: n.Member,
			Score:  n.Score,
		}) {
			break
		}
		n = n.level[0].forward
	}
}

// Range 遍历有序集合中指定范围的元素（支持正序或逆序）
func (sortedSet *SortedSet) Range(start, stop int64, reverse bool, fn func(element *Element) bool) {
	// 如果是逆序遍历
	if reverse {
		// 处理负数索引
		if start < 0 {
			start = sortedSet.length + start
			if start < 0 {
				start = 0
			}
		}
		if stop < 0 {
			stop = sortedSet.length + stop
			if stop < 0 {
				stop = 0
			}
		}

		// 如果 start 大于 stop，则交换 start 和 stop
		if start > stop {
			start, stop = stop, start
		}

		// 从尾节点开始
		n := sortedSet.tail
		i := sortedSet.length - 1

		// 跳过元素直到 start 索引
		for n != nil && i > start {
			n = n.backward
			i--
		}

		// 遍历范围内的元素
		for n != nil && i >= start && i <= stop {
			if !fn(&Element{
				Member: n.Member,
				Score:  n.Score,
			}) {
				break
			}
			n = n.backward
			i--
		}
	} else {
		// 处理负数索引
		if start < 0 {
			start = sortedSet.length + start
			if start < 0 {
				start = 0
			}
		}
		if stop < 0 {
			stop = sortedSet.length + stop
			if stop < 0 {
				stop = 0
			}
		}

		// 如果 start 大于 stop，则交换 start 和 stop
		if start > stop {
			start, stop = stop, start
		}

		// 从头节点开始
		n := sortedSet.header.level[0].forward
		i := int64(0)

		// 跳过元素直到 start 索引
		for n != nil && i < start {
			n = n.level[0].forward
			i++
		}

		// 遍历范围内的元素
		for n != nil && i >= start && i <= stop {
			if !fn(&Element{
				Member: n.Member,
				Score:  n.Score,
			}) {
				break
			}
			n = n.level[0].forward
			i++
		}
	}
}
