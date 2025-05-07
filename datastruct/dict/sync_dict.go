package dict

import "sync"

// SyncDict 使用 sync.Map 来实现线程安全的字典
type SyncDict struct {
	m sync.Map // 内部使用 sync.Map 来存储键值对
}

// MakeSyncDict 创建一个新的 SyncDict 实例
func MakeSyncDict() *SyncDict {
	return &SyncDict{} // 返回一个新的空字典实例
}

// Get 根据键获取对应的值，返回值和是否存在的标志
func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m.Load(key) // 使用 Load 方法获取值
	return val, ok              // 返回值和是否存在的标志
}

// Len 获取字典中键值对的数量
func (dict *SyncDict) Len() int {
	lenth := 0
	// 使用 Range 遍历字典中的所有键值对
	dict.m.Range(func(k, v interface{}) bool {
		lenth++     // 每遍历到一个键值对，计数器加 1
		return true // 继续遍历
	})
	return lenth // 返回字典的大小
}

// Put 将键值对插入字典，如果键已存在则不做修改
func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key) // 检查键是否已经存在
	dict.m.Store(key, val)         // 存储键值对
	if existed {                   // 如果键已存在，返回 0
		return 0
	}
	return 1 // 如果键是新的，返回 1
}

// PutIfAbsent 只有当键不存在时才插入键值对
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key) // 检查键是否已经存在
	if existed {                   // 如果键已存在，返回 0
		return 0
	}
	dict.m.Store(key, val) // 存储键值对
	return 1               // 返回 1，表示插入成功
}

// PutIfExists 只有当键已存在时才更新值
func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key) // 检查键是否已存在
	if existed {                   // 如果键已存在，更新值
		dict.m.Store(key, val)
		return 1 // 返回 1，表示更新成功
	}
	return 0 // 如果键不存在，返回 0
}

// Remove 从字典中移除指定的键
func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key) // 检查键是否存在
	dict.m.Delete(key)             // 删除键值对
	if existed {                   // 如果键存在，返回 1
		return 1
	}
	return 0 // 如果键不存在，返回 0
}

// Keys 获取字典中所有键的列表
func (dict *SyncDict) Keys() []string {
	result := make([]string, dict.Len()) // 创建一个与字典大小相同的字符串切片
	i := 0
	// 遍历字典中的所有键值对
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string) // 将键存入结果切片
		i++
		return true // 继续遍历
	})
	return result // 返回所有键的切片
}

// ForEach 遍历字典中的所有键值对，执行传入的函数
func (dict *SyncDict) ForEach(consumer Consumer) {
	// 遍历字典中的每个键值对
	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value) // 执行回调函数
		return true                   // 继续遍历
	})
}

// RandomKeys 获取字典中随机选择的键，限制数量
func (dict *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, limit) // 创建一个大小为 limit 的切片
	for i := 0; i < limit; i++ {
		// 遍历字典，随机选择一个键
		dict.m.Range(func(key, value interface{}) bool {
			result[i] = key.(string) // 将键存入结果切片
			return false             // 返回 false 停止遍历
		})
	}
	return result // 返回随机选择的键切片
}

// RandomDistinctKeys 获取字典中随机选择的不同键，限制数量
func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, limit) // 创建一个大小为 limit 的切片
	i := 0
	// 遍历字典，随机选择不同的键
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string) // 将键存入结果切片
		i++
		if i == limit { // 如果已选到指定数量，停止遍历
			return false
		}
		return true
	})
	return result // 返回随机选择的不同键切片
}

// Clear 清空字典
func (dict *SyncDict) Clear() {
	*dict = *MakeSyncDict() // 通过重新创建一个新的空字典来清空原字典
}
