package utils

// NormalizeRange 将 Redis 风格的 start, stop 转换为 Go 切片的安全索引 [start, end)
// listLen: 切片的长度
// 返回值: safeStart, safeEnd (可以直接用于 slice[safeStart:safeEnd])
func NormalizeRange(start, stop int, len int) (int, int) {
	// 1.处理负数索引
	if start < 0 {
		start = len + start
	}
	if stop < 0 {
		stop = len + stop
	}

	// 2. 处理负数越界（例如长度5，索引-100 -> -95，需要修正为0）
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = 0
	}

	// 3. 处理 stop 越界（超过长度）
	if stop > len {
		stop = len - 1
	}

	// 4. 处理无效区间
	if start > stop {
		return 0, 0
	}

	// 5. 转换为 Go 切片友好的 end (左闭右开，所以要 +1)
	return start, stop + 1
}
