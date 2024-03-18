package searchbolt

func mapUnion[T string | [8]byte](m1 map[T]byte, other ...map[T]byte) {
	for k := range m1 {
		for _, otherMap := range other {
			if otherMap[k] == 0 {
				delete(m1, k)
				break
			}
		}
	}
}

func Ptr[T any](i T) *T {
	return &i
}
