package searchbolt

import "bytes"

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

func ContainsKey(keys [][]byte, byteK []byte) bool {
	for _, key := range keys {
		if bytes.Equal(key, byteK) {
			return true
		}
	}
	return false
}

func byteSlide(ar []byte, size int) [][]byte {
	keys := [][]byte{}
	for i := 0; i < len(ar); i = i + size {
		// for chunkSize < len(keyBytes) {
		key := ar[i : i+size]
		keys = append(keys, key)
	}

	return keys
}

func Ptr[T any](i T) *T {
	return &i
}
