package searchbolt

import (
	"crypto/md5"
	"encoding/binary"
	"hash/maphash"
)

var seed = maphash.MakeSeed()

func IntKey(v int) [8]byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return [8]byte(b)
}

func UintKey(v uint64) [8]byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return [8]byte(b)
}

func StrKey(v string) [8]byte {
	var h maphash.Hash
	h.SetSeed(seed)
	h.WriteString(v)
	
	return [8]byte(md5.New().Sum([]byte(v))[:8]) //UintKey(h.Sum64())
}
