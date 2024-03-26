package searchbolt

import (
	"bytes"
	"encoding/json"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/arriqaaq/art"

	bolt "go.etcd.io/bbolt"
)

func cleanInput(input string) string {
	str := strings.Trim(input, " ")
	str = regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(str, " ")
	return str
}

type CreateFTS = func(v map[string]any) (string, []string)

func CreateFTSOpts(name string, unify *bool) CreateFTS {
	return func(v map[string]any) (string, []string) {
		val, ok := v[name].(string)
		if !ok {
			return name, []string{}
		}
		val = cleanInput(val)
		if unify != nil && *unify {
			val = strings.ToLower(val)
		}

		vals := strings.Split(val, " ")
		res := []string{}
		for _, split := range vals {
			str := split
			if len(str) >= 3 {
				res = append(res, str)
			}
		}

		return name, res
	}
}

func AddTempFTSIndex(ar *map[string][]string, k []byte, v []byte, fields ...CreateFTS) error {
	dar := *ar
	var q map[string]any
	if err := json.Unmarshal(v, &q); err != nil {
		return err
	}
	for _, field := range fields {
		_, items := field(q)
		for _, b := range items {
			if dar[b] == nil {
				dar[b] = []string{}
			}
			data := dar[b]

			if !slices.Contains(data, string(k)) {
				data = append(data, string(k))
			}
			dar[b] = data
		}
	}

	return nil
}

func PersistTempFTSIndex(tx *bolt.Bucket, ar *map[string][]string) error {
	ftsBucket, err := tx.CreateBucketIfNotExists([]byte("fts"))
	if err != nil {
		return err
	}

	for k, v := range *ar {
		byteK := []byte(k)
		data := ftsBucket.Get(byteK)
		if data != nil {
			keys := byteSlide(data, 8)
			for _, vals := range v {
				if !ContainsKey(keys, []byte(vals)) {
					data = append(data, []byte(vals)...)
				}
			}
		} else {
			data = []byte{}
			for _, vals := range v {
				data = append(data, []byte(vals)...)
			}
		}
		sorted := byteSlide(data, 8)
		sort.Slice(sorted, func(i, j int) bool {
			return bytes.Compare(sorted[i], sorted[j]) < 0
		})
		data = []byte{}
		for _, vals := range sorted {
			data = append(data, []byte(vals)...)
		}

		if err := ftsBucket.Put(byteK, data); err != nil {
			return err
		}
	}

	return nil
}

func RecreateFTSIndex(db *bolt.DB, bucket string, fields ...CreateFTS) error {
	return db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		dataBucket := bucket.Bucket([]byte("data"))
		bucket.DeleteBucket([]byte("fts"))
		ftsB, _ := bucket.CreateBucketIfNotExists([]byte("fts"))
		for _, field := range fields {
			if err := dataBucket.ForEach(func(k, v []byte) error {
				var q map[string]any
				if err := json.Unmarshal(v, &q); err != nil {
					return err
				}

				_, items := field(q)
				for _, b := range items {
					val := ftsB.Get([]byte(b))
					if val == nil {
						if err := ftsB.Put([]byte(b), k); err != nil {
							return err
						}
					} else {
						val = append(val, k...)

						if err := ftsB.Put([]byte(b), val); err != nil {
							return err
						}
					}
				}

				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})
}

func LoadFTSIndex(db *bolt.Bucket) *art.Tree {
	idx := art.NewTree()

	db.ForEach(func(k, v []byte) error {
		idx.Insert(k, v)

		return nil
	})

	return idx
}
