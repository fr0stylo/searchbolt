package searchbolt

import (
	"encoding/json"
	"regexp"
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
		val := v[name].(string)
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

func CreateFTSIndex(db *bolt.DB, bucket string, fields ...CreateFTS) error {
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
