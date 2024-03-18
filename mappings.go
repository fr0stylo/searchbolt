package searchbolt

import (
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

func CreateMappings(db *bolt.DB, bucket string, filters map[string]string, search []string) error {
	return db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		mb, err := b.CreateBucketIfNotExists([]byte("mappings"))
		if err != nil {
			return err
		}

		jb, err := json.Marshal(filters)
		if err != nil {
			return err
		}
		mb.Put([]byte("facets"), jb)
		if err != nil {
			return err
		}

		jsb, err := json.Marshal(filters)
		if err != nil {
			return err
		}
		mb.Put([]byte("fts"), jsb)
		if err != nil {
			return err
		}

		return nil
	})
}
