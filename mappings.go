package searchbolt

import (
	"encoding/json"
	"log"

	bolt "go.etcd.io/bbolt"
)

func CreateMappings(db *bolt.DB, bucket string, filters map[string]string, search []string) error {
	log.Print("cm1")
	return db.Batch(func(tx *bolt.Tx) error {
		log.Print("cm2")
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		mb, err := b.CreateBucketIfNotExists([]byte("mappings"))
		if err != nil {
			return err
		}

		log.Print("cm3")
		jb, err := json.Marshal(filters)
		if err != nil {
			return err
		}

		if err := mb.Put([]byte("facets"), jb); err != nil {
			return err
		}
		log.Print("cm4")

		jsb, err := json.Marshal(search)
		if err != nil {
			return err
		}
		log.Print("cm5")

		return mb.Put([]byte("fts"), jsb)
	})
}

func GetMappings(db *bolt.DB, bucket string) ([]string, map[string]string, error) {
	fts := []string{}
	facets := map[string]string{}
	err := db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(bucket))
		if c == nil {
			return nil
		}
		b := c.Bucket([]byte("mappings"))
		if b == nil {
			return nil
		}

		data := b.Get([]byte("fts"))
		if err := json.Unmarshal(data, &fts); err != nil {
			return err
		}

		data = b.Get([]byte("facets"))
		return json.Unmarshal(data, &facets)
	})

	return fts, facets, err
}
