package searchbolt

import (
	"encoding/hex"
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

func Insert(db *bolt.DB, bucket string, id any, data any) (resId string, err error) {
	var key []byte
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		dataBucket, err := bucket.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		idsBucket, err := bucket.CreateBucketIfNotExists([]byte("ids"))
		if err != nil {
			return err
		}

		idBytes, err := json.Marshal(id)
		if err != nil {
			return err
		}
		key = idsBucket.Get(idBytes)
		if key == nil {
			id, _ := dataBucket.NextSequence()
			k := UintKey(id)
			key = k[:]
			idsBucket.Put(idBytes, key)
		}
		j, err := json.Marshal(data)
		if err != nil {
			return err
		}
		dataBucket.Put(key[:], []byte(j))
		return nil
	})

	return hex.EncodeToString(key[:]), err
}

func insert(idsBucket *bolt.Bucket, dataBucket *bolt.Bucket, id any, data any) (resId string, err error) {
	var key []byte

	idBytes, err := json.Marshal(id)
	if err != nil {
		return "", err
	}
	key = idsBucket.Get(idBytes)
	if key == nil {
		id, _ := dataBucket.NextSequence()
		k := UintKey(id)
		key = k[:]
		idsBucket.Put(idBytes, key)
	}
	j, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	dataBucket.Put(key[:], []byte(j))

	return hex.EncodeToString(key[:]), err
}

func UpsertOne(db *bolt.DB, bucket string, id any, data any) (resId string, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		dataBucket, err := bucket.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		idsBucket, err := bucket.CreateBucketIfNotExists([]byte("ids"))
		if err != nil {
			return err
		}

		resId, err = insert(idsBucket, dataBucket, id, data)
		return err
	})

	return
}

type BatchEntry struct {
	Id   any            `json:"id"`
	Data map[string]any `json:"data"`
}

func UpsertBatch(db *bolt.DB, bucket string, items []BatchEntry) (resIds []string, err error) {
	err = db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		bucket.CreateBucketIfNotExists([]byte("facets"))
		bucket.CreateBucketIfNotExists([]byte("fts"))
		dataBucket, err := bucket.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		idsBucket, err := bucket.CreateBucketIfNotExists([]byte("ids"))
		if err != nil {
			return err
		}

		for _, v := range items {
			id, err := insert(idsBucket, dataBucket, v.Id, v.Data)
			if err != nil {
				return err
			}

			resIds = append(resIds, id)
		}
		return err
	})

	return
}
