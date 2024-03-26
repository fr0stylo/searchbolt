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

func insert(idsBucket *bolt.Bucket, dataBucket *bolt.Bucket, oridinalid []byte, data []byte) (resId []byte, err error) {
	var key []byte

	key = idsBucket.Get(oridinalid)
	if key == nil {
		id, _ := dataBucket.NextSequence()
		k := UintKey(id)
		key = k[:]
		idsBucket.Put(oridinalid, key)
	}

	dataBucket.Put(key[:], data)

	return key[:], err
}

func prepareData(id any, data any) ([]byte, []byte, error) {
	idBytes, err := json.Marshal(id)
	if err != nil {
		return nil, nil, err
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, nil, err
	}

	return idBytes, dataBytes, nil
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

		idBytes, dataBytes, err := prepareData(id, data)
		if err != nil {
			return err
		}
		res, err := insert(idsBucket, dataBucket, idBytes, dataBytes)

		resId = hex.EncodeToString(res)
		return err
	})

	return
}

type BatchEntry struct {
	Id   any            `json:"id"`
	Data map[string]any `json:"data"`
}

func UpsertBatch(db *bolt.DB, bucketName string, items []BatchEntry) (resIds []string, err error) {
	err = db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		fts, _, err := GetMappings(db, bucketName)
		if err != nil {
			return err
		}

		ftsIdx := []CreateFTS{}
		for _, v := range fts {
			ftsIdx = append(ftsIdx, CreateFTSOpts(v, Ptr(true)))
		}

		bucket.CreateBucketIfNotExists([]byte("facets"))
		dataBucket, err := bucket.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		idsBucket, err := bucket.CreateBucketIfNotExists([]byte("ids"))
		if err != nil {
			return err
		}

		idx := map[string][]string{}
		for _, v := range items {
			idBytes, dataBytes, err := prepareData(v.Id, v.Data)
			if err != nil {
				return err
			}
			id, err := insert(idsBucket, dataBucket, idBytes, dataBytes)
			if err != nil {
				return err
			}
			if err := AddTempFTSIndex(&idx, id, dataBytes, ftsIdx...); err != nil {
				return err
			}

			resIds = append(resIds, hex.EncodeToString(id))
		}

		return PersistTempFTSIndex(bucket, &idx)
	})

	return
}
