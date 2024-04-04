package searchbolt

import (
	"encoding/hex"
	"encoding/json"
	"github.com/fr0stylo/searchbolt/database"
	"reflect"

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
			k := database.UintKey(id)
			key = k[:]
			err := idsBucket.Put(idBytes, key)
			if err != nil {
				return err
			}
		}
		j, err := json.Marshal(data)
		if err != nil {
			return err
		}
		return dataBucket.Put(key[:], j)
	})

	return hex.EncodeToString(key[:]), err
}

func insert(idsBucket *bolt.Bucket, dataBucket *bolt.Bucket, originalid []byte, data []byte) (resId []byte, err error) {
	var key []byte

	key = idsBucket.Get(originalid)
	if key == nil {
		id, _ := dataBucket.NextSequence()
		k := database.UintKey(id)
		key = k[:]
		err := idsBucket.Put(originalid, key)
		if err != nil {
			return nil, err
		}
	}

	err = dataBucket.Put(key[:], data)
	if err != nil {
		return nil, err
	}

	return key[:], err
}

func removeNulls(m map[string]interface{}) {
	val := reflect.ValueOf(m)
	for _, e := range val.MapKeys() {
		v := val.MapIndex(e)
		if v.IsNil() {
			delete(m, e.String())
			continue
		}
		switch t := v.Interface().(type) {
		// If key is a JSON object (Go Map), use recursion to go deeper
		case map[string]interface{}:
			removeNulls(t)
		}
	}
}

func prepareData(id any, data any) ([]byte, []byte, error) {
	removeNulls(data.(map[string]interface{}))
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

func UpsertBatch(db *bolt.DB, indexer chan *IndexRequest, bucketName string, items []BatchEntry) (resIds []string, err error) {
	insertedIds := [][]byte{}
	err = db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		_, _ = bucket.CreateBucketIfNotExists([]byte("facets"))
		dataBucket, err := bucket.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		idsBucket, err := bucket.CreateBucketIfNotExists([]byte("ids"))
		if err != nil {
			return err
		}

		//idx := map[string][]string{}
		for _, v := range items {
			idBytes, dataBytes, err := prepareData(v.Id, v.Data)
			if err != nil {
				return err
			}
			id, err := insert(idsBucket, dataBucket, idBytes, dataBytes)
			if err != nil {
				return err
			}
			//if err := AddTempFTSIndex(&idx, id, dataBytes, ftsIdx...); err != nil {
			//	return err
			//}

			insertedIds = append(insertedIds, id)
		}
		return nil
		//return PersistTempFTSIndex(bucket, &idx)
	})

	for _, id := range insertedIds {
		indexer <- &IndexRequest{Bucket: bucketName, ObjectId: id}
		resIds = append(resIds, hex.EncodeToString(id))
	}

	return
}

func UpsertObjectBatch(db *database.Database, indexer chan *IndexRequest, bucketName string, items []BatchEntry) (resIds []string, err error) {
	var entries []database.BatchEntry
	for _, v := range items {
		idBytes, dataBytes, err := prepareData(v.Id, v.Data)
		if err != nil {
			return nil, err
		}
		entries = append(entries, database.BatchEntry{
			Id:   idBytes,
			Data: dataBytes,
		})
	}
	insertedIds := db.UpsertObjects(entries)

	for _, id := range insertedIds {
		indexer <- &IndexRequest{Bucket: bucketName, ObjectId: id}
		resIds = append(resIds, hex.EncodeToString(id))
	}
	return
}
