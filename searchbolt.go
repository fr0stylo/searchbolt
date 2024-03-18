package searchbolt

import (
	"encoding/json"
	"log"
	"net/http"

	bolt "go.etcd.io/bbolt"
)

type Quote struct {
	Id     int
	Quote  string
	Author string
}

type Wrapper struct {
	Quotes []Quote
	Total  int
	Skip   int
	Limit  int
}

func getData() []Quote {
	resp, err := http.Get("https://dummyjson.com/quotes?limit=2000")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	var res Wrapper

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		log.Fatal(err)
	}

	return res.Quotes
}

func SeedData(db *bolt.DB, bucket string) {
	if err := db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		dataBucket, err := bucket.CreateBucket([]byte("data"))
		if err != nil && err != bolt.ErrBucketExists {
			return err
		} else if err != bolt.ErrBucketExists {
			data := getData()
			for _, v := range data {
				j, _ := json.Marshal(v)
				id := IntKey(v.Id)
				dataBucket.Put(id[:], []byte(j))
			}
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}
}

func LoadData(db *bolt.DB, bucket string, idField func(item map[string]any) [8]byte, items []map[string]any) error {
	return db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		dataBucket, err := bucket.CreateBucket([]byte("data"))
		if err != nil && err != bolt.ErrBucketExists {
			return err
		} else if err != bolt.ErrBucketExists {
			for _, v := range items {
				j, _ := json.Marshal(v)
				id := idField(v)
				dataBucket.Put(id[:], []byte(j))
			}
		}

		return nil
	})
}

func GetById[T any](db *bolt.DB, bucket string, id [8]byte, obj *T) error {
	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		dataBucket := bucket.Bucket([]byte("data"))

		if err := json.Unmarshal(dataBucket.Get(id[:]), obj); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
