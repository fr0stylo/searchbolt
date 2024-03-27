package searchbolt

import (
	"encoding/json"
	"log"
	"net/http"

	bolt "go.etcd.io/bbolt"
)

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
		if err != nil {
			return err
		}
		data := getData()
		for _, v := range data {
			j, _ := json.Marshal(v)
			id := IntKey(v.Id)
			dataBucket.Put(id[:], []byte(j))
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
