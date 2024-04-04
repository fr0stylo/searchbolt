package database

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"sync"
)

type Database struct {
	db         *bolt.DB
	bucketName []byte
}

func NewDatabase(db *bolt.DB, bucketName string) *Database {
	return &Database{db: db, bucketName: []byte(bucketName)}
}

var (
	FacetBucket = []byte("facets")
	DataBucket  = []byte("data")
	IdsBucket   = []byte("ids")
)

func (d *Database) CreateBuckets() error {
	return d.db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(d.bucketName)
		if err != nil {
			return err
		}
		_, err = bucket.CreateBucketIfNotExists(FacetBucket)
		if err != nil {
			return err
		}
		_, err = bucket.CreateBucketIfNotExists(DataBucket)
		if err != nil {
			return err
		}
		_, err = bucket.CreateBucketIfNotExists(IdsBucket)
		return err
	})
}

type BatchEntry struct {
	Id   []byte
	Data []byte
}

func (d *Database) UpsertObjects(entries []BatchEntry) [][]byte {
	var wg sync.WaitGroup
	res := make(chan []byte, len(entries))
	for _, entry := range entries {
		wg.Add(1)
		entry := entry
		go func() {
			id, err := d.UpsertObject(entry.Data, entry.Id)
			if err != nil {
				log.Print(err)
			}
			res <- id
			wg.Done()
		}()
	}
	wg.Wait()
	close(res)
	insertedIds := make([][]byte, 0)
	for ids := range res {
		insertedIds = append(insertedIds, ids)
	}

	return insertedIds
}

func (d *Database) UpsertObject(data, originalid []byte) (key []byte, err error) {
	err = d.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.bucketName)
		if b == nil {
			return fmt.Errorf("%s bucket not exist", d.bucketName)
		}
		idsBucket := b.Bucket(IdsBucket)
		if idsBucket == nil {
			return fmt.Errorf("%s bucket not exist", IdsBucket)
		}
		dataBucket := b.Bucket(DataBucket)
		if idsBucket == nil {
			return fmt.Errorf("%s bucket not exist", DataBucket)
		}
		key = idsBucket.Get(originalid)
		if key == nil {
			id, _ := dataBucket.NextSequence()
			k := UintKey(id)
			key = k[:]
			err := idsBucket.Put(originalid, key)
			if err != nil {
				return err
			}
		}
		return dataBucket.Put(key[:], data)
	})
	return
}
