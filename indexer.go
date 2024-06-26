package searchbolt

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"time"
)

type IndexRequest struct {
	Bucket   string
	ObjectId []byte
}

type TempFTSIndex = map[string][]string

func StartIndexer(db *bolt.DB) chan *IndexRequest {
	requests := make(chan *IndexRequest)

	go func() {
		var t *time.Timer
		ftsIdx := map[string]*map[string][]string{}
		for {
			select {
			case request := <-requests:
				idx := ftsIdx[request.Bucket]
				if idx == nil {
					ftsIdx[request.Bucket] = &map[string][]string{}
					idx = ftsIdx[request.Bucket]
				}

				err := appendTempIndex(db, request, idx)
				if err != nil {
					log.Print(err)
				}
				if t == nil {
					t = time.AfterFunc(1*time.Second, func() {
						index(db, ftsIdx)
						t = nil
					})
				}
			}
		}
	}()

	return requests
}

func index(db *bolt.DB, ftsIdx map[string]*map[string][]string) {
	log.Print("Putting index")
	err := db.Batch(func(tx *bolt.Tx) error {
		for bucket, index := range ftsIdx {
			ftsIdx[bucket] = &map[string][]string{}
			if err := PersistTempFTSIndex(tx.Bucket([]byte(bucket)), index); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Print(err)
	}
}

func appendTempIndex(db *bolt.DB, request *IndexRequest, ftsIdx *map[string][]string) error {
	fts, _, err := GetMappings(db, request.Bucket)
	if err != nil {
		return err
	}

	ftsMappings := []CreateFTS{}
	for _, v := range fts {
		ftsMappings = append(ftsMappings, CreateFTSOpts(v, Ptr(true)))
	}

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(request.Bucket))
		dataBucket := b.Bucket([]byte("data"))
		if dataBucket == nil {
			return fmt.Errorf("data bucket not found")
		}
		d := dataBucket.Get(request.ObjectId)
		return AddTempFTSIndex(ftsIdx, request.ObjectId, d, ftsMappings...)
	})
}
