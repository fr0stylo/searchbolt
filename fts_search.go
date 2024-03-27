package searchbolt

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/arriqaaq/art"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

func prepareQuery(query string) []string {
	kw := cleanInput(query)
	kws := strings.Split(kw, " ")
	result := []string{}
	for _, v := range kws {
		if len(v) >= 3 {
			result = append(result, strings.ToLower(v))
		}
	}

	return result
}

var idx *art.Tree
var once sync.Once

func getIndexItems(bucket *bolt.Bucket, value string, keys *map[[8]byte]byte) error {
	(*keys) = map[[8]byte]byte{}

	var keyBytes []byte
	if idx != nil {
		if s, ok := idx.Search([]byte(value)).([]byte); !ok {
			keyBytes = []byte{}
			idx.Scan([]byte(value), func(n *art.Node) {
				if n.IsLeaf() {
					if s, ok := idx.Search(n.Key()).([]byte); ok {
						keyBytes = append(keyBytes, s...)
					}
				}
			})
		} else {
			keyBytes = s
		}
	} else {
		keyBytes = bucket.Get([]byte(value))
	}

	for i := 0; i < len(keyBytes); i = i + chunkSize {
		key := keyBytes[i : i+chunkSize]
		(*keys)[[8]byte(key)] = 1
	}

	return nil
}

func scanFTSIndex(bucket *bolt.Bucket, keys []string) (map[[8]byte]byte, error) {
	once.Do(func() {
		idx = LoadFTSIndex(bucket)
	})
	if len(keys) == 0 {
		return map[[8]byte]byte{}, nil
	}
	wg, _ := errgroup.WithContext(context.Background())
	k := make([]map[[8]byte]byte, len(keys))
	for i, val := range keys {
		wg.Go(func() error {
			return getIndexItems(bucket, val, &k[i])
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	if len(keys) > 1 {
		mapUnion(k[0], k[1:]...)
	}

	return k[0], nil
}

func retrieveByKeys(bucket *bolt.Bucket, keys map[[8]byte]byte) (chan []byte, error) {
	resChan := make(chan []byte, len(keys))
	var wg sync.WaitGroup
	for key := range keys {
		wg.Add(1)
		go readOne(resChan, &wg, key[:], bucket)
	}

	wg.Wait()
	close(resChan)

	return resChan, nil
}

func retrieveArray(bucket *bolt.Bucket, keys [][8]byte) (chan []byte, error) {
	resChan := make(chan []byte, len(keys))
	for _, key := range keys {
		log.Print(key)
		resChan <- bucket.Get(key[:])
	}

	close(resChan)

	return resChan, nil
}

func Search(db *bolt.DB, bucket string, query string) (io.Reader, error) {
	buf := make([]byte, 0, 1024)
	rawresult := bytes.NewBuffer(buf)
	err := db.View(func(tx *bolt.Tx) error {
		rawresult.WriteRune('[')
		b := tx.Bucket([]byte(bucket))
		fts := b.Bucket([]byte("fts"))
		d := b.Bucket([]byte("data"))
		preparedQuery := prepareQuery(query)
		loc, err := scanFTSIndex(fts, preparedQuery)
		if err != nil {
			return err
		}

		res, _ := retrieveByKeys(d, loc)
		for r := range res {
			rawresult.Write(r)
			if len(res) != 0 {
				rawresult.WriteRune(',')
			}
		}

		rawresult.WriteRune(']')

		return nil
	})

	return rawresult, err
}
