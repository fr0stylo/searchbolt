package searchbolt

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"strings"
	"sync"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

func GetFacets(db *bolt.DB, bucket string) (map[string][]string, error) {
	result := make(map[string][]string)
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket)).Bucket([]byte("facets"))

		g, _ := errgroup.WithContext(context.Background())
		g.Go(func() error {
			return b.ForEachBucket(func(k []byte) error {
				log.Print(string(k))
				return b.Bucket(k).ForEach(func(k1, v []byte) error {
					log.Print(string(k1), " ", len(v)/8)

					return nil
				})
			})
		})

		return g.Wait()
	})

	return result, err
}

var chunkSize = 8

func Filter[T any](db *bolt.DB, bucket, facet, value string) ([]T, error) {
	result := make([]T, 0)
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		f := b.Bucket([]byte("facets"))
		d := b.Bucket([]byte("data"))

		v, _ := json.Marshal(value)
		keyBytes := f.Bucket([]byte(facet)).Get(v)
		var r T
		var key []byte
		for chunkSize < len(keyBytes) {
			keyBytes, key = keyBytes[chunkSize:], keyBytes[0:chunkSize:chunkSize]
			if err := json.Unmarshal(d.Get(key), &r); err != nil {
				return err
			}
			result = append(result, r)
		}

		return nil
	})

	return result, err
}

func readOne(ch chan []byte, wg *sync.WaitGroup, key []byte, tx *bolt.Bucket) {
	if wg != nil {
		defer wg.Done()
	}
	ch <- tx.Get(key)
}

func FilterFn(db *bolt.DB, bucket string, facet ...FacetFilter) (io.Reader, error) {
	buf := make([]byte, 0, 1024)
	rawresult := bytes.NewBuffer(buf)
	err := db.View(func(tx *bolt.Tx) error {
		rawresult.WriteRune('[')
		b := tx.Bucket([]byte(bucket))
		fb := b.Bucket([]byte("facets"))
		d := b.Bucket([]byte("data"))

		var wgg sync.WaitGroup
		keys := make([]map[[8]byte]byte, len(facet))
		for i, f := range facet {
			wgg.Add(1)
			go f(fb, &wgg, &keys[i])
		}

		wgg.Wait()
		if len(keys) > 1 {
			mapUnion(keys[0], keys[1:]...)
		}

		res := make(chan []byte, len(keys[0]))
		var wg sync.WaitGroup
		for key := range keys[0] {
			wg.Add(1)
			go readOne(res, &wg, key[:], d)
		}

		wg.Wait()

		close(res)
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

type FacetFilter = func(bucket *bolt.Bucket, wg *sync.WaitGroup, keys *map[[8]byte]byte) map[[8]byte]byte

func Facet(name string, values ...string) FacetFilter {
	return func(bucket *bolt.Bucket, wgg *sync.WaitGroup, keys *map[[8]byte]byte) map[[8]byte]byte {
		defer wgg.Done()
		(*keys) = map[[8]byte]byte{}
		for _, value := range values {
			v, _ := json.Marshal(value)

			b := bucket.Bucket([]byte(strings.ToLower(name)))
			if b == nil {
				return nil
			}
			keyBytes := b.Get(v)
			for i := 0; i < len(keyBytes); i = i + chunkSize {
				// for chunkSize < len(keyBytes) {
				key := keyBytes[i : i+chunkSize]
				(*keys)[[8]byte(key)] = 1
			}
		}
		return nil
	}
}
