package searchbolt

import (
	"bytes"
	"io"
	"sync"

	bolt "go.etcd.io/bbolt"
)

func Query(db *bolt.DB, bucket string, query string, facet ...FacetFilter) (io.Reader, error) {
	buf := make([]byte, 0, 1024*1024)
	rawresult := bytes.NewBuffer(buf)
	err := db.View(func(tx *bolt.Tx) error {
		rawresult.WriteRune('[')
		defer rawresult.WriteRune(']')

		b := tx.Bucket([]byte(bucket))
		fts := b.Bucket([]byte("fts"))
		fb := b.Bucket([]byte("facets"))
		d := b.Bucket([]byte("data"))
		preparedQuery := prepareQuery(query)
		loc, err := scanFTSIndex(fts, preparedQuery)
		if err != nil {
			return err
		}
		var wgg sync.WaitGroup
		keys := make([]map[[8]byte]byte, len(facet))
		for i, f := range facet {
			wgg.Add(1)
			go f(fb, &wgg, &keys[i])
		}

		wgg.Wait()
		if len(query) > 0 {
			mapUnion(loc, keys...)
		} else if len(keys) > 1 {
			mapUnion(keys[0], keys[1:]...)
		}

		res, _ := retrieveByKeys(d, loc)
		for r := range res {
			rawresult.Write(r)
			if len(res) != 0 {
				rawresult.WriteRune(',')
			}
		}

		return nil
	})

	return rawresult, err
}
