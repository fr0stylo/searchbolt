package searchbolt

import (
	"encoding/json"
	"fmt"
	"github.com/fr0stylo/searchbolt/database"
	bolt "go.etcd.io/bbolt"
	"io"
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

type Queryer interface {
	QueryReader(bucket string, query string, facet ...FacetFilter) (io.Reader, error)
}

type Writter interface {
	UpsertBatch(bucketName string, items []BatchEntry) (resIds []string, err error)
	UpsertObjectBatch(bucket string, items []BatchEntry) (resIds []string, err error)
}

type Indexer interface {
	RecreateFacetIndex(bucket string, facets ...CreateFacet) error
	RecreateFTSIndex(bucket string, facets ...CreateFTS) error
	IndexItem(bucket string, objId []byte) error
	GetMappings(bucket string) ([]string, map[string]string, error)
	CreateMappings(bucket string, filters map[string]string, search []string) error
}

type SearchBolt struct {
	db         *bolt.DB
	indexerC   chan *IndexRequest
	ftsIndex   *bolt.DB
	facetIndex *bolt.DB
}

func (b *SearchBolt) Close() error {
	close(b.indexerC)
	return b.db.Close()
}

func (b *SearchBolt) QueryReader(bucket string, query string, facet ...FacetFilter) (io.Reader, error) {
	return QueryReader(b.db, bucket, query, facet...)
}

func (b *SearchBolt) UpsertBatch(bucketName string, items []BatchEntry) (resIds []string, err error) {
	return UpsertBatch(b.db, b.indexerC, bucketName, items)
}

func (b *SearchBolt) RecreateFacetIndex(bucket string, facets ...CreateFacet) error {
	return RecreateFacetIndex(b.db, bucket, facets...)
}

func (b *SearchBolt) RecreateFTSIndex(bucket string, facets ...CreateFTS) error {
	return RecreateFTSIndex(b.db, bucket, facets...)
}

func (b *SearchBolt) IndexItem(bucket string, objId []byte) error {
	b.indexerC <- &IndexRequest{bucket, objId}
	return nil
}

func (b *SearchBolt) GetMappings(bucket string) ([]string, map[string]string, error) {
	return GetMappings(b.db, bucket)
}

func (b *SearchBolt) CreateMappings(bucket string, filters map[string]string, search []string) error {
	return CreateMappings(b.db, bucket, filters, search)
}

func (b *SearchBolt) UpsertObjectBatch(bucket string, items []BatchEntry) (resIds []string, err error) {
	db := database.NewDatabase(b.db, bucket)
	return UpsertObjectBatch(db, b.indexerC, bucket, items)
}

func NewSearchBolt(path string) (*SearchBolt, error) {

	db, err := bolt.Open(fmt.Sprintf("%s", path), 0600, nil) // &bolt.Options{ReadOnly: true, NoSync: true, NoGrowSync: true, NoFreelistSync: true})
	if err != nil {
		return nil, err
	}
	//fts, err := bolt.Open(fmt.Sprintf("%s/fts", path), 0600, nil) // &bolt.Options{ReadOnly: true, NoSync: true, NoGrowSync: true, NoFreelistSync: true})
	//if err != nil {
	//	return nil, err
	//}
	//facet, err := bolt.Open(fmt.Sprintf("%s/", path), 0600, nil) // &bolt.Options{ReadOnly: true, NoSync: true, NoGrowSync: true, NoFreelistSync: true})
	//if err != nil {
	//	return nil, err
	//}

	c := StartIndexer(db)

	return &SearchBolt{
		db:         db,
		ftsIndex:   nil,
		facetIndex: nil,
		indexerC:   c,
	}, nil
}
