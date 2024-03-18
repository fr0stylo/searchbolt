package searchbolt

import (
	"encoding/json"
	"strings"

	bolt "go.etcd.io/bbolt"
)

func CreateFacetsIndex(db *bolt.DB, bucket string, facets ...string) error {
	return db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		dataBucket := bucket.Bucket([]byte("data"))
		facetsBucket, _ := bucket.CreateBucketIfNotExists([]byte("facets"))
		for _, facet := range facets {
			ff := []byte(strings.ToLower(facet))
			facetsBucket.DeleteBucket(ff)
			if err := dataBucket.ForEach(func(k, v []byte) error {
				var q map[string]any
				if err := json.Unmarshal(v, &q); err != nil {
					return err
				}
				fb, _ := facetsBucket.CreateBucketIfNotExists(ff)
				b, err := json.Marshal(q[facet])
				if err != nil {
					return err
				}

				val := fb.Get(b)
				if val == nil {
					if err := fb.Put(b, k); err != nil {
						return err
					}
				} else {
					val = append(val, k...)

					if err := fb.Put(b, val); err != nil {
						return err
					}
				}

				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})
}

type CreateFacet = func(v map[string]any) (string, []string)

func CreateFacetOpts(name string, separator *string, unify *bool) CreateFacet {
	return func(v map[string]any) (string, []string) {
		val := v[name].(string)
		if unify != nil && *unify {
			val = strings.ToLower(val)
		}
		if separator == nil {
			return name, []string{val}
		}

		vals := strings.Split(val, *separator)
		res := []string{}
		for _, split := range vals {
			res = append(res, strings.Trim(split, " "))
		}

		return strings.ToLower(name), res
	}
}

func CreateFacetsIndexFn(db *bolt.DB, bucket string, facets ...CreateFacet) error {
	return db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		dataBucket := bucket.Bucket([]byte("data"))
		bucket.DeleteBucket([]byte("facets"))
		facetsBucket, _ := bucket.CreateBucketIfNotExists([]byte("facets"))
		for _, facet := range facets {
			if err := dataBucket.ForEach(func(k, v []byte) error {
				var q map[string]any
				if err := json.Unmarshal(v, &q); err != nil {
					return err
				}

				name, items := facet(q)
				fb, _ := facetsBucket.CreateBucketIfNotExists([]byte(name))
				for _, fi := range items {
					b, err := json.Marshal(fi)
					if err != nil {
						return err
					}

					val := fb.Get(b)
					if val == nil {
						if err := fb.Put(b, k); err != nil {
							return err
						}
					} else {
						val = append(val, k...)

						if err := fb.Put(b, val); err != nil {
							return err
						}
					}
				}

				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})
}
