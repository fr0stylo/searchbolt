package searchbolt_test

import (
	"testing"

	"github.com/fr0stylo/searchbolt"
	bolt "go.etcd.io/bbolt"
)

func BenchmarkFilterFn(b *testing.B) {
	db, err := bolt.Open("search.bbdb", 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := searchbolt.FilterFn(
			db,
			"stock",
			searchbolt.Facet("Emotional Intensity", "high"),
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}
