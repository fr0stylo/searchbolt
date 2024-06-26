package server

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fr0stylo/searchbolt"
	"github.com/fr0stylo/searchbolt/server/handlers"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	bolt "go.etcd.io/bbolt"
)

func stats(db *bolt.DB) {
	prev := db.Stats()

	for {
		time.Sleep(10 * time.Second)

		stats := db.Stats()
		diff := stats.Sub(&prev)

		json.NewEncoder(os.Stderr).Encode(diff)

		prev = stats
	}
}

type SearchRequest struct {
	Query      string   `schema:"q"`
	FilterList []string `schema:"filter"`
}

func (r *SearchRequest) GenerateFilters() []searchbolt.FacetFilter {
	fns := make([]searchbolt.FacetFilter, 0)

	for _, v := range r.FilterList {
		s := strings.Split(v, "=")
		if len(s) == 2 {
			vals := strings.Split(s[1], "|")
			fns = append(fns, searchbolt.Facet(strings.ToLower(s[0]), vals...))
		}
	}

	return fns
}

// func init() {
// 	rq := SearchRequest{
// 		QueryReader: "test asdas asd a",
// 		FilterList: []string{
// 			"Emotion=joy|joyfull",
// 			"Emotional Intensity=high",
// 		}}

// 	form := url.Values{}
// 	schema.NewEncoder().Encode(rq, form)
// 	log.Print(form.Encode())
// }

func ListenAndServe(db *searchbolt.SearchBolt, addr string) error {
	c := chi.NewRouter()
	c.Use(
		middleware.Logger,
		middleware.Recoverer,
		middleware.RequestID,
		middleware.Heartbeat("/health"),
	)

	c.Get("/", handlers.Search(db))
	c.Put("/", handlers.Insert(db))
	c.Put("/batch", handlers.InsertBatch(db))
	c.Post("/mappings", handlers.CreateMappings(db))
	c.Get("/mappings", handlers.GetMappings(db))
	c.Post("/reindex", handlers.Reindex(db))

	log.Print("Listening")
	return http.ListenAndServe(addr, c)
}
