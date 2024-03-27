package handlers

import (
	"encoding/json"
	"github.com/fr0stylo/searchbolt"
	bolt "go.etcd.io/bbolt"
	"net/http"
)

type ReindexRequestBody struct {
	Bucket string `json:"bucket"`
}

func Reindex(db *bolt.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var payload ReindexRequestBody
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			r.JSON(w, http.StatusBadRequest, err.Error())
			return
		}

		fts, facets, err := searchbolt.GetMappings(db, payload.Bucket)
		if err != nil {
			r.JSON(w, http.StatusBadRequest, err.Error())
			return
		}

		ftsIdx := []searchbolt.CreateFTS{}
		for _, v := range fts {
			ftsIdx = append(ftsIdx, searchbolt.CreateFTSOpts(v, searchbolt.Ptr(true)))
		}
		facetIdx := []searchbolt.CreateFacet{}
		for k, _ := range facets {
			facetIdx = append(facetIdx, searchbolt.CreateFacetOpts(k, nil, searchbolt.Ptr(true)))
		}

		searchbolt.RecreateFacetIndex(db, payload.Bucket, facetIdx...)
		searchbolt.RecreateFTSIndex(db, payload.Bucket, ftsIdx...)

		w.WriteHeader(http.StatusOK)
	}
}
