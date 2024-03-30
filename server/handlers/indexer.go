package handlers

import (
	"encoding/json"
	"github.com/fr0stylo/searchbolt"
	"net/http"
)

type ReindexRequestBody struct {
	Bucket string `json:"bucket"`
}

func Reindex(db searchbolt.Indexer) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var payload ReindexRequestBody
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			r.JSON(w, http.StatusBadRequest, err.Error())
			return
		}

		fts, facets, err := db.GetMappings(payload.Bucket)
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

		err = db.RecreateFacetIndex(payload.Bucket, facetIdx...)
		if err != nil {
			r.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		err = db.RecreateFTSIndex(payload.Bucket, ftsIdx...)
		if err != nil {
			r.JSON(w, http.StatusBadRequest, err.Error())
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}
