package handlers

import (
	"io"
	"net/http"
	"strings"

	"github.com/fr0stylo/searchbolt"
)

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

func Search(queryer searchbolt.Queryer) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		err := req.ParseForm()
		if err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		defer req.Body.Close()
		var payload SearchRequest
		if err := decoder.Decode(&payload, req.Form); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		rw, err := queryer.QueryReader("creators", payload.Query, payload.GenerateFilters()...)
		if err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rw)
	}
}
