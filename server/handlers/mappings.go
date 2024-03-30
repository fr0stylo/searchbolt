package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"

	"github.com/fr0stylo/searchbolt"
	validation "github.com/go-ozzo/ozzo-validation"
)

type CreateMappingsRequest struct {
	Bucket  string            `json:"bucket"`
	Filters map[string]string `json:"filters"`
	Search  []string          `json:"search"`
}

func (i *CreateMappingsRequest) Validate() error {
	return validation.ValidateStruct(i,
		validation.Field(&i.Filters, validation.Each(validation.Match(regexp.MustCompile("^(number|string|boolean|date)$")))),
		validation.Field(&i.Search),
		validation.Field(&i.Bucket, validation.Required))
}

func CreateMappings(db searchbolt.Indexer) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()

		var body CreateMappingsRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		if err := body.Validate(); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		log.Print("1")
		if err := db.CreateMappings(body.Bucket, body.Filters, body.Search); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		log.Print("2")

		w.WriteHeader(http.StatusCreated)
	}
}

func GetMappings(db searchbolt.Indexer) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()

		fts, facets, err := db.GetMappings("creators")
		if err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		r.JSON(w, http.StatusOK, CreateMappingsRequest{Bucket: "creators", Filters: facets, Search: fts})
	}
}
