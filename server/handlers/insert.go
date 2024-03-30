package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/fr0stylo/searchbolt"
	validation "github.com/go-ozzo/ozzo-validation"
)

type InsertRequest struct {
	Id     any
	Data   map[string]any
	Bucket string
}

func (i *InsertRequest) Validate() error {
	return validation.ValidateStruct(i,
		// Street cannot be empty, and the length must between 5 and 50
		validation.Field(&i.Id, validation.Required),
		validation.Field(&i.Data, validation.Required),
		validation.Field(&i.Bucket, validation.Required))
}

type InsertResponse struct {
	Id string `json:"id"`
}

func Insert(writter searchbolt.Writter) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()

		var body InsertRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		if err := body.Validate(); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		id, err := writter.UpsertBatch(body.Bucket, []searchbolt.BatchEntry{{Id: body.Id, Data: body.Data}})
		if err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		r.JSON(w, http.StatusCreated, InsertResponse{Id: id[0]})
	}
}

type InsertBatchRequest struct {
	Data   []searchbolt.BatchEntry `json:"batch"`
	Bucket string                  `json:"bucket"`
}

type InsertBatchResponse struct {
	Ids []string
}

func (i *InsertBatchRequest) Validate() error {
	return validation.ValidateStruct(i,
		validation.Field(&i.Data, validation.Required),
		validation.Field(&i.Bucket, validation.Required))
}

func InsertBatch(writter searchbolt.Writter) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()

		var body InsertBatchRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		if err := body.Validate(); err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		id, err := writter.UpsertBatch(body.Bucket, body.Data)
		if err != nil {
			r.JSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		r.JSON(w, http.StatusCreated, InsertBatchResponse{Ids: id})
	}
}
