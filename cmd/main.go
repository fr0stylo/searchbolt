package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fr0stylo/searchbolt/server"
	bolt "go.etcd.io/bbolt"
)

func CSVToMap(reader io.Reader) []map[string]any {
	r := csv.NewReader(reader)
	r.LazyQuotes = true
	r.Comma = '\t'
	rows := []map[string]any{}
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]any{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return rows
}

//
// for range 1000 {
// 	t := time.Now()
// 	_, err := searchbolt.Query(db,
// 		"stock",
// 		"Acne solution",
// 		searchbolt.Facet("Participants", "woman"),
// 	)
// 	tt := time.Since(t)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	log.Print(tt)
// }

// f, _ := os.Create("./res.json")
// io.Copy(f, result)
// defer f.Close()

func ftsWordlis(db *bolt.DB) {
	f, _ := os.Create("./words")
	defer f.Close()
	db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("stock")).Bucket([]byte("fts")).ForEach(func(k, v []byte) error {
			f.Write(k)
			f.Write([]byte{'\n'})
			return nil
		})
	})
}

func send(b []byte) {
	r, err := http.NewRequest(http.MethodPut, "http://localhost:8080/batch", bytes.NewReader(b))
	if err != nil {
		log.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Fatal(err)
	}
	log.Print(resp.Status)
	defer resp.Body.Close()
}

func putData(data []map[string]any) {
	time.Sleep(1 * time.Second)

	batch := []map[string]any{}
	for i, v := range data {
		batch = append(batch, map[string]any{
			"id":   v["Clip ID"],
			"data": v,
		})
		if i == len(data) || i%100 == 0 {
			b, err := json.Marshal(map[string]any{
				"batch":   batch,
				"bucket": "stock",
			})

			if err != nil {
				log.Fatal(err)
			}

			send(b)
		}
	}
}

func main() {
	path := "./search.bbdb"

	db, err := bolt.Open(path, 0600, nil) // &bolt.Options{ReadOnly: true, NoSync: true, NoGrowSync: true, NoFreelistSync: true})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// f, err := os.Open("./cosmetics.tsv")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// m := CSVToMap(f)
	// f.Close()

	// go putData(m)

	// searchbolt.LoadData(db, "stock", func(item map[string]any) [8]byte { return searchbolt.StrKey(item["Clip ID"].(string)) }, m)
	// log.Print(searchbolt.CreateFTSIndex(db, "stock",
	// 	searchbolt.CreateFTSOpts("Video Title", ptr(true)),
	// 	searchbolt.CreateFTSOpts("Video Description", ptr(true)),
	// 	searchbolt.CreateFTSOpts("Keywords", ptr(true)),
	// 	searchbolt.CreateFTSOpts("Action KWs", ptr(true)),
	// 	searchbolt.CreateFTSOpts("Transcript", ptr(true)),
	// ))

	// searchbolt.CreateFacetsIndexFn(db, "stock",
	// 	searchbolt.CreateFacetOpts("Emotions", ptr(","), ptr(true)),
	// 	searchbolt.CreateFacetOpts("Participants", ptr(","), ptr(true)),
	// 	searchbolt.CreateFacetOpts("Box size", nil, ptr(true)),
	// 	searchbolt.CreateFacetOpts("Creator ID", nil, nil),
	// 	searchbolt.CreateFacetOpts("Video type", nil, ptr(true)),
	// 	searchbolt.CreateFacetOpts("Participants", ptr(","), ptr(true)),
	// 	searchbolt.CreateFacetOpts("Emotional Intensity", nil, ptr(true)),
	// 	searchbolt.CreateFacetOpts("Product type", ptr(","), ptr(true)),
	// 	searchbolt.CreateFacetOpts("Visual quality", nil, ptr(true)))

	log.Fatal(server.ListenAndServe(db, ":8080"))
}

func ptr[T any](i T) *T {
	return &i
}
