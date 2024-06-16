package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var port = flag.Int("port", 8100, "server port")
var db *datastore.Db

func main() {
	flag.Parse()
	h := new(http.ServeMux)
	newDb, err := datastore.NewDb("./out")
	if err != nil {
		panic(err)
	}
	db = newDb

	h.HandleFunc("/db/", handleDb)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func handleDb(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleDbGet(rw, r)
	case http.MethodPost:
		handleDbPost(rw, r)
	default:
		http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleDbGet(rw http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	t := r.URL.Query().Get("type")
	getter := typeToGetter(t)
	if getter == nil {
		http.Error(rw, "Unknown data type", http.StatusBadRequest)
		return
	}
	data, err := getter(key)

	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
	} else {
		_ = json.NewEncoder(rw).Encode(data)
	}
}

func typeToGetter(t string) func(string) (interface{}, error) {
	if t == "" || t == "string" {
		return get
	} else {
		return nil
	}
}

func get(key string) (interface{}, error) {
	value, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	data := struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}{key, value}
	return data, nil
}

func handleDbPost(rw http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	value := r.FormValue("value")
	t := r.URL.Query().Get("type")
	putter := typeToPutter(t)
	if putter == nil {
		http.Error(rw, "Unknown data type", http.StatusBadRequest)
		return
	}
	err := putter(key, value)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
	}
}

func typeToPutter(t string) func(string, string) error {
	if t == "" || t == "string" {
		return put
	} else {
		return nil
	}
}

func put(key, value string) error {
	if value == "" {
		return fmt.Errorf("Can't save empty value")
	}
	return db.Put(key, value)
}
