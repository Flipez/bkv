package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type API struct {
	db     *badger.DB
	Server *chi.Mux
}

func main() {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(authMiddleware)

	kvAPI := &API{
		db:     db,
		Server: r,
	}

	r.Get("/{key}", kvAPI.getValue)
	r.Get("/", kvAPI.listValue)
	r.Post("/{key}", kvAPI.setValue)

	http.ListenAndServe(":3000", kvAPI.Server)
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")

		if len(token) == 6 {
			next.ServeHTTP(w, r)
		} else {
			w.WriteHeader(401)
			w.Write([]byte("Unknown user"))
		}
	})
}

func (api *API) setValue(w http.ResponseWriter, r *http.Request) {
	accessToken := r.Header.Get("Authorization")
	keyParam := chi.URLParam(r, "key")
	value, err := io.ReadAll(r.Body)

	if err != nil {
		log.Fatal(err)
	}

	err = api.db.Update(func(txn *badger.Txn) error {
		dbKey := fmt.Sprintf("%s/%s", accessToken, keyParam)
		txn.Set([]byte(dbKey), value)
		return nil
	})
}

func (api *API) getValue(w http.ResponseWriter, r *http.Request) {
	accessToken := r.Header.Get("Authorization")
	keyParam := chi.URLParam(r, "key")

	api.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("%s/%s", accessToken, keyParam))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				w.Write(v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (api *API) listValue(w http.ResponseWriter, r *http.Request) {
	accessToken := r.Header.Get("Authorization")

	api.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(accessToken)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				fmt.Println(len(accessToken))
				w.Write(k[len(accessToken)+1:])
				w.Write([]byte("\n"))
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}
