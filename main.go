package main

import (
	"crypto/rand"
	"encoding/hex"
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

	r.Get("/", showLandingPage)
	r.Post("/", kvAPI.createBucket)
	r.Get("/{bucket}/{key}", kvAPI.getValue)
	r.Post("/{bucket}/{key}", kvAPI.setValue)
	r.Get("/{bucket}", kvAPI.listValue)

	http.ListenAndServe(":3000", kvAPI.Server)
}

func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")

		// Check if a token is present on every page other than the landing page
		if r.URL.Path == "/" || len(token) == 6 {
			next.ServeHTTP(w, r)
		} else {
			w.WriteHeader(401)
			w.Write([]byte("Unknown user"))
		}
	})
}

func showLandingPage(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Bobby's KV Store"))
}

func (api *API) checkBucketExists(name string) bool {
	err := api.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(name))
		return err
	})

	return err != badger.ErrKeyNotFound
}

func (api *API) handleError(w http.ResponseWriter, r *http.Request, err string) {
	w.WriteHeader(500)
	w.Write([]byte(err))
}

// called as POST /
func (api *API) createBucket(w http.ResponseWriter, r *http.Request) {
	accessToken := r.Header.Get("Authorization")

	err := api.db.Update(func(txn *badger.Txn) error {
		bucketName, err := randomHex(20)
		if err != nil {
			api.handleError(w, r, fmt.Sprintf("error creating bucket name: %v", err))
			return err
		}

		if api.checkBucketExists(fmt.Sprintf("%s/%s", accessToken, bucketName)) {
			api.handleError(w, r, "error bucket name collsion")
			return nil
		}

		err = txn.Set([]byte(fmt.Sprintf("%s/%s", accessToken, bucketName)), []byte("bucket"))
		if err != nil {
			api.handleError(w, r, fmt.Sprintf("error creating bucket: %v", err))
			return err
		}
		return nil
	})

	if err != nil {
		api.handleError(w, r, fmt.Sprintf("error creating transaction for bucket: %v", err))
	}

}

// called as POST /{bucket}/{key}
func (api *API) setValue(w http.ResponseWriter, r *http.Request) {
	accessToken := r.Header.Get("Authorization")
	keyParam := chi.URLParam(r, "key")
	bucketParam := chi.URLParam(r, "bucket")
	value, err := io.ReadAll(r.Body)

	if err != nil {
		api.handleError(w, r, fmt.Sprintf("error reading body to set %s/%s: %v", bucketParam, keyParam, err))
	}

	err = api.db.Update(func(txn *badger.Txn) error {
		dbKey := fmt.Sprintf("%s/%s", accessToken, keyParam)
		err = txn.Set([]byte(dbKey), value)
		if err != nil {
			api.handleError(w, r, fmt.Sprintf("error set value for %s/%s: %v", bucketParam, keyParam, err))
		}
		return nil
	})
	if err != nil {
		api.handleError(w, r, fmt.Sprintf("error creating transaction to set %s/%s: %v", bucketParam, keyParam, err))
	}
}

// called as GET /{bucket}/{key}
func (api *API) getValue(w http.ResponseWriter, r *http.Request) {
	accessToken := r.Header.Get("Authorization")
	keyParam := chi.URLParam(r, "key")
	bucketParam := chi.URLParam(r, "bucket")

	err := api.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fmt.Sprintf("%s/%s/%s", accessToken, bucketParam, keyParam)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				w.WriteHeader(404)
				return nil
			}
			api.handleError(w, r, fmt.Sprintf("error performing get %s/%s: %v", bucketParam, keyParam, err))
			return err
		}

		var valCopy []byte
		err = item.Value(func(val []byte) error {
			// This func with val would only be called if item.Value encounters no error.
			valCopy = append([]byte{}, val...)
			return nil
		})

		if err != nil {
			api.handleError(w, r, fmt.Sprintf("error fetching value %s/%s: %v", bucketParam, keyParam, err))
			return err
		}

		w.Write(valCopy)

		return nil
	})

	if err != nil {
		log.Fatalf("error performing get transaction for %s/%s", bucketParam, keyParam)
	}
}

// called as GET /{bucket}
func (api *API) listValue(w http.ResponseWriter, r *http.Request) {
	accessToken := r.Header.Get("Authorization")
	bucketParam := chi.URLParam(r, "bucket")

	api.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(accessToken + "/" + bucketParam)
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
