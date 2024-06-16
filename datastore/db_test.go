package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(filepath.Join(dir, db.segmentName+strconv.Itoa((db.segmentNumber))))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}

		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})
}

func TestDb_Delete(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db-delete")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, pair := range pairs {
		err := db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatalf("Cannot put %s: %s", pair[0], err)
		}
	}

	t.Run("delete", func(t *testing.T) {
		err := db.Delete("key2")
		if err != nil {
			t.Fatalf("Cannot delete key2: %s", err)
		}

		_, err = db.Get("key2")
		if err == nil {
			t.Fatalf("Expected error when getting deleted key2, but got none")
		}

		for _, pair := range pairs {
			if pair[0] != "key2" {
				value, err := db.Get(pair[0])
				if err != nil {
					t.Errorf("Cannot get %s: %s", pair[0], err)
				}
				if value != pair[1] {
					t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
				}
			}
		}
	})
}
