package storage

import "testing"

// TestExistsContractDistinguishesMissingFromError pins the contract behind
// issue #110: "not found" returns (false, nil), while any other condition must
// be returned as an error so callers cannot mistake a backend fault for
// absence.
func TestExistsContractDistinguishesMissingFromError(t *testing.T) {
	ls := NewLocalStorage(t.TempDir())

	exists, err := ls.Exists("missing.txt")
	if err != nil {
		t.Fatalf("missing path must report (false, nil), got error: %v", err)
	}
	if exists {
		t.Fatal("missing path reported as existing")
	}

	if err := ls.Write("file.txt", []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	exists, err = ls.Exists("file.txt")
	if err != nil {
		t.Fatalf("existing path returned error: %v", err)
	}
	if !exists {
		t.Fatal("existing path reported as missing")
	}

	// A path escaping the storage root is a caller/programming error, not
	// absence.
	if _, err := ls.Exists("../escape.txt"); err == nil {
		t.Fatal("escaping path must return an error rather than false")
	}

	// A non-directory intermediate component ("file.txt/child") is a real
	// filesystem error, not "not found".
	if _, err := ls.Exists("file.txt/child"); err == nil {
		t.Fatal("path through a non-directory component must return an error rather than false")
	}
}

func TestMinIOExistsContractRejectsEscapingKey(t *testing.T) {
	store, _ := newTestMinIOStorage(t)
	ensureBucket(t, store)

	if _, err := store.Exists("../escape.txt"); err == nil {
		t.Fatal("escaping key must return an error rather than false")
	}
	exists, err := store.Exists("missing.txt")
	if err != nil {
		t.Fatalf("missing key must report (false, nil), got error: %v", err)
	}
	if exists {
		t.Fatal("missing key reported as existing")
	}
}
