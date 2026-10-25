package verify

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCheck(t *testing.T) {
	submitted := []Submitted{
		{ID: "a", Type: "fast", RunAt: 100},
		{ID: "b", Type: "fast", RunAt: 100},
		{ID: "c", Type: "poison", RunAt: 100},
		{ID: "d", Type: "fast", RunAt: 500},
		{ID: "e", Type: "fast", RunAt: 100},
	}
	completed := []Completed{
		{ID: "a", At: 150},
		{ID: "a", At: 900},    // duplicate execution
		{ID: "d", At: 400},    // ran before runAt
		{ID: "zzz", At: 1000}, // never submitted
		{ID: "b", At: 200},
	}
	r := Check(submitted, completed, map[string]bool{"c": true})

	if r.Submitted != 5 || r.Completed != 3 || r.DeadLettered != 1 {
		t.Fatalf("counts wrong: %+v", r)
	}
	if !reflect.DeepEqual(r.Missing, []string{"e"}) {
		t.Errorf("Missing = %v, want [e]", r.Missing)
	}
	if r.Duplicates["a"] != 2 || r.ExtraExecutions() != 1 {
		t.Errorf("Duplicates = %v, extra = %d", r.Duplicates, r.ExtraExecutions())
	}
	if !reflect.DeepEqual(r.Early, []string{"d"}) {
		t.Errorf("Early = %v, want [d]", r.Early)
	}
	if !reflect.DeepEqual(r.Unexpected, []string{"zzz"}) {
		t.Errorf("Unexpected = %v, want [zzz]", r.Unexpected)
	}
	if r.OK() {
		t.Error("OK() = true, want false")
	}
}

func TestCheckCleanRunWithDuplicatesIsOK(t *testing.T) {
	submitted := []Submitted{{ID: "a", RunAt: 1}, {ID: "b", RunAt: 1}}
	completed := []Completed{{ID: "a", At: 5}, {ID: "a", At: 6}, {ID: "b", At: 7}}
	r := Check(submitted, completed, nil)
	if !r.OK() {
		t.Fatalf("duplicates alone must not fail a run: %+v", r)
	}
}

func TestReadJSONLIgnoresTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "completed.jsonl")
	content := "{\"id\":\"a\",\"at\":1}\n\n{\"id\":\"b\",\"at\":2}\n{\"id\":\"c\",\"a"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadJSONL[Completed](path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("got %+v, want a and b", got)
	}
}

func TestReadJSONLRejectsCorruptMiddle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.jsonl")
	content := "{\"id\":\"a\"}\nnot json\n{\"id\":\"b\"}\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadJSONL[Completed](path); err == nil {
		t.Fatal("expected an error for a corrupt line in the middle of the file")
	}
}
