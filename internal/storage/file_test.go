package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/iceboundrock/miniraft/internal/raft"
	"github.com/iceboundrock/miniraft/internal/storage/storagetest"
)

// mustOpen opens dir and fails the test on error.
func mustOpen(t *testing.T, dir string) *FileStorage {
	t.Helper()
	fs, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return fs
}

// reopenAfterCrash simulates a process crash: the directory is reopened while
// crashed is abandoned without Close, exactly as after a kill, so nothing runs
// between the last acknowledged write and the reopen. The abandoned handle is
// released at test end only so the test does not leak a descriptor;
// FileStorage.Close writes nothing, so that cannot affect what the reopen saw.
func reopenAfterCrash(t *testing.T, crashed *FileStorage) *FileStorage {
	t.Helper()
	t.Cleanup(func() { crashed.Close() })
	fs := mustOpen(t, crashed.dir)
	t.Cleanup(func() { fs.Close() })
	return fs
}

func mustLoad(t *testing.T, st raft.Storage) raft.PersistentState {
	t.Helper()
	s, err := st.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return s
}

func TestFileStorageFreshDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node-a") // Open must create it
	fs := mustOpen(t, dir)
	defer fs.Close()
	if s := mustLoad(t, fs); !reflect.DeepEqual(s, raft.PersistentState{}) {
		t.Fatalf("fresh Load() = %+v, want zero", s)
	}
}

func TestFileStorageReopenPreservesState(t *testing.T) {
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	if err := fs.SaveTermVote(5, "b"); err != nil {
		t.Fatal(err)
	}
	batch := storagetest.Entries([2]uint64{1, 2}, [2]uint64{2, 5}, [2]uint64{3, 5})
	if err := fs.AppendEntries(batch); err != nil {
		t.Fatal(err)
	}
	fs2 := reopenAfterCrash(t, fs)
	want := raft.PersistentState{CurrentTerm: 5, VotedFor: "b", Entries: batch}
	if s := mustLoad(t, fs2); !reflect.DeepEqual(s, want) {
		t.Fatalf("after reopen Load() = %+v, want %+v", s, want)
	}
}

func TestFileStorageTruncateThenAppend(t *testing.T) {
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1})); err != nil {
		t.Fatal(err)
	}
	if err := fs.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{2, 3}, [2]uint64{3, 3})); err != nil {
		t.Fatal(err)
	}
	fs2 := reopenAfterCrash(t, fs)
	want := storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 3}, [2]uint64{3, 3})
	if s := mustLoad(t, fs2); !reflect.DeepEqual(s.Entries, want) {
		t.Fatalf("after reopen Entries = %+v, want %+v", s.Entries, want)
	}
	// The rewritten file must contain exactly three lines.
	data, err := os.ReadFile(filepath.Join(dir, logFileName))
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(data, []byte("\n")); n != 3 {
		t.Fatalf("log.jsonl has %d lines, want 3:\n%s", n, data)
	}
}

func TestFileStorageDiscardsPartialTrailingLine(t *testing.T) {
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 1})); err != nil {
		t.Fatal(err)
	}
	// Crash mid-append: chop the file in the middle of the last line.
	path := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.SplitAfter(data, []byte("\n"))
	cut := len(lines[0]) + len(lines[1])/2
	if err := os.Truncate(path, int64(cut)); err != nil {
		t.Fatal(err)
	}

	fs2 := reopenAfterCrash(t, fs)
	want := storagetest.Entries([2]uint64{1, 1})
	if s := mustLoad(t, fs2); !reflect.DeepEqual(s.Entries, want) {
		t.Fatalf("Load() after partial tail = %+v, want %+v", s.Entries, want)
	}
	// A subsequent append must produce a valid file: reopen again and check.
	if err := fs2.AppendEntries(storagetest.Entries([2]uint64{2, 7})); err != nil {
		t.Fatal(err)
	}
	fs3 := reopenAfterCrash(t, fs2)
	want = storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 7})
	if s := mustLoad(t, fs3); !reflect.DeepEqual(s.Entries, want) {
		t.Fatalf("Load() after append over repaired tail = %+v, want %+v", s.Entries, want)
	}
}

func TestFileStorageDiscardsUnterminatedValidLine(t *testing.T) {
	// A final line that parses but lacks its newline was never fully written;
	// it is discarded too, so the rule is "no newline ⇒ the append did not
	// happen" regardless of content.
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 1})); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, int64(len(data)-1)); err != nil {
		t.Fatal(err)
	}
	fs2 := reopenAfterCrash(t, fs)
	if s := mustLoad(t, fs2); len(s.Entries) != 1 {
		t.Fatalf("Load() = %+v, want only entry 1", s.Entries)
	}
}

func TestFileStorageMidFileCorruptionIsError(t *testing.T) {
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1})); err != nil {
		t.Fatal(err)
	}
	fs.Close()
	path := filepath.Join(dir, logFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(data), "\n")
	lines[1] = "{\"index\":2,\"term\":garbage}\n"
	if err := os.WriteFile(path, []byte(strings.Join(lines, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open succeeded on a log with corruption before the last line")
	}
}

func TestFileStorageIgnoresStaleTmp(t *testing.T) {
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	if err := fs.SaveTermVote(3, "c"); err != nil {
		t.Fatal(err)
	}
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{1, 3})); err != nil {
		t.Fatal(err)
	}
	fs.Close()
	// Crash after writing the tmp files but before rename: both must be
	// ignored and removed; the committed files win.
	stateTmp := filepath.Join(dir, stateFileName+tmpSuffix)
	logTmp := filepath.Join(dir, logFileName+tmpSuffix)
	if err := os.WriteFile(stateTmp, []byte(`{"currentTerm":99,"votedFor":"z"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logTmp, []byte("not even json"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs2 := mustOpen(t, dir)
	defer fs2.Close()
	s := mustLoad(t, fs2)
	if s.CurrentTerm != 3 || s.VotedFor != "c" || len(s.Entries) != 1 {
		t.Fatalf("Load() = %+v, want term 3 / vote c / 1 entry", s)
	}
	for _, p := range []string{stateTmp, logTmp} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s still exists after Open (err=%v)", p, err)
		}
	}
}

func TestFileStorageNonContiguousIndexIsError(t *testing.T) {
	dir := t.TempDir()
	log := `{"index":1,"term":1,"command":"AQ=="}` + "\n" + `{"index":3,"term":1,"command":"Aw=="}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, logFileName), []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open succeeded on a log with an index gap")
	}
	// A log that does not start at index 1 is equally invalid.
	log = `{"index":2,"term":1,"command":"Ag=="}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, logFileName), []byte(log), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open succeeded on a log starting at index 2")
	}
}

func TestFileStorageCorruptStateIsError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, stateFileName), []byte(`{"currentTerm":`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open succeeded on a truncated state.json")
	}
}

func TestFileStorageStateFileIsHumanReadable(t *testing.T) {
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	defer fs.Close()
	if err := fs.SaveTermVote(7, "b"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"currentTerm"`)) || !bytes.Contains(data, []byte(`"votedFor"`)) {
		t.Fatalf("state.json = %s, want currentTerm/votedFor keys", data)
	}
}

func TestFileStorageTruncateSucceedsWhenOldHandleCloseFails(t *testing.T) {
	// Once the rewritten log has been renamed into place and the directory
	// synced, the truncation is durable; releasing the previous append handle
	// is cleanup and cannot fail it. Force that Close to fail by closing the
	// handle underneath the storage (white-box) and check the result still
	// reflects the disk.
	dir := t.TempDir()
	fs := mustOpen(t, dir)
	defer fs.Close()
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 1})); err != nil {
		t.Fatal(err)
	}
	fs.logFile.Close()
	if err := fs.TruncateSuffix(2); err != nil {
		t.Fatalf("TruncateSuffix reported failure although the rewrite is durable: %v", err)
	}
	// The storage keeps working on the new handle.
	if err := fs.AppendEntries(storagetest.Entries([2]uint64{2, 4})); err != nil {
		t.Fatal(err)
	}
	fs2 := mustOpen(t, dir)
	defer fs2.Close()
	want := storagetest.Entries([2]uint64{1, 1}, [2]uint64{2, 4})
	if s := mustLoad(t, fs2); !reflect.DeepEqual(s.Entries, want) {
		t.Fatalf("after reopen Entries = %+v, want %+v", s.Entries, want)
	}
}
