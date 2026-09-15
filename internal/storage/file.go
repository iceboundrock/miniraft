package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/iceboundrock/miniraft/internal/raft"
)

// File names inside a FileStorage directory.
const (
	stateFileName = "state.json" // {"currentTerm": N, "votedFor": "id"}
	logFileName   = "log.jsonl"  // one JSON-encoded raft.LogEntry per line
	tmpSuffix     = ".tmp"       // staging file for an atomic replace
)

// ErrFailed is wrapped by every error a FileStorage returns once it has lost
// track of what its files hold: a write failed and the rollback that would
// have restored the previous state failed too, or an atomic replace failed
// after the rename. Such a store refuses every further call so that nothing
// is written on top of bytes whose fate is unknown; the process must exit
// and a new Open reconciles the directory. The in-memory copy of a failed
// store still describes the last acknowledged state but the disk may hold an
// unacknowledged write (in full or as a prefix), which is the same situation
// as a crash between write and fsync and is handled by the load rules.
var ErrFailed = errors.New("storage: failed, reopen required")

// FileStorage is the durable raft.Storage used by a real process. All state
// lives in one directory as human-readable JSON (see the package doc for the
// durability model):
//
//   - state.json holds currentTerm and votedFor and is replaced atomically on
//     every SaveTermVote.
//   - log.jsonl is append-only NDJSON; AppendEntries appends lines and fsyncs,
//     TruncateSuffix rewrites the whole file atomically from the in-memory
//     copy (truncation is rare: it happens only on a log conflict).
//
// The mutex protects every field. Storage I/O happens while it is held, which
// is deliberate: the core calls Storage synchronously and expects the write to
// be durable when the method returns.
type FileStorage struct {
	mu       sync.Mutex
	dir      string
	logFile  logFile // O_APPEND handle on log.jsonl; nil once closed
	logSize  int64   // bytes of log.jsonl that hold acknowledged, fsynced lines
	term     raft.Term
	votedFor raft.NodeID
	entries  []raft.LogEntry
	closed   bool
	failed   error // non-nil once the disk state is unknown; see ErrFailed
}

// logFile is the part of *os.File the log append path uses. It is an
// interface only so that tests can inject write, sync and truncate failures,
// which a real filesystem does not produce on request.
type logFile interface {
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
}

// stateFile is the JSON shape of state.json.
type stateFile struct {
	CurrentTerm raft.Term   `json:"currentTerm"`
	VotedFor    raft.NodeID `json:"votedFor"`
}

// Open opens (creating if necessary) the storage directory and loads its
// contents into memory. Missing files mean a fresh node. A leftover *.tmp
// from a crash before rename is discarded. A partial trailing line in
// log.jsonl (crash mid-append) is discarded and the file is trimmed to the
// last complete line; any other corruption is an error and is never repaired.
func Open(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: open: %w", err)
	}
	// A *.tmp file was never renamed into place, so whatever it holds was not
	// acknowledged to the core; removing it keeps the directory unambiguous.
	for _, name := range []string{stateFileName, logFileName} {
		if err := os.Remove(filepath.Join(dir, name+tmpSuffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("storage: open: remove stale %s: %w", name+tmpSuffix, err)
		}
	}

	term, votedFor, err := loadState(filepath.Join(dir, stateFileName))
	if err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, logFileName)
	entries, size, err := loadLog(logPath)
	if err != nil {
		return nil, err
	}

	// The append handle. O_APPEND makes every Write land at the current end
	// of file, so the in-memory logSize and the file stay in step.
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", logFileName, err)
	}
	// If the file was just created, its directory entry is not durable until
	// the directory is synced; without this a crash could lose the file even
	// though later appends fsync its contents.
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return &FileStorage{
		dir:      dir,
		logFile:  f,
		logSize:  size,
		term:     term,
		votedFor: votedFor,
		entries:  entries,
	}, nil
}

// loadState reads state.json. A missing file is the zero state.
func loadState(path string) (raft.Term, raft.NodeID, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, raft.None, nil
	}
	if err != nil {
		return 0, raft.None, fmt.Errorf("storage: read %s: %w", stateFileName, err)
	}
	var s stateFile
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, raft.None, fmt.Errorf("storage: parse %s: %w", stateFileName, err)
	}
	return s.CurrentTerm, s.VotedFor, nil
}

// loadLog reads log.jsonl and returns the entries plus the byte length of the
// valid prefix. A missing file is the empty log.
//
// The file is a sequence of newline-terminated lines. A final segment without
// its newline is the visible trace of a crash between Write and Sync in
// AppendEntries; that append never returned to the core, so the segment is
// discarded (even if it happens to parse) and the file is trimmed so the next
// append starts on a clean line boundary. A terminated line that does not
// parse is corruption before the last append and is reported, not repaired.
func loadLog(path string) ([]raft.LogEntry, int64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("storage: read %s: %w", logFileName, err)
	}

	var entries []raft.LogEntry
	valid := 0 // bytes covered by complete lines
	for lineNo := 1; ; lineNo++ {
		line, rest, ok := bytes.Cut(data[valid:], []byte{'\n'})
		if !ok {
			break // unterminated tail (possibly empty)
		}
		var e raft.LogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, 0, fmt.Errorf("storage: %s line %d: %w", logFileName, lineNo, err)
		}
		entries = append(entries, e)
		valid = len(data) - len(rest)
	}
	if err := raft.ValidateEntries(entries, 1); err != nil {
		return nil, 0, fmt.Errorf("storage: %s: %w", logFileName, err)
	}

	if valid < len(data) {
		if err := truncateAndSync(path, int64(valid)); err != nil {
			return nil, 0, err
		}
	}
	return entries, int64(valid), nil
}

// usable is the guard at the top of every operation: ErrClosed after Close,
// the sticky failure after fail, nil otherwise. Callers hold f.mu.
func (f *FileStorage) usable() error {
	if f.closed {
		return ErrClosed
	}
	return f.failed
}

// fail records that the on-disk state is unknown, marks the store unusable
// and returns the error every later call will also return. Callers hold f.mu.
func (f *FileStorage) fail(cause error) error {
	f.failed = fmt.Errorf("%w: %w", ErrFailed, cause)
	return f.failed
}

// Load implements raft.Storage. It never touches the disk: Open already
// loaded everything, and every mutation keeps the in-memory copy current.
func (f *FileStorage) Load() (raft.PersistentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.usable(); err != nil {
		return raft.PersistentState{}, err
	}
	return raft.PersistentState{
		CurrentTerm: f.term,
		VotedFor:    f.votedFor,
		Entries:     raft.CloneEntries(f.entries),
	}, nil
}

// SaveTermVote implements raft.Storage by atomically replacing state.json.
func (f *FileStorage) SaveTermVote(term raft.Term, votedFor raft.NodeID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.usable(); err != nil {
		return err
	}
	data, err := json.Marshal(stateFile{CurrentTerm: term, VotedFor: votedFor})
	if err != nil {
		return fmt.Errorf("storage: encode state: %w", err)
	}
	tmp, err := replaceFile(f.dir, stateFileName, append(data, '\n'))
	if err != nil {
		return f.replaceFailed(err)
	}
	// The new state is durable once replaceFile returns, so the in-memory copy
	// follows the disk unconditionally; closing the handle is cleanup and
	// cannot undo the replace (see replaceFile), so its result is not reported.
	f.term, f.votedFor = term, votedFor
	_ = tmp.Close()
	return nil
}

// AppendEntries implements raft.Storage. The whole batch is encoded first and
// written with a single Write followed by Sync, so on success every line is
// durable. On failure the in-memory log is unchanged and the file is rolled
// back to its previous length (see rollbackTail), so the core's retry of the
// same suffix appends exactly one copy of it.
func (f *FileStorage) AppendEntries(entries []raft.LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.usable(); err != nil {
		return err
	}
	if err := raft.ValidateEntries(entries, raft.Index(len(f.entries)+1)); err != nil {
		return fmt.Errorf("storage: append: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	data, err := encodeLog(entries)
	if err != nil {
		return err
	}
	if _, err := f.logFile.Write(data); err != nil {
		return f.rollbackTail(fmt.Errorf("storage: append %s: %w", logFileName, err))
	}
	if err := f.logFile.Sync(); err != nil {
		return f.rollbackTail(fmt.Errorf("storage: sync %s: %w", logFileName, err))
	}
	f.logSize += int64(len(data))
	f.entries = append(f.entries, raft.CloneEntries(entries)...)
	return nil
}

// rollbackTail restores log.jsonl to its acknowledged length after an append
// failed with cause, and returns the error the caller should report.
//
// A failed Write or Sync may have left nothing, a partial line, or every
// line of the batch in the file (a failed fsync says nothing about which
// bytes reached the disk). Truncating to logSize removes all of it and the
// Sync makes the cut durable, so afterwards the file holds exactly the
// acknowledged log and the next append (typically the core retrying the same
// batch) starts on a clean line boundary. logSize itself was made durable by
// the Sync of the append that produced it, so this Sync only has to persist
// the shorter length.
//
// If the truncate or its Sync fails the file's content is unknown and the
// store fails (ErrFailed): were it to stay usable, the retry would append a
// second copy of the batch after a surviving first one, and ValidateEntries
// would reject the log on the next Open. Refusing every further write keeps
// the file loadable; a restart reconciles it.
func (f *FileStorage) rollbackTail(cause error) error {
	if err := f.logFile.Truncate(f.logSize); err != nil {
		return f.fail(fmt.Errorf("%w; rollback truncate: %w", cause, err))
	}
	if err := f.logFile.Sync(); err != nil {
		return f.fail(fmt.Errorf("%w; rollback sync: %w", cause, err))
	}
	return cause
}

// replaceFailed turns a replaceFile error into the error to report. A
// failure before the rename left the old file intact and the store usable;
// a failure after it (errAfterRename) means the new file is in place but not
// known to be durable and, for log.jsonl, that the append handle still
// points at the old, unlinked file, so the store fails.
func (f *FileStorage) replaceFailed(err error) error {
	if errors.Is(err, errAfterRename) {
		return f.fail(err)
	}
	return err
}

// TruncateSuffix implements raft.Storage. The retained prefix is rewritten to
// a temporary file that atomically replaces log.jsonl; the append handle is
// swapped for the new file's handle in the same step.
func (f *FileStorage) TruncateSuffix(from raft.Index) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.usable(); err != nil {
		return err
	}
	if from > raft.Index(len(f.entries)) {
		return nil // nothing to delete
	}
	var keep []raft.LogEntry
	if from > 0 {
		keep = f.entries[:from-1]
	}
	data, err := encodeLog(keep)
	if err != nil {
		return err
	}
	newFile, err := replaceFile(f.dir, logFileName, data)
	if err != nil {
		return f.replaceFailed(err)
	}
	// The new handle was opened with O_APPEND on the file that is now
	// log.jsonl. The old handle points at the unlinked previous file: nothing
	// will read or write it again, and the truncation is already durable, so a
	// failure to close it is not reported as a failed truncation.
	old := f.logFile
	f.logFile = newFile
	f.logSize = int64(len(data))
	f.entries = raft.CloneEntries(keep)
	_ = old.Close()
	return nil
}

// Close implements raft.Storage. It is idempotent and also closes a failed
// store.
func (f *FileStorage) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	err := f.logFile.Close()
	f.logFile = nil
	if err != nil {
		return fmt.Errorf("storage: close %s: %w", logFileName, err)
	}
	return nil
}

// encodeLog renders entries as NDJSON: one JSON object per line.
func encodeLog(entries []raft.LogEntry) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf) // Encode appends the trailing newline
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return nil, fmt.Errorf("storage: encode entry %d: %w", e.Index, err)
		}
	}
	return buf.Bytes(), nil
}

// replaceFile atomically replaces dir/name with data and returns an O_APPEND
// handle on the new file (the caller closes it, or keeps it for appending).
// The sequence and the reason for each step:
//
//  1. write dir/name.tmp   — the old file stays intact while the new content
//     is still incomplete;
//  2. tmp.Sync()           — the new content is on disk before it can become
//     visible under the real name (rename is not ordered against data writes);
//  3. os.Rename(tmp, name) — POSIX rename is atomic: a reader sees either the
//     complete old file or the complete new one, never a mix;
//  4. syncDir(dir)         — the rename itself is a directory modification and
//     only becomes durable once the directory is synced.
//
// Step 4 is the commit point: when replaceFile returns nil the replace has
// happened and is durable, and nothing the caller does afterwards (closing the
// returned handle, releasing an older one) can undo it. Callers therefore
// update their in-memory state as soon as replaceFile returns and treat handle
// cleanup as best-effort, so that nil always means "durable". An error from
// steps 1-3 leaves the old file intact under its name. An error from step 4
// wraps errAfterRename: the new file is already under the name, its
// durability is unknown, and the caller must not carry on as if the old one
// were still there.
func replaceFile(dir, name string, data []byte) (*os.File, error) {
	path := filepath.Join(dir, name)
	tmpPath := path + tmpSuffix
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: create %s: %w", name+tmpSuffix, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("storage: write %s: %w", name+tmpSuffix, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("storage: sync %s: %w", name+tmpSuffix, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("storage: rename %s: %w", name+tmpSuffix, err)
	}
	if err := syncDir(dir); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("%w: %s: %w", errAfterRename, name, err)
	}
	return tmp, nil
}

// errAfterRename marks a replaceFile failure in the step after the rename.
var errAfterRename = errors.New("storage: replace failed after rename")

// truncateAndSync cuts path to size and makes the cut durable.
func truncateAndSync(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("storage: open %s for repair: %w", filepath.Base(path), err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("storage: trim partial tail of %s: %w", filepath.Base(path), err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("storage: sync %s: %w", filepath.Base(path), err)
	}
	return nil
}

// syncDir fsyncs a directory so that renames and file creations inside it are
// durable. It is a variable so that a test can make it fail: it is the one
// step of replaceFile that runs after the rename.
var syncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storage: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("storage: sync dir: %w", err)
	}
	return nil
}
