package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	manifestFile     = "manifest.json"
	manifestTmpFile  = "manifest.json.tmp"
	shardFilePattern = "shard-%06d"
	shardTmpSuffix   = ".tmp"
)

// Store is a filesystem-backed, sharded checkpoint store rooted at dir.
// It is safe for concurrent use within a process; cross-process safety
// relies on the atomic rename commit protocol (readers never observe a
// partially committed checkpoint).
type Store struct {
	dir string

	mu      sync.Mutex
	writers map[string]*Writer // jobID -> active writer

	// commitHook, when set, is invoked at named stages during Commit.
	// Test-only failpoint injection; makes interruption deterministic.
	commitHook func(stage string) error
}

// NewStore creates (or opens) a store rooted at dir.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir, writers: map[string]*Writer{}}, nil
}

func (s *Store) jobDir(jobID string) string { return filepath.Join(s.dir, jobID) }

func (s *Store) checkpointDir(jobID, checkpointID string) string {
	return filepath.Join(s.jobDir(jobID), "checkpoints", checkpointID)
}

// --- Writer ---------------------------------------------------------------

// Writer accumulates shards for one checkpoint. Exactly one Writer may be
// active per job at a time.
type Writer struct {
	store        *Store
	jobID        string
	checkpointID string
	baseID       string
	baseOffsets  map[int]int64 // shard index -> committed end offset of base

	shards map[int]ShardMeta
	closed bool
}

// Begin starts a new checkpoint whose incremental base is the job's latest
// committed checkpoint (empty base if none exists).
func (s *Store) Begin(jobID, checkpointID string) (*Writer, error) {
	base, err := s.Latest(jobID)
	if err != nil && !isKind(err, KindNotFound) {
		return nil, err
	}
	baseID := ""
	if base != nil {
		baseID = base.Manifest.CheckpointID
	}
	return s.begin(jobID, checkpointID, baseID)
}

// BeginFrom starts a new checkpoint on top of an explicit base checkpoint.
// The base must be committed and complete; resuming from a base that has
// regressed relative to the latest committed checkpoint is rejected.
func (s *Store) BeginFrom(jobID, checkpointID, baseID string) (*Writer, error) {
	latest, err := s.Latest(jobID)
	if err != nil && !isKind(err, KindNotFound) {
		return nil, err
	}
	if latest != nil && baseID != latest.Manifest.CheckpointID {
		// Idempotent retry: recommitting the latest checkpoint itself with
		// its original base is allowed; Commit decides no-op vs conflict.
		isRetry := checkpointID == latest.Manifest.CheckpointID &&
			baseID == latest.Manifest.BaseCheckpoint
		if isRetry {
			return s.begin(jobID, checkpointID, baseID)
		}
		return nil, newError("begin", KindRegression, jobID, checkpointID, -1,
			"base %q is behind latest committed checkpoint %q", baseID, latest.Manifest.CheckpointID)
	}
	return s.begin(jobID, checkpointID, baseID)
}

func (s *Store) begin(jobID, checkpointID, baseID string) (*Writer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.writers[jobID]; ok {
		return nil, newError("begin", KindWriterConflict, jobID, checkpointID, -1,
			"another writer is active for this job")
	}
	baseOffsets := map[int]int64{}
	if baseID != "" {
		m, err := s.readManifest(jobID, baseID)
		if err != nil {
			return nil, err
		}
		for _, sh := range m.Shards {
			baseOffsets[sh.Index] = sh.Offset
		}
	}
	w := &Writer{
		store:        s,
		jobID:        jobID,
		checkpointID: checkpointID,
		baseID:       baseID,
		baseOffsets:  baseOffsets,
		shards:       map[int]ShardMeta{},
	}
	s.writers[jobID] = w
	return w, nil
}

// WriteShard writes one shard's data. offset is the logical end position of
// data in this shard; it must not be lower than the base checkpoint's
// offset for the same shard (that would be a regression). May be called
// concurrently for distinct shard indexes.
func (w *Writer) WriteShard(index int, offset int64, data []byte) error {
	w.store.mu.Lock()
	closed := w.closed
	w.store.mu.Unlock()
	if closed {
		return newError("write", KindWriterConflict, w.jobID, w.checkpointID, index,
			"writer is closed")
	}
	if base, ok := w.baseOffsets[index]; ok && offset < base {
		return newError("write", KindRegression, w.jobID, w.checkpointID, index,
			"offset %d is behind base offset %d", offset, base)
	}
	dir := w.store.checkpointDir(w.jobID, w.checkpointID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, fmt.Sprintf(shardFilePattern, index)+shardTmpSuffix)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	w.store.mu.Lock()
	w.shards[index] = ShardMeta{
		Index:  index,
		File:   fmt.Sprintf(shardFilePattern, index),
		Size:   int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]),
		Offset: offset,
	}
	w.store.mu.Unlock()
	return nil
}

// Commit makes the checkpoint visible atomically: shard temp files are
// renamed to final names, then the manifest is renamed into place last.
// Committing the same checkpoint ID with identical content is a no-op
// (idempotent resubmit); different content is a commit conflict.
func (w *Writer) Commit() error {
	s := w.store
	s.mu.Lock()
	if w.closed {
		s.mu.Unlock()
		return newError("commit", KindWriterConflict, w.jobID, w.checkpointID, -1,
			"writer is closed")
	}
	w.closed = true
	delete(s.writers, w.jobID)
	s.mu.Unlock()

	dir := s.checkpointDir(w.jobID, w.checkpointID)
	manifestPath := filepath.Join(dir, manifestFile)

	m := &Manifest{
		FormatVersion:  formatVersion,
		JobID:          w.jobID,
		CheckpointID:   w.checkpointID,
		BaseCheckpoint: w.baseID,
		CreatedAt:      time.Now().UTC(),
	}
	for _, sh := range w.shards {
		m.Shards = append(m.Shards, sh)
	}
	sort.Slice(m.Shards, func(i, j int) bool { return m.Shards[i].Index < m.Shards[j].Index })

	// Idempotent resubmit: an identical manifest is already committed.
	if existing, err := s.readManifest(w.jobID, w.checkpointID); err == nil {
		if existing.equalContent(m) {
			return nil
		}
		return newError("commit", KindCommitConflict, w.jobID, w.checkpointID, -1,
			"checkpoint ID already committed with different content")
	}

	if s.commitHook != nil {
		if err := s.commitHook("before_shard_rename"); err != nil {
			return err
		}
	}
	// Finalize shard files.
	for _, sh := range m.Shards {
		tmp := filepath.Join(dir, sh.File+shardTmpSuffix)
		final := filepath.Join(dir, sh.File)
		if err := os.Rename(tmp, final); err != nil {
			return err
		}
	}
	if s.commitHook != nil {
		if err := s.commitHook("before_manifest_commit"); err != nil {
			return err
		}
	}
	// Write manifest to temp, fsync, atomic rename, fsync dir.
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmpManifest := filepath.Join(dir, manifestTmpFile)
	if err := os.WriteFile(tmpManifest, buf, 0o644); err != nil {
		return err
	}
	if f, err := os.Open(tmpManifest); err == nil {
		f.Sync()
		f.Close()
	}
	if err := os.Rename(tmpManifest, manifestPath); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// Abort discards the pending checkpoint and releases the write lock.
func (w *Writer) Abort() error {
	s := w.store
	s.mu.Lock()
	if w.closed {
		s.mu.Unlock()
		return nil
	}
	w.closed = true
	delete(s.writers, w.jobID)
	s.mu.Unlock()
	return os.RemoveAll(s.checkpointDir(w.jobID, w.checkpointID))
}

// --- Reader ---------------------------------------------------------------

// Reader exposes one committed checkpoint. Shards are verified (existence,
// size, checksum) on read; any mismatch is reported with the shard index
// and a distinct error kind. There is no fallback to older checkpoints.
type Reader struct {
	Manifest *Manifest
	dir      string
}

// Shards returns the committed shard indexes in order.
func (r *Reader) Shards() []int {
	out := make([]int, 0, len(r.Manifest.Shards))
	for _, sh := range r.Manifest.Shards {
		out = append(out, sh.Index)
	}
	return out
}

// ReadShard returns the verified content of one shard.
func (r *Reader) ReadShard(index int) ([]byte, ShardMeta, error) {
	meta, ok := r.Manifest.shard(index)
	if !ok {
		return nil, ShardMeta{}, newError("read", KindNotFound,
			r.Manifest.JobID, r.Manifest.CheckpointID, index, "shard not in manifest")
	}
	data, err := os.ReadFile(filepath.Join(r.dir, meta.File))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, meta, newError("read", KindShardMissing,
				r.Manifest.JobID, r.Manifest.CheckpointID, index, "file %q absent", meta.File)
		}
		return nil, meta, err
	}
	if int64(len(data)) < meta.Size {
		return nil, meta, newError("read", KindShardTruncated,
			r.Manifest.JobID, r.Manifest.CheckpointID, index,
			"size %d < manifest size %d", len(data), meta.Size)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != meta.SHA256 || int64(len(data)) != meta.Size {
		return nil, meta, newError("read", KindShardCorrupted,
			r.Manifest.JobID, r.Manifest.CheckpointID, index, "checksum/size mismatch")
	}
	return data, meta, nil
}

// Open returns a Reader for a committed checkpoint. Directories without a
// committed manifest are rejected as incomplete, never silently served.
func (s *Store) Open(jobID, checkpointID string) (*Reader, error) {
	m, err := s.readManifest(jobID, checkpointID)
	if err != nil {
		return nil, err
	}
	return &Reader{Manifest: m, dir: s.checkpointDir(jobID, checkpointID)}, nil
}

// Latest returns the newest committed checkpoint of the job, or a
// KindNotFound error. Incomplete directories are ignored.
func (s *Store) Latest(jobID string) (*Reader, error) {
	ids, err := s.listCommitted(jobID)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, newError("latest", KindNotFound, jobID, "", -1, "no committed checkpoints")
	}
	return s.Open(jobID, ids[len(ids)-1])
}

// List returns the IDs of all committed checkpoints, oldest first.
func (s *Store) List(jobID string) ([]string, error) {
	return s.listCommitted(jobID)
}

// Cleanup removes incomplete (never committed) checkpoint directories,
// e.g. debris left by interrupted writes.
func (s *Store) Cleanup(jobID string) (removed []string, err error) {
	dir := filepath.Join(s.jobDir(jobID), "checkpoints")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, e.Name(), manifestFile)); os.IsNotExist(err) {
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				return removed, err
			}
			removed = append(removed, e.Name())
		}
	}
	return removed, nil
}

func (s *Store) listCommitted(jobID string) ([]string, error) {
	dir := filepath.Join(s.jobDir(jobID), "checkpoints")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, e.Name(), manifestFile)); err == nil {
			ids = append(ids, e.Name())
		}
	}
	sort.Slice(ids, func(i, j int) bool { return checkpointLess(ids[i], ids[j]) })
	return ids, nil
}

// checkpointLess orders IDs numerically when both are numeric, else
// lexicographically.
func checkpointLess(a, b string) bool {
	na, errA := strconv.ParseInt(strings.TrimPrefix(a, "ckpt-"), 10, 64)
	nb, errB := strconv.ParseInt(strings.TrimPrefix(b, "ckpt-"), 10, 64)
	if errA == nil && errB == nil {
		return na < nb
	}
	return a < b
}

func (s *Store) readManifest(jobID, checkpointID string) (*Manifest, error) {
	path := filepath.Join(s.checkpointDir(jobID, checkpointID), manifestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			kind := KindNotFound
			if _, derr := os.Stat(filepath.Dir(path)); derr == nil {
				kind = KindIncomplete
			}
			return nil, newError("open", kind, jobID, checkpointID, -1, "no committed manifest")
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, newError("open", KindShardCorrupted, jobID, checkpointID, -1,
			"manifest unparseable: %v", err)
	}
	return &m, nil
}

func isKind(err error, k Kind) bool {
	var ce *Error
	if errors.As(err, &ce) {
		return ce.Kind == k
	}
	return false
}
