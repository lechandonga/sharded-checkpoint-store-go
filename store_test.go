package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func commitCheckpoint(t *testing.T, s *Store, job, ckpt string, shards map[int][]byte) {
	t.Helper()
	commitCheckpointFrom(t, s, job, ckpt, "", shards)
}

func commitCheckpointFrom(t *testing.T, s *Store, job, ckpt, base string, shards map[int][]byte) {
	t.Helper()
	var w *Writer
	var err error
	if base != "" || ckpt == mustLatestID(t, s, job) {
		w, err = s.BeginFrom(job, ckpt, base)
	} else {
		w, err = s.Begin(job, ckpt)
	}
	if err != nil {
		t.Fatalf("Begin(%s, %s): %v", job, ckpt, err)
	}
	offset := int64(0)
	for _, idx := range sortedKeys(shards) {
		data := shards[idx]
		offset += int64(len(data))
		if err := w.WriteShard(idx, offset, data); err != nil {
			t.Fatalf("WriteShard(%d): %v", idx, err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit(%s): %v", ckpt, err)
	}
}

func mustLatestID(t *testing.T, s *Store, job string) string {
	t.Helper()
	r, err := s.Latest(job)
	if err != nil {
		return ""
	}
	return r.Manifest.CheckpointID
}

func sortedKeys(m map[int][]byte) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

func TestBasicCommitAndRead(t *testing.T) {
	t.Log("scenario: multi-shard parallel write, commit, read back verified data")
	s := newTestStore(t)
	w, err := s.Begin("job-a", "ckpt-1")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := w.WriteShard(i, int64((i+1)*100), []byte(fmt.Sprintf("shard-data-%d", i))); err != nil {
				t.Errorf("WriteShard(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	r, err := s.Latest("job-a")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if r.Manifest.CheckpointID != "ckpt-1" {
		t.Fatalf("got checkpoint %q", r.Manifest.CheckpointID)
	}
	for _, idx := range r.Shards() {
		data, _, err := r.ReadShard(idx)
		if err != nil {
			t.Fatalf("ReadShard(%d): %v", idx, err)
		}
		if string(data) != fmt.Sprintf("shard-data-%d", idx) {
			t.Fatalf("shard %d content mismatch: %q", idx, data)
		}
	}
	t.Log("case ok: 4 shards written concurrently, committed, verified")
}

func TestInterruptedCommit(t *testing.T) {
	s := newTestStore(t)
	commitCheckpoint(t, s, "job", "ckpt-1", map[int][]byte{0: []byte("v0")})

	stages := []string{"before_shard_rename", "before_manifest_commit"}
	for _, stage := range stages {
		t.Run("fail_at_"+stage, func(t *testing.T) {
			t.Logf("scenario: commit interrupted at %q; checkpoint must be wholly invisible", stage)
			s.commitHook = func(got string) error {
				if got == stage {
					return fmt.Errorf("injected crash at %s", stage)
				}
				return nil
			}
			defer func() { s.commitHook = nil }()

			w, err := s.Begin("job", "ckpt-2")
			if err != nil {
				t.Fatal(err)
			}
			if err := w.WriteShard(0, 200, []byte("v1")); err != nil {
				t.Fatal(err)
			}
			if err := w.Commit(); err == nil {
				t.Fatal("expected injected commit failure")
			}

			latest, err := s.Latest("job")
			if err != nil {
				t.Fatalf("Latest: %v", err)
			}
			if latest.Manifest.CheckpointID != "ckpt-1" {
				t.Fatalf("recovery selected incomplete checkpoint %q", latest.Manifest.CheckpointID)
			}
			if _, err := s.Open("job", "ckpt-2"); !errors.Is(err, ErrIncomplete) {
				t.Fatalf("Open(ckpt-2) = %v, want ErrIncomplete", err)
			}
			removed, err := s.Cleanup("job")
			if err != nil || len(removed) != 1 || removed[0] != "ckpt-2" {
				t.Fatalf("Cleanup = %v, %v", removed, err)
			}
			t.Logf("case ok: interruption at %s left no usable half-committed state", stage)
		})
	}
}

func TestAbort(t *testing.T) {
	t.Log("scenario: writer aborts mid-checkpoint; debris removed, lock released")
	s := newTestStore(t)
	w, _ := s.Begin("job", "ckpt-1")
	if err := w.WriteShard(0, 10, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("job", "ckpt-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open after Abort = %v, want ErrNotFound", err)
	}
	if _, err := s.Begin("job", "ckpt-2"); err != nil {
		t.Fatalf("Begin after Abort: %v", err)
	}
	t.Log("case ok: aborted checkpoint gone, writer lock released")
}

func TestIdempotentResubmit(t *testing.T) {
	t.Log("scenario: same batch committed twice yields identical state; conflicting content errors")
	s := newTestStore(t)
	commitCheckpoint(t, s, "job", "ckpt-1", map[int][]byte{0: []byte("a"), 1: []byte("b")})
	before, _ := s.Latest("job")

	// Resubmit identical batch under the same checkpoint ID.
	commitCheckpoint(t, s, "job", "ckpt-1", map[int][]byte{0: []byte("a"), 1: []byte("b")})
	after, _ := s.Latest("job")
	if !before.Manifest.equalContent(after.Manifest) {
		t.Fatal("resubmit changed committed state")
	}
	ids, _ := s.List("job")
	if len(ids) != 1 {
		t.Fatalf("expected 1 checkpoint, got %v", ids)
	}
	t.Log("case ok: duplicate commit is a no-op")

	// Same ID, different content -> conflict.
	w, err := s.Begin("job", "ckpt-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteShard(0, 300, []byte("different")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("Commit = %v, want ErrCommitConflict", err)
	}
	t.Log("case ok: conflicting recommit rejected with ErrCommitConflict")
}

func TestRegression(t *testing.T) {
	t.Log("scenario: offset or baseline regression is rejected with a distinct error")
	s := newTestStore(t)
	commitCheckpoint(t, s, "job", "ckpt-1", map[int][]byte{0: []byte("aaaa")})
	commitCheckpoint(t, s, "job", "ckpt-2", map[int][]byte{0: []byte("bbbb")})

	// Offset regression within a shard.
	w, err := s.Begin("job", "ckpt-3")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteShard(0, 1, []byte("z")); !errors.Is(err, ErrRegression) {
		t.Fatalf("WriteShard behind base = %v, want ErrRegression", err)
	}
	w.Abort()
	t.Log("case ok: shard offset regression rejected")

	// Baseline regression: resume from ckpt-1 while ckpt-2 is latest.
	if _, err := s.BeginFrom("job", "ckpt-4", "ckpt-1"); !errors.Is(err, ErrRegression) {
		t.Fatalf("BeginFrom stale base = %v, want ErrRegression", err)
	}
	t.Log("case ok: stale baseline rejected")
}

func TestIncrementalResume(t *testing.T) {
	t.Log("scenario: incremental checkpoints chain on bases; recovery picks latest committed")
	s := newTestStore(t)
	commitCheckpoint(t, s, "job", "ckpt-1", map[int][]byte{0: []byte("part-1")})
	commitCheckpoint(t, s, "job", "ckpt-2", map[int][]byte{0: []byte("part-2"), 1: []byte("extra")})

	r, err := s.Latest("job")
	if err != nil {
		t.Fatal(err)
	}
	if r.Manifest.BaseCheckpoint != "ckpt-1" {
		t.Fatalf("base = %q, want ckpt-1", r.Manifest.BaseCheckpoint)
	}
	_, meta, err := r.ReadShard(0)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Offset <= 0 {
		t.Fatalf("resume cursor not recorded: offset=%d", meta.Offset)
	}

	// Simulate process restart: a fresh Store over the same dir recovers.
	s2, err := NewStore(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s2.Latest("job")
	if err != nil || r2.Manifest.CheckpointID != "ckpt-2" {
		t.Fatalf("recovery after restart: %v, %v", r2, err)
	}
	t.Log("case ok: incremental chain + restart recovery")
}

func TestConcurrentAccess(t *testing.T) {
	t.Log("scenario: concurrent writers and readers never corrupt or observe partial state")
	s := newTestStore(t)
	commitCheckpoint(t, s, "job", "ckpt-1", map[int][]byte{0: []byte("committed")})

	// Two writers on the same job: exactly one wins the lock.
	w1, err := s.Begin("job", "ckpt-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin("job", "ckpt-3"); !errors.Is(err, ErrWriterConflict) {
		t.Fatalf("second Begin = %v, want ErrWriterConflict", err)
	}
	t.Log("case ok: concurrent writer rejected with ErrWriterConflict")

	// Readers during an in-flight write only see the committed checkpoint.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Latest("job")
			if err != nil {
				t.Errorf("Latest during write: %v", err)
				return
			}
			if r.Manifest.CheckpointID != "ckpt-1" {
				t.Errorf("reader observed uncommitted %q", r.Manifest.CheckpointID)
			}
			if _, _, err := r.ReadShard(0); err != nil {
				t.Errorf("ReadShard during write: %v", err)
			}
		}()
	}
	for i := 0; i < 8; i++ {
		if err := w1.WriteShard(i, int64((i+1)*10), []byte(fmt.Sprintf("d%d", i))); err != nil {
			t.Errorf("WriteShard(%d): %v", i, err)
		}
	}
	wg.Wait()
	if err := w1.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if r, _ := s.Latest("job"); r.Manifest.CheckpointID != "ckpt-2" {
		t.Fatal("committed checkpoint not visible after commit")
	}
	t.Log("case ok: readers saw only committed state throughout concurrent write")
}

func TestCorruption(t *testing.T) {
	s := newTestStore(t)
	commitCheckpoint(t, s, "job", "ckpt-1", map[int][]byte{0: []byte("hello"), 1: []byte("world")})
	dir := s.checkpointDir("job", "ckpt-1")
	shard0 := filepath.Join(dir, "shard-000000")
	shard1 := filepath.Join(dir, "shard-000001")

	t.Run("missing_shard", func(t *testing.T) {
		t.Log("scenario: shard file deleted -> ErrShardMissing naming the shard")
		os.Rename(shard0, shard0+".bak")
		defer os.Rename(shard0+".bak", shard0)
		r, _ := s.Open("job", "ckpt-1")
		var ce *Error
		if _, _, err := r.ReadShard(0); !errors.Is(err, ErrShardMissing) || !errors.As(err, &ce) || ce.Shard != 0 {
			t.Fatalf("got %v, want ErrShardMissing for shard 0", err)
		}
		t.Log("case ok: missing shard localized")
	})

	t.Run("truncated_shard", func(t *testing.T) {
		t.Log("scenario: shard file truncated -> ErrShardTruncated naming the shard")
		orig, _ := os.ReadFile(shard0)
		os.WriteFile(shard0, orig[:len(orig)-2], 0o644)
		defer os.WriteFile(shard0, orig, 0o644)
		r, _ := s.Open("job", "ckpt-1")
		var ce *Error
		if _, _, err := r.ReadShard(0); !errors.Is(err, ErrShardTruncated) || !errors.As(err, &ce) || ce.Shard != 0 {
			t.Fatalf("got %v, want ErrShardTruncated for shard 0", err)
		}
		t.Log("case ok: truncation localized")
	})

	t.Run("modified_shard", func(t *testing.T) {
		t.Log("scenario: shard content modified -> ErrShardCorrupted naming the shard")
		orig, _ := os.ReadFile(shard1)
		mut := append([]byte(nil), orig...)
		mut[0] ^= 0xff
		os.WriteFile(shard1, mut, 0o644)
		defer os.WriteFile(shard1, orig, 0o644)
		r, _ := s.Open("job", "ckpt-1")
		var ce *Error
		if _, _, err := r.ReadShard(1); !errors.Is(err, ErrShardCorrupted) || !errors.As(err, &ce) || ce.Shard != 1 {
			t.Fatalf("got %v, want ErrShardCorrupted for shard 1", err)
		}
		t.Log("case ok: modification localized")
	})

	t.Run("deleted_manifest", func(t *testing.T) {
		t.Log("scenario: manifest removed -> checkpoint treated as incomplete, never served")
		manifest := filepath.Join(dir, manifestFile)
		os.Rename(manifest, manifest+".bak")
		defer os.Rename(manifest+".bak", manifest)
		if _, err := s.Open("job", "ckpt-1"); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Open = %v, want ErrIncomplete", err)
		}
		if _, err := s.Latest("job"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Latest = %v, want ErrNotFound (no fallback to partial data)", err)
		}
		t.Log("case ok: manifest-less checkpoint invisible")
	})
}
