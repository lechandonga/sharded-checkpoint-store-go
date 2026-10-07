package checkpoint

import (
	"errors"
	"fmt"
)

// Kind classifies store errors so callers can react programmatically.
type Kind string

const (
	// KindNotFound: the requested job or checkpoint does not exist.
	KindNotFound Kind = "not_found"
	// KindIncomplete: a checkpoint directory exists but was never committed
	// (interrupted/cancelled write). It is never served as readable data.
	KindIncomplete Kind = "incomplete"
	// KindShardMissing: a committed checkpoint references a shard file that
	// is absent on disk.
	KindShardMissing Kind = "shard_missing"
	// KindShardTruncated: a shard file is shorter than the size recorded in
	// the manifest.
	KindShardTruncated Kind = "shard_truncated"
	// KindShardCorrupted: a shard file's checksum does not match the
	// manifest (content modified or longer than recorded).
	KindShardCorrupted Kind = "shard_corrupted"
	// KindRegression: the writer attempted to resume from a position or
	// baseline that is behind the committed state.
	KindRegression Kind = "regression"
	// KindCommitConflict: a checkpoint ID was committed with different
	// content than the pending commit.
	KindCommitConflict Kind = "commit_conflict"
	// KindWriterConflict: another writer holds the job's write lock.
	KindWriterConflict Kind = "writer_conflict"
)

// Sentinel values for errors.Is checks.
var (
	ErrNotFound       = errors.New("checkpoint: not found")
	ErrIncomplete     = errors.New("checkpoint: incomplete")
	ErrShardMissing   = errors.New("checkpoint: shard missing")
	ErrShardTruncated = errors.New("checkpoint: shard truncated")
	ErrShardCorrupted = errors.New("checkpoint: shard corrupted")
	ErrRegression     = errors.New("checkpoint: regression")
	ErrCommitConflict = errors.New("checkpoint: commit conflict")
	ErrWriterConflict = errors.New("checkpoint: writer conflict")
)

var kindSentinel = map[Kind]error{
	KindNotFound:       ErrNotFound,
	KindIncomplete:     ErrIncomplete,
	KindShardMissing:   ErrShardMissing,
	KindShardTruncated: ErrShardTruncated,
	KindShardCorrupted: ErrShardCorrupted,
	KindRegression:     ErrRegression,
	KindCommitConflict: ErrCommitConflict,
	KindWriterConflict: ErrWriterConflict,
}

// Error is the single error type returned by the store. It always carries
// a Kind and, where applicable, the job, checkpoint and shard involved.
type Error struct {
	Op           string
	Kind         Kind
	JobID        string
	CheckpointID string
	Shard        int // -1 when not shard-specific
	Detail       string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("checkpoint: %s: %s (job=%q checkpoint=%q", e.Op, e.Kind, e.JobID, e.CheckpointID)
	if e.Shard >= 0 {
		s += fmt.Sprintf(" shard=%d", e.Shard)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s + ")"
}

// Is supports errors.Is against the Kind sentinels above.
func (e *Error) Is(target error) bool {
	return kindSentinel[e.Kind] == target
}

func newError(op string, kind Kind, jobID, checkpointID string, shard int, format string, args ...any) *Error {
	return &Error{
		Op:           op,
		Kind:         kind,
		JobID:        jobID,
		CheckpointID: checkpointID,
		Shard:        shard,
		Detail:       fmt.Sprintf(format, args...),
	}
}
