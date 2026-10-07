package checkpoint

import "time"

// formatVersion is bumped when the on-disk manifest schema changes.
const formatVersion = 1

// ShardMeta describes one committed shard file.
type ShardMeta struct {
	Index  int    `json:"index"`
	File   string `json:"file"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	// Offset is the logical stream position this shard's data ends at.
	// It is the resume cursor for incremental writes.
	Offset int64 `json:"offset"`
}

// Manifest is the commit record of a checkpoint. Its presence in a
// checkpoint directory is what makes the checkpoint visible to readers:
// it is written last and renamed into place atomically.
type Manifest struct {
	FormatVersion  int         `json:"format_version"`
	JobID          string      `json:"job_id"`
	CheckpointID   string      `json:"checkpoint_id"`
	BaseCheckpoint string      `json:"base_checkpoint,omitempty"`
	Shards         []ShardMeta `json:"shards"`
	CreatedAt      time.Time   `json:"created_at"`
}

func (m *Manifest) shard(index int) (ShardMeta, bool) {
	for _, s := range m.Shards {
		if s.Index == index {
			return s, true
		}
	}
	return ShardMeta{}, false
}

// equalContent reports whether two manifests describe the same committed
// data. Used to make repeated commits of the same batch idempotent.
func (m *Manifest) equalContent(o *Manifest) bool {
	if m.JobID != o.JobID || m.CheckpointID != o.CheckpointID ||
		m.BaseCheckpoint != o.BaseCheckpoint || len(m.Shards) != len(o.Shards) {
		return false
	}
	for i := range m.Shards {
		a, b := m.Shards[i], o.Shards[i]
		if a.Index != b.Index || a.Size != b.Size || a.SHA256 != b.SHA256 || a.Offset != b.Offset {
			return false
		}
	}
	return true
}
