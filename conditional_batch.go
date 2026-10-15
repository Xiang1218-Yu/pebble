// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// Optimistic conditional batches (also called "conditional batches") pair an
// immutable set of write operations (Set, Merge, Delete, DeleteRange and the
// RangeKey* operations) with a read set collected by the caller against a
// snapshot. On Commit, the commit pipeline atomically re-validates the read
// set: if any concurrent mutation has modified a key or key range in the read
// set since the snapshot was taken, the commit is rejected with a
// ConditionalConflictError before any WAL record, memtable entry, blob
// reference or version metadata is produced. Otherwise all writes are appended
// to a single WAL record and become visible at a single commit sequence
// number.
//
// The feature is opt-in via
// Options.ExperimentalOptimisticConditionalBatches and is disabled by default.
// When disabled, no code path changes behavior: conditional batches cannot be
// constructed, and the ordinary Batch, DB.Apply, Get, NewIter, flush, ingest
// and compaction paths are untouched.

// ErrConditionalDisabled is returned by DB.NewConditionalBatch when the
// optimistic conditional batch feature is not enabled in the database options.
var ErrConditionalDisabled = errors.New(
	"pebble: optimistic conditional batches are disabled; set Options.ExperimentalOptimisticConditionalBatches")

// ErrConditionalReadSetEmpty is returned when a conditional batch is committed
// (or frozen) without any read condition. A conditional commit must be
// conditioned on at least one observed key, key range or range key.
var ErrConditionalReadSetEmpty = errors.New(
	"pebble: conditional batch has an empty read set")

// ErrConditionalInvalidRange is returned when a tracked key range is malformed
// (for example when the end key is not greater than the start key).
var ErrConditionalInvalidRange = errors.New(
	"pebble: invalid key range in conditional batch read set")

// ErrConditionalBatchAlreadyCommitted is returned when Commit is called again
// on a conditional batch that has already committed successfully. The original
// writes are not applied a second time.
var ErrConditionalBatchAlreadyCommitted = errors.New(
	"pebble: conditional batch already committed")

// ErrConditionalBatchAborted is returned when Commit (or any read-tracking or
// write operation) is attempted after a conditional batch has aborted. An
// aborted conditional batch must not be reused: neither its snapshot nor its
// buffers are valid for a retry. The caller must build a new conditional batch
// from a fresh snapshot instead.
var ErrConditionalBatchAborted = errors.New(
	"pebble: conditional batch has been aborted; construct a new conditional batch to retry")

// ErrConditionalBatchFrozen is returned by the read-tracking and write methods
// of a conditional batch after it has been frozen for commit.
var ErrConditionalBatchFrozen = errors.New(
	"pebble: conditional batch is frozen and cannot be modified")

// ConflictReason identifies the kind of mutation that invalidated a
// conditional batch's read set.
type ConflictReason uint8

const (
	// ConflictUnknown is the zero value and never reported.
	ConflictUnknown ConflictReason = iota
	// ConflictPointWrite indicates that a point key in the read set was
	// concurrently set, overwritten or deleted.
	ConflictPointWrite
	// ConflictRangeDelete indicates that a concurrent range deletion
	// (DeleteRange) tombstone covers part of the read set.
	ConflictRangeDelete
	// ConflictRangeKey indicates that a concurrent range key set, unset or
	// delete covers part of the read set, potentially masking point reads.
	ConflictRangeKey
	// ConflictMergeResult indicates that a concurrent Merge operation changed
	// the merge result of a point key in the read set.
	ConflictMergeResult
	// ConflictExternalIngest indicates that an externally ingested sstable
	// (Ingest/IngestAndExcise) introduced new versions inside the read set.
	ConflictExternalIngest
)

// String implements fmt.Stringer.
func (r ConflictReason) String() string {
	switch r {
	case ConflictPointWrite:
		return "point key modified"
	case ConflictRangeDelete:
		return "range deletion"
	case ConflictRangeKey:
		return "range key modification"
	case ConflictMergeResult:
		return "merge result changed"
	case ConflictExternalIngest:
		return "external file ingest"
	default:
		return "unknown conflict"
	}
}

// ErrConditionalConflict is the sentinel error that wraps every
// *ConditionalConflictError. Use errors.Is(err, ErrConditionalConflict) to
// detect an optimistic concurrency conflict.
var ErrConditionalConflict = errors.New("pebble: conditional batch conflict")

// ConditionalConflictError describes a single read-set condition that was
// invalidated by a concurrent mutation. A conflict is returned before the
// conditional batch's writes touch the WAL, memtables, blob files or the
// version set, so a conflicted commit leaves no state behind and is never
// durable.
type ConditionalConflictError struct {
	// Reason classifies the conflicting mutation.
	Reason ConflictReason
	// SnapshotSeqNum is the read-set snapshot sequence number.
	SnapshotSeqNum SeqNum
	// CommitSeqNum is the next commit sequence number at which the conflict
	// was observed. The conflicting mutation has a sequence number in
	// (SnapshotSeqNum, CommitSeqNum).
	CommitSeqNum SeqNum
	// ConflictSeqNum is the sequence number of the conflicting operation, when
	// known.
	ConflictSeqNum SeqNum
	// Point is true when the violated condition tracked a single point key; in
	// that case Start is the tracked key and End is empty.
	Point bool
	// Start and End describe the violated read condition's key range
	// [Start, End). End is empty for point conditions.
	Start, End []byte
}

// Error implements error.
func (e *ConditionalConflictError) Error() string {
	if e.Point {
		return fmt.Sprintf("pebble: conditional batch conflict at seqnum %d: %s for key %q (snapshot seqnum %d, conflicting seqnum %d)",
			e.CommitSeqNum, e.Reason, e.Start, e.SnapshotSeqNum, e.ConflictSeqNum)
	}
	return fmt.Sprintf("pebble: conditional batch conflict at seqnum %d: %s over range [%q, %q) (snapshot seqnum %d, conflicting seqnum %d)",
		e.CommitSeqNum, e.Reason, e.Start, e.End, e.SnapshotSeqNum, e.ConflictSeqNum)
}

// Unwrap allows errors.Is(err, ErrConditionalConflict).
func (e *ConditionalConflictError) Unwrap() error { return ErrConditionalConflict }

// ConditionalCommitStage identifies the stage of a failed conditional commit.
type ConditionalCommitStage uint8

const (
	// CommitStageUnknown is the zero value.
	CommitStageUnknown ConditionalCommitStage = iota
	// CommitStageCanceled indicates the context was canceled before the WAL
	// record was written. Nothing was committed or persisted.
	CommitStageCanceled
	// CommitStageWALWrite indicates a failure while writing the WAL record.
	CommitStageWALWrite
	// CommitStageApply indicates a failure while applying the writes to the
	// memtable after the WAL record had already been written.
	CommitStageApply
	// CommitStageSync indicates the WAL record was written and made visible
	// but the requested fsync did not succeed.
	CommitStageSync
)

// String implements fmt.Stringer.
func (s ConditionalCommitStage) String() string {
	switch s {
	case CommitStageCanceled:
		return "canceled before WAL write"
	case CommitStageWALWrite:
		return "WAL write"
	case CommitStageApply:
		return "memtable apply"
	case CommitStageSync:
		return "WAL sync"
	default:
		return "unknown"
	}
}

// ConditionalCommitError is returned by ConditionalBatch.Commit when the
// outcome of a (non-conflict) commit failure is relevant to the caller, in
// particular whether the writes may already have been persisted. The zero
// value of the flags means "known not committed/persisted"; callers must not
// blindly retry when MaybePersisted or Committed is true without first
// re-reading the affected keys.
type ConditionalCommitError struct {
	// Stage is the commit stage at which the failure occurred.
	Stage ConditionalCommitStage
	// Committed is true if the batch's mutations have definitely become
	// visible (they were applied and their sequence number was published).
	Committed bool
	// Durable is true if a synced WAL record confirms the batch is durable
	// (only possible when WriteOptions.Sync was requested and succeeded).
	Durable bool
	// MaybePersisted is true when a WAL record was written but durability or
	// visibility could not be confirmed. A crash may have committed the
	// batch, so it is not safe to blindly retry.
	MaybePersisted bool
	// Err is the underlying error.
	Err error
}

// Error implements error.
func (e *ConditionalCommitError) Error() string {
	switch {
	case e.Durable:
		return fmt.Sprintf("pebble: conditional commit %s: %v (committed and durable)", e.Stage, e.Err)
	case e.Committed:
		return fmt.Sprintf("pebble: conditional commit %s: %v (committed and visible, durability unconfirmed)", e.Stage, e.Err)
	case e.MaybePersisted:
		return fmt.Sprintf("pebble: conditional commit %s: %v (commit outcome uncertain; the batch may already be persisted)", e.Stage, e.Err)
	default:
		return fmt.Sprintf("pebble: conditional commit %s: %v (not committed, not persisted)", e.Stage, e.Err)
	}
}

// Unwrap returns the underlying error.
func (e *ConditionalCommitError) Unwrap() error { return e.Err }

func newConditionalCommitError(
	stage ConditionalCommitStage, err error,
) *ConditionalCommitError {
	e := &ConditionalCommitError{Stage: stage, Err: err}
	switch stage {
	case CommitStageWALWrite:
		// The record may have made it to the OS or stable storage even though
		// the write reported an error.
		e.MaybePersisted = true
	case CommitStageApply:
		// The WAL record was written successfully.
		e.MaybePersisted = true
	case CommitStageSync:
		// Visibility was published before the fsync result is observed, so the
		// batch is committed but not known to be durable.
		e.Committed = true
		e.MaybePersisted = true
	}
	return e
}

// ConditionalReadSetInfo summarizes the metadata recorded with a conditional
// batch's read set.
type ConditionalReadSetInfo struct {
	// Comparer is the name of the comparer in use when the read set was
	// collected.
	Comparer string
	// Merger is the name of the merger in use when the read set was collected.
	Merger string
	// FormatMajorVersion is the format major version observed when the read set
	// was collected.
	FormatMajorVersion FormatMajorVersion
	// SnapshotSeqNum is the sequence number of the snapshot the reads were
	// performed against.
	SnapshotSeqNum SeqNum
	// NumPointKeys is the number of tracked point keys.
	NumPointKeys int
	// NumKeyRanges is the number of tracked point key ranges.
	NumKeyRanges int
	// NumRangeKeys is the number of tracked range-key ranges.
	NumRangeKeys int
}

type condStatus uint8

const (
	condOpen condStatus = iota
	condFrozen
	condCommitted
	condAborted
	condClosed
)

type condKind uint8

const (
	condPoint condKind = iota + 1
	condPointRange
	condRangeKey
)

// readCondition is a single entry of a conditional batch's read set.
type readCondition struct {
	kind condKind
	// start/end delimit [start, end). end is nil for a point condition. Either
	// endpoint may be nil for internally-tracked open-ended ranges (gaps
	// observed through an iterator); user-tracked ranges always have both
	// bounds set.
	start, end []byte

	// Point version digest captured at snapshot time: the newest raw internal
	// record (kind and sequence number, including tombstones) visible at the
	// snapshot, or found=false if no record existed.
	found bool
	kind_ InternalKeyKind
	seq   SeqNum

	// Range digest: approximate number of point versions observed in the
	// range at snapshot time and the largest point sequence number observed.
	pointCount uint64
	pointMax   SeqNum
	// Range-key range digest: number of overlapping fragmented spans and the
	// largest range-key sequence number at snapshot time.
	spanCount uint64
	spanMax   SeqNum
}

// optimisticReadSet is the immutable (after freezing) collection of read
// conditions.
type optimisticReadSet struct {
	conds         []readCondition
	snapSeq       SeqNum
	comparerName  string
	mergerName    string
	formatVersion FormatMajorVersion
	frozen        bool
}

// copyKey appends a private copy of key so later reuse of caller buffers cannot
// mutate the read set ("failed retries cannot reuse user buffers").
func cloneKey(k []byte) []byte {
	if k == nil {
		return nil
	}
	return append(make([]byte, 0, len(k)), k...)
}

// addPoint records a point key condition together with its version digest.
func (rs *optimisticReadSet) addPoint(c *readCondition) {
	c.kind = condPoint
	c.end = nil
	c.start = cloneKey(c.start)
	rs.conds = append(rs.conds, *c)
}

// freeze canonicalizes the conditions: sorting and de-duplicating them. The
// read set must not be modified afterwards.
func (rs *optimisticReadSet) freeze(cmp base.Compare) error {
	if rs.frozen {
		return nil
	}
	if len(rs.conds) == 0 {
		return ErrConditionalReadSetEmpty
	}
	sort.SliceStable(rs.conds, func(i, j int) bool {
		a, b := &rs.conds[i], &rs.conds[j]
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if c := cmp(a.start, b.start); c != 0 {
			return c < 0
		}
		return cmp(a.end, b.end) < 0
	})
	// Deduplicate identical conditions (cheap O(n); read sets are small).
	out := rs.conds[:0]
	for i := range rs.conds {
		c := &rs.conds[i]
		if len(out) > 0 {
			last := &out[len(out)-1]
			if last.kind == c.kind && cmp(last.start, c.start) == 0 && cmp(last.end, c.end) == 0 {
				continue
			}
		}
		out = append(out, *c)
	}
	rs.conds = out
	rs.frozen = true
	return nil
}

// ingestConflictRecord remembers the sequence number and key bounds of an
// external ingest so conditional commits can distinguish ingest conflicts.
type ingestConflictRecord struct {
	seq    SeqNum
	bounds []base.UserKeyBounds
}

// recordConditionalIngestLocked appends an ingest record. d.mu must be held.
func (d *DB) recordConditionalIngestLocked(seq SeqNum, items []bounded) {
	rec := ingestConflictRecord{seq: seq}
	for _, b := range items {
		// Clone the bounds so the record does not pin ingested file
		// metadata after the files become obsolete.
		ub := b.UserKeyBounds()
		rec.bounds = append(rec.bounds, base.UserKeyBounds{
			Start: cloneKey(ub.Start),
			End: base.UserKeyBoundary{
				Key:  cloneKey(ub.End.Key),
				Kind: ub.End.Kind,
			},
		})
	}
	d.mu.conditional.ingests = append(d.mu.conditional.ingests, rec)

	// Prune entries older than the oldest open snapshot. Every conditional
	// batch pins a snapshot, so an entry older than the oldest snapshot is
	// irrelevant to all live and future read sets. With no open snapshots,
	// earliest() returns MaxUint64 and all records are prunable.
	earliest := d.mu.snapshots.earliest()
	ingests := d.mu.conditional.ingests
	drop := 0
	for drop < len(ingests) && ingests[drop].seq < earliest {
		drop++
	}
	if drop > 0 {
		copy(ingests, ingests[drop:])
		for i := len(ingests) - drop; i < len(ingests); i++ {
			ingests[i] = ingestConflictRecord{}
		}
		d.mu.conditional.ingests = ingests[:len(ingests)-drop]
	}
}

// ingestOverlapsLocked reports an ingest sequence number newer than snapSeq
// whose bounds overlap the condition: the point start when point is true, or
// [start, end) where either bound may be nil for an open-ended range.
// d.mu must be held.
func (d *DB) ingestOverlapsLocked(
	snapSeq SeqNum, start, end []byte, point bool,
) (SeqNum, bool) {
	cmp := d.cmp
	for i := range d.mu.conditional.ingests {
		rec := &d.mu.conditional.ingests[i]
		if rec.seq < snapSeq {
			continue
		}
		for j := range rec.bounds {
			ub := &rec.bounds[j]
			if userBoundsOverlap(cmp, start, end, point, ub) {
				return rec.seq, true
			}
		}
	}
	return 0, false
}

// userBoundsOverlap reports whether the condition overlaps ub. When point is
// true it tests whether the single key lo is within ub; otherwise lo/hi are
// range bounds and either may be nil (-infinity / +infinity).
func userBoundsOverlap(cmp base.Compare, lo, hi []byte, point bool, ub *base.UserKeyBounds) bool {
	if point {
		return cmp(ub.Start, lo) <= 0 && ub.End.IsUpperBoundFor(cmp, lo)
	}
	switch {
	case lo == nil && hi == nil:
		return true
	case hi == nil:
		// [lo, +inf): overlap iff ub ends after lo.
		return ub.End.IsUpperBoundFor(cmp, lo)
	case lo == nil:
		// (-inf, hi): overlap iff ub starts before hi.
		return cmp(ub.Start, hi) < 0
	default:
		b := base.UserKeyBoundsEndExclusive(lo, hi)
		return ub.Overlaps(cmp, b)
	}
}

// A ConditionalBatch is an optimistic transaction-like batch. It pairs an
// ordinary write batch with a read set collected against a Snapshot (or
// against an implicit snapshot taken at construction time). Reads are
// registered through the controlled read interface (TrackPoint, TrackRange,
// TrackRangeKey, Get and NewIter); writes are the usual Set, Merge, Delete,
// DeleteRange and RangeKey* operations.
//
// Once Commit is called the conditional batch becomes immutable. Commit
// atomically validates the read set inside the commit pipeline and, if the
// read set is still satisfied, applies all writes with a single commit
// sequence number and a single WAL record. On conflict nothing is written.
//
// A ConditionalBatch, like a Batch, is not safe for concurrent use. The
// caller must call Close when finished with it.
type ConditionalBatch struct {
	db      *DB
	batch   *Batch
	snap    *Snapshot
	owned   bool
	status  condStatus
	readSet *optimisticReadSet
	// commitSeq is the sequence number assigned after a successful commit.
	commitSeq SeqNum
}

// NewConditionalBatch begins a new optimistic conditional batch. If snap is
// nil, the database takes a fresh snapshot internally (and releases it on
// Close). If snap is non-nil, the caller must keep it open until Commit or
// Close returns. The feature must be enabled through
// Options.ExperimentalOptimisticConditionalBatches.
//
// All controlled reads (TrackPoint, TrackRange, TrackRangeKey, Get, NewIter)
// observe the state at the snapshot sequence number.
func (d *DB) NewConditionalBatch(snap *Snapshot) (*ConditionalBatch, error) {
	if !d.opts.ExperimentalOptimisticConditionalBatches {
		return nil, ErrConditionalDisabled
	}
	if v := d.closed.Load(); v != nil {
		return nil, v.(error)
	}
	owned := false
	if snap == nil {
		snap = d.NewSnapshot()
		owned = true
	} else if snap.db == nil {
		return nil, errors.WithStack(ErrClosed)
	} else if snap.db != d {
		return nil, errors.AssertionFailedf("pebble: snapshot belongs to a different database")
	}
	mergerName := ""
	if d.opts.Merger != nil {
		mergerName = d.opts.Merger.Name
	}
	cb := &ConditionalBatch{
		db:    d,
		batch: d.NewBatch(),
		snap:  snap,
		owned: owned,
		readSet: &optimisticReadSet{
			snapSeq:       snap.seqNum,
			comparerName:  d.opts.Comparer.Name,
			mergerName:    mergerName,
			formatVersion: d.FormatMajorVersion(),
		},
	}
	return cb, nil
}

// SnapshotSeqNum returns the sequence number the conditional batch's reads are
// performed against.
func (cb *ConditionalBatch) SnapshotSeqNum() SeqNum { return cb.readSet.snapSeq }

// CommitSeqNum returns the sequence number at which the conditional batch's
// writes committed, or 0 before a successful commit.
func (cb *ConditionalBatch) CommitSeqNum() SeqNum { return cb.commitSeq }

// ReadSetInfo returns the metadata recorded with the read set.
func (cb *ConditionalBatch) ReadSetInfo() ConditionalReadSetInfo {
	info := ConditionalReadSetInfo{
		Comparer:           cb.readSet.comparerName,
		Merger:             cb.readSet.mergerName,
		FormatMajorVersion: cb.readSet.formatVersion,
		SnapshotSeqNum:     cb.readSet.snapSeq,
	}
	for i := range cb.readSet.conds {
		switch cb.readSet.conds[i].kind {
		case condPoint:
			info.NumPointKeys++
		case condPointRange:
			info.NumKeyRanges++
		case condRangeKey:
			info.NumRangeKeys++
		}
	}
	return info
}

func (cb *ConditionalBatch) checkMutable() error {
	switch cb.status {
	case condOpen:
		return nil
	case condCommitted:
		return ErrConditionalBatchAlreadyCommitted
	case condAborted, condClosed:
		return ErrConditionalBatchAborted
	default:
		return ErrConditionalBatchFrozen
	}
}

// Close releases the conditional batch's resources, including any internally
// held snapshot and the underlying write batch. It is safe to call after a
// successful commit, a conflict or another failure. Close is idempotent.
func (cb *ConditionalBatch) Close() error {
	if cb.status == condClosed {
		return nil
	}
	cb.status = condClosed
	var err error
	if cb.batch != nil {
		err = cb.batch.Close()
	}
	if cb.owned && cb.snap != nil && cb.snap.db != nil {
		err = errors.CombineErrors(err, cb.snap.Close())
	}
	return err
}

// ConditionalReadConditionKind labels a read-set condition.
type ConditionalReadConditionKind uint8

// The kinds of read-set conditions.
const (
	ConditionPoint ConditionalReadConditionKind = iota + 1
	ConditionKeyRange
	ConditionRangeKey
)

// ConditionalReadCondition describes a single condition recorded in a
// conditional batch's read set.
type ConditionalReadCondition struct {
	// Kind is the condition kind.
	Kind ConditionalReadConditionKind
	// Start and End delimit [Start, End); End is empty for point conditions.
	Start, End []byte
	// Observed reports whether a point version existed at snapshot time. Only
	// populated for point conditions.
	Observed bool
	// ObservedKind is the internal kind of the newest observed point version.
	ObservedKind InternalKeyKind
	// ObservedSeqNum is the sequence number of the newest observed point
	// version.
	ObservedSeqNum SeqNum
}

// ReadSet returns a copy of the conditions currently in the read set,
// including the recorded point version summaries. It must not be called
// concurrently with read-tracking operations.
func (cb *ConditionalBatch) ReadSet() []ConditionalReadCondition {
	out := make([]ConditionalReadCondition, len(cb.readSet.conds))
	for i := range cb.readSet.conds {
		c := &cb.readSet.conds[i]
		out[i] = ConditionalReadCondition{
			Start:          cloneKey(c.start),
			End:            cloneKey(c.end),
			Observed:       c.found,
			ObservedKind:   c.kind_,
			ObservedSeqNum: c.seq,
		}
		switch c.kind {
		case condPoint:
			out[i].Kind = ConditionPoint
		case condPointRange:
			out[i].Kind = ConditionKeyRange
		case condRangeKey:
			out[i].Kind = ConditionRangeKey
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Write operations (delegated to the embedded batch).
// ---------------------------------------------------------------------------

func (cb *ConditionalBatch) write(fn func() error) error {
	if err := cb.checkMutable(); err != nil {
		return err
	}
	return fn()
}

// Set queues a Set operation. See Batch.Set.
func (cb *ConditionalBatch) Set(key, value []byte) error {
	return cb.write(func() error { return cb.batch.Set(key, value, nil) })
}

// Merge queues a Merge operation. See Batch.Merge.
func (cb *ConditionalBatch) Merge(key, value []byte) error {
	return cb.write(func() error { return cb.batch.Merge(key, value, nil) })
}

// Delete queues a Delete operation. See Batch.Delete.
func (cb *ConditionalBatch) Delete(key []byte) error {
	return cb.write(func() error { return cb.batch.Delete(key, nil) })
}

// SingleDelete queues a SingleDelete operation. See Batch.SingleDelete.
func (cb *ConditionalBatch) SingleDelete(key []byte) error {
	return cb.write(func() error { return cb.batch.SingleDelete(key, nil) })
}

// DeleteSized queues a DeleteSized operation. See Batch.DeleteSized.
func (cb *ConditionalBatch) DeleteSized(key []byte, deletedValueSize uint32) error {
	return cb.write(func() error { return cb.batch.DeleteSized(key, deletedValueSize, nil) })
}

// DeleteRange queues a DeleteRange operation. See Batch.DeleteRange.
func (cb *ConditionalBatch) DeleteRange(start, end []byte) error {
	return cb.write(func() error { return cb.batch.DeleteRange(start, end, nil) })
}

// RangeKeySet queues a RangeKeySet operation. See Batch.RangeKeySet.
func (cb *ConditionalBatch) RangeKeySet(start, end, suffix, value []byte) error {
	return cb.write(func() error { return cb.batch.RangeKeySet(start, end, suffix, value, nil) })
}

// RangeKeyUnset queues a RangeKeyUnset operation. See Batch.RangeKeyUnset.
func (cb *ConditionalBatch) RangeKeyUnset(start, end, suffix []byte) error {
	return cb.write(func() error { return cb.batch.RangeKeyUnset(start, end, suffix, nil) })
}

// RangeKeyDelete queues a RangeKeyDelete operation. See Batch.RangeKeyDelete.
func (cb *ConditionalBatch) RangeKeyDelete(start, end []byte) error {
	return cb.write(func() error { return cb.batch.RangeKeyDelete(start, end, nil) })
}

// LogData queues a LogData operation. See Batch.LogData.
func (cb *ConditionalBatch) LogData(data []byte) error {
	return cb.write(func() error { return cb.batch.LogData(data, nil) })
}

// ---------------------------------------------------------------------------
// Controlled read interface.
// ---------------------------------------------------------------------------

func (cb *ConditionalBatch) snapshotProbe(limit SeqNum) (*condProbe, func()) {
	state := cb.db.loadReadState()
	p := newCondProbe(cb.db, state.current, state.memtables, limit)
	return p, func() {
		p.close()
		state.unref()
	}
}

// TrackPoint records a read dependency on a single point key. The newest
// version of key visible at the conditional batch's snapshot is summarized in
// the read set; any later set, delete, single delete, merge, covering range
// deletion, covering range key or ingest overlapping the key aborts the
// commit.
//
// It is safe to modify the contents of key after TrackPoint returns.
func (cb *ConditionalBatch) TrackPoint(key []byte) error {
	if err := cb.checkMutable(); err != nil {
		return err
	}
	if len(key) == 0 {
		return errors.Wrap(ErrConditionalInvalidRange, "point key must be non-empty")
	}
	p, cleanup := cb.snapshotProbe(cb.readSet.snapSeq)
	defer cleanup()
	c := readCondition{start: key}
	c.found, c.kind_, c.seq = p.newestPointVersion(key)
	cb.readSet.addPoint(&c)
	return nil
}

// TrackRange records a read dependency on every point key in the user key
// range [start, end). Any later point write, merge, range deletion, range key
// operation or external ingest that overlaps the range aborts the commit.
//
// It is safe to modify the contents of the arguments after TrackRange
// returns.
func (cb *ConditionalBatch) TrackRange(start, end []byte) error {
	if err := cb.checkMutable(); err != nil {
		return err
	}
	if err := validateUserRange(cb.db.cmp, start, end); err != nil {
		return err
	}
	p, cleanup := cb.snapshotProbe(cb.readSet.snapSeq)
	defer cleanup()
	c := readCondition{kind: condPointRange, start: cloneKey(start), end: cloneKey(end)}
	c.pointCount, c.pointMax = p.rangePointDigest(start, end)
	cb.readSet.conds = append(cb.readSet.conds, c)
	return nil
}

// TrackRangeKey records a read dependency on every range key in the user key
// range [start, end). Any later range key set, unset, delete or external
// ingest carrying range keys that overlaps the range aborts the commit.
//
// It is safe to modify the contents of the arguments after TrackRangeKey
// returns.
func (cb *ConditionalBatch) TrackRangeKey(start, end []byte) error {
	if err := cb.checkMutable(); err != nil {
		return err
	}
	if err := validateUserRange(cb.db.cmp, start, end); err != nil {
		return err
	}
	p, cleanup := cb.snapshotProbe(cb.readSet.snapSeq)
	defer cleanup()
	c := readCondition{kind: condRangeKey, start: cloneKey(start), end: cloneKey(end)}
	c.spanCount, c.spanMax = p.rangeKeyDigest(start, end)
	cb.readSet.conds = append(cb.readSet.conds, c)
	return nil
}

// trackPointFast records a point condition without computing a version
// digest. It is used by the auto-tracking iterator, which observes the
// version itself: validation only depends on the condition bounds and the
// snapshot sequence number.
func (cb *ConditionalBatch) trackPointFast(key []byte) error {
	if err := cb.checkMutable(); err != nil {
		return err
	}
	cb.readSet.conds = append(cb.readSet.conds, readCondition{
		kind:  condPoint,
		start: cloneKey(key),
	})
	return nil
}

// trackRangeFast records a point-range condition without computing a digest.
// Either bound may be nil to denote an open end (used for iterator gaps).
func (cb *ConditionalBatch) trackRangeFast(lo, hi []byte) error {
	if err := cb.checkMutable(); err != nil {
		return err
	}
	if lo != nil && hi != nil && cb.db.cmp(lo, hi) >= 0 {
		return nil
	}
	cb.readSet.conds = append(cb.readSet.conds, readCondition{
		kind:  condPointRange,
		start: cloneKey(lo),
		end:   cloneKey(hi),
	})
	return nil
}

func validateUserRange(cmp base.Compare, start, end []byte) error {
	if start == nil || end == nil || len(start) == 0 || len(end) == 0 {
		return errors.Wrap(ErrConditionalInvalidRange, "range bounds must be non-empty")
	}
	if cmp(start, end) >= 0 {
		return errors.Wrapf(ErrConditionalInvalidRange, "start %q must be less than end %q", start, end)
	}
	return nil
}

// Get performs a read of key against the conditional batch's snapshot and
// additionally records the key in the read set (see TrackPoint). The returned
// closer must be closed by the caller, as with Snapshot.Get.
func (cb *ConditionalBatch) Get(key []byte) ([]byte, io.Closer, error) {
	if err := cb.checkMutable(); err != nil {
		return nil, nil, err
	}
	// Register the dependency first; the value is read at the same snapshot.
	if err := cb.TrackPoint(key); err != nil {
		return nil, nil, err
	}
	return cb.snap.Get(key)
}

// NewIter returns an iterator over the conditional batch's snapshot. Every
// point key the iterator returns (and every gap between seek targets and
// returned keys) is automatically registered in the read set. Callers that
// depend on the contents of range keys within the iteration bounds should
// additionally call TrackRangeKey, as range-key state is not automatically
// tracked. The returned iterator must be closed.
func (cb *ConditionalBatch) NewIter(o *IterOptions) (*ConditionalIterator, error) {
	if err := cb.checkMutable(); err != nil {
		return nil, err
	}
	iter, err := cb.snap.NewIter(o)
	if err != nil {
		return nil, err
	}
	ci := &ConditionalIterator{cb: cb, iter: iter}
	if o != nil {
		ci.lower = cloneKey(o.GetLowerBound())
		ci.upper = cloneKey(o.GetUpperBound())
	}
	return ci, nil
}

// ---------------------------------------------------------------------------
// Commit.
// ---------------------------------------------------------------------------

// Commit freezes the conditional batch, atomically validates its read set
// inside the Pebble commit pipeline, and on success applies all writes in a
// single WAL record at a single commit sequence number.
//
// A *ConditionalConflictError (errors.Is(err, ErrConditionalConflict)) means
// the read set was invalidated and nothing was written or persisted. The
// conditional batch is aborted in that case; the caller should re-read against
// a fresh snapshot and build a new conditional batch.
//
// A *ConditionalCommitError reports whether the batch may already have been
// committed or persisted; the caller must not blindly retry when
// MaybePersisted or Committed is set.
//
// If ctx is canceled before the WAL record is written, Commit returns a
// ConditionalCommitError at stage CommitStageCanceled with no persistence.
// Once the WAL record has been written the commit runs to completion; a
// canceled context is then reported together with the committed/durable
// status.
func (cb *ConditionalBatch) Commit(ctx context.Context, opts *WriteOptions) error {
	switch cb.status {
	case condCommitted:
		return ErrConditionalBatchAlreadyCommitted
	case condAborted, condClosed:
		return ErrConditionalBatchAborted
	case condFrozen:
		return ErrConditionalBatchFrozen
	}
	if err := checkClosedAndReadOnly(cb); err != nil {
		cb.markAborted()
		return err
	}
	if err := ctx.Err(); err != nil {
		cb.markAborted()
		return &ConditionalCommitError{Stage: CommitStageCanceled, Err: err}
	}
	if err := cb.readSet.freeze(cb.db.cmp); err != nil {
		cb.markAborted()
		return err
	}
	cb.status = condFrozen

	sync := false
	if opts != nil {
		sync = opts.Sync
	}
	seq, err := cb.db.commitConditionalBatch(ctx, cb, sync)
	if err != nil {
		cb.markAborted()
		return err
	}
	cb.commitSeq = seq
	cb.status = condCommitted
	if err := ctx.Err(); err != nil {
		// Already committed; report honestly.
		return &ConditionalCommitError{
			Stage:     CommitStageSync,
			Committed: true,
			Durable:   sync,
			Err:       err,
		}
	}
	return nil
}

func checkClosedAndReadOnly(cb *ConditionalBatch) error {
	if v := cb.db.closed.Load(); v != nil {
		return v.(error)
	}
	if cb.db.opts.ReadOnly {
		return ErrReadOnly
	}
	return nil
}

func (cb *ConditionalBatch) markAborted() {
	cb.status = condAborted
	if cb.batch != nil {
		// Prevent batch reuse on error, mirroring the commit pipeline.
		cb.batch.db = nil
	}
}

// commitConditionalBatch is the DB-side counterpart to applyInternal for
// conditional batches. It performs the same pre-commit preparation as
// applyInternal and then hands the batch to the commit pipeline, which runs
// the read-set validation atomically with WAL and sequence-number handling.
func (d *DB) commitConditionalBatch(
	ctx context.Context, cb *ConditionalBatch, sync bool,
) (SeqNum, error) {
	b := cb.batch
	if b.committing {
		// ConditionalBatch.Commit sets its own status guard, so reaching this
		// twice indicates concurrent misuse.
		return 0, errors.AssertionFailedf("pebble: conditional batch already committing")
	}
	if b.applied.Load() {
		return 0, errors.AssertionFailedf("pebble: conditional batch already applied")
	}
	if sync && d.opts.DisableWAL {
		return 0, errors.New("pebble: WAL disabled")
	}
	if fmv := d.FormatMajorVersion(); fmv < b.minimumFormatMajorVersion {
		return 0, errors.AssertionFailedf(
			"pebble: conditional batch requires at least format major version %d (current: %d)",
			b.minimumFormatMajorVersion, fmv)
	}
	if b.countRangeKeys > 0 && d.split == nil {
		return 0, errNoSplit
	}
	b.committing = true
	if b.db == nil {
		if err := b.refreshMemTableSize(); err != nil {
			return 0, err
		}
	}
	// Mirror applyInternal's large-batch handling. The flushable wrapper is
	// only an in-memory object until the pipeline commits, so a validation
	// conflict simply discards it.
	if b.memTableSize >= d.largeBatchThreshold {
		fb, err := newFlushableBatch(b, d.opts.Comparer)
		if err != nil {
			return 0, err
		}
		b.flushable = fb
	}
	if err := d.commit.commitConditional(ctx, d, b, cb.readSet, sync, false /* noSyncWait */); err != nil {
		return 0, err
	}
	if b.flushable != nil {
		b.data = nil
	}
	return b.SeqNum(), nil
}

// ---------------------------------------------------------------------------
// condProbe: reads raw internal state (flushables + LSM) at a sequence
// number limit. Used both to build read-set digests against a snapshot and to
// validate the read set at commit time.
// ---------------------------------------------------------------------------

type condSpanKind uint8

const (
	spanRangeDelete condSpanKind = iota
	spanRangeKey
)

type condProbe struct {
	db    *DB
	vers  *manifest.Version
	mems  flushableList
	limit SeqNum
	cmp   base.Compare

	// Lazily created iterators, aligned with mems.
	point []internalIterator
	rdel  []keyspan.FragmentIterator
	rkey  []keyspan.FragmentIterator
}

func newCondProbe(
	d *DB, vers *manifest.Version, mems flushableList, limit SeqNum,
) *condProbe {
	return &condProbe{
		db:    d,
		vers:  vers,
		mems:  mems,
		limit: limit,
		cmp:   d.cmp,
	}
}

// memsVisible returns the oldest-to-scan index range. Memtables whose
// logSeqNum is >= limit cannot contain records visible at limit (mirrors the
// trimming done for snapshot iterators). Returns indexes in descending order
// (newest first) via the returned slice.
func (p *condProbe) visibleMemIndexes() []int {
	var out []int
	for j := len(p.mems) - 1; j >= 0; j-- {
		if p.mems[j].logSeqNum >= p.limit {
			continue
		}
		out = append(out, j)
	}
	return out
}

func (p *condProbe) memPoint(j int) internalIterator {
	if p.point == nil {
		p.point = make([]internalIterator, len(p.mems))
	}
	if p.point[j] == nil {
		p.point[j] = p.mems[j].newIter(nil)
	}
	return p.point[j]
}

func (p *condProbe) memRDel(j int) keyspan.FragmentIterator {
	if p.rdel == nil {
		p.rdel = make([]keyspan.FragmentIterator, len(p.mems))
	}
	if p.rdel[j] == nil {
		p.rdel[j] = p.mems[j].newRangeDelIter(nil)
	}
	return p.rdel[j]
}

func (p *condProbe) memRKey(j int) keyspan.FragmentIterator {
	if p.rkey == nil {
		p.rkey = make([]keyspan.FragmentIterator, len(p.mems))
	}
	if p.rkey[j] == nil {
		p.rkey[j] = p.mems[j].newRangeKeyIter(nil)
	}
	return p.rkey[j]
}

func (p *condProbe) close() {
	for _, it := range p.point {
		if it != nil {
			_ = it.Close()
		}
	}
	for _, it := range p.rdel {
		if it != nil {
			it.Close()
		}
	}
	for _, it := range p.rkey {
		if it != nil {
			it.Close()
		}
	}
}

// newestPointVersion returns the kind and sequence number of the newest raw
// internal point record for key with a sequence number <= the probe limit
// (across all visible flushables and LSM levels), or found=false.
func (p *condProbe) newestPointVersion(key []byte) (found bool, kind InternalKeyKind, seq SeqNum) {
	var best SeqNum
	var bestKind InternalKeyKind
	consider := func(kv *base.InternalKV) {
		if kv == nil {
			return
		}
		s := kv.SeqNum()
		// A snapshot/limit S makes committed keys visible iff seqnum < S.
		if s < p.limit && s > best {
			best = s
			bestKind = kv.Kind()
			found = true
		}
	}
	for _, j := range p.visibleMemIndexes() {
		it := p.memPoint(j)
		for kv := it.SeekGE(key, base.SeekGEFlagsNone); kv != nil && p.cmp(kv.K.UserKey, key) == 0; kv = it.Next() {
			consider(kv)
		}
	}
	p.forEachOverlappingFile(key, nil, false, true, func(f *manifest.TableMetadata) shouldContinue {
		iters, err := p.db.newIters(context.TODO(), f, nil, internalIterOpts{}, iterPointKeys)
		if err != nil {
			return stopIteration
		}
		defer func() { _ = iters.CloseAll() }()
		pit := iters.Point()
		for kv := pit.SeekGE(key, base.SeekGEFlagsNone); kv != nil && p.cmp(kv.K.UserKey, key) == 0; kv = pit.Next() {
			consider(kv)
		}
		return continueIteration
	})
	return found, bestKind, best
}

// findNewerPoint returns the newest internal point record in (after, limit]
// for the exact key lo (point true), or anywhere within [lo, hi) (point
// false; hi nil denotes [lo, +inf)). The returned key is owned by the
// underlying iterator and must be consumed immediately.
func (p *condProbe) findNewerPoint(
	lo, hi []byte, point bool, after SeqNum,
) (key []byte, kind InternalKeyKind, seq SeqNum) {
	considerKV := func(kv *base.InternalKV) bool {
		s := kv.SeqNum()
		// A snapshot at `after` sees seqnums < after; [after, limit) are the
		// newer, potentially conflicting sequence numbers.
		if s >= after && s < p.limit {
			key = append(key[:0], kv.K.UserKey...)
			kind = kv.Kind()
			seq = s
			return true
		}
		return false
	}
	if point {
		// Point lookup. Keep the maximum sequence number across all layers;
		// the first internal key of a user key in a layer is its newest
		// version in that layer.
		for _, j := range p.visibleMemIndexes() {
			it := p.memPoint(j)
			if kv := it.SeekGE(lo, base.SeekGEFlagsNone); kv != nil && p.cmp(kv.K.UserKey, lo) == 0 {
				considerKV(kv)
			}
		}
		p.forEachOverlappingFile(lo, nil, false, true, func(f *manifest.TableMetadata) shouldContinue {
			if f.SeqNums.High < after {
				return continueIteration
			}
			iters, err := p.db.newIters(context.TODO(), f, nil, internalIterOpts{}, iterPointKeys)
			if err != nil {
				return stopIteration
			}
			defer func() { _ = iters.CloseAll() }()
			if kv := iters.Point().SeekGE(lo, base.SeekGEFlagsNone); kv != nil && p.cmp(kv.K.UserKey, lo) == 0 {
				considerKV(kv)
			}
			return continueIteration
		})
		return key, kind, seq
	}

	// Range scan: return the first hit in any layer. hi nil scans [lo, +inf).
	inRange := func(k []byte) bool {
		return hi == nil || p.cmp(k, hi) < 0
	}
	for _, j := range p.visibleMemIndexes() {
		it := p.mems[j].newIter(&IterOptions{LowerBound: lo, UpperBound: hi})
		for kv := it.SeekGE(lo, base.SeekGEFlagsNone); kv != nil; kv = it.Next() {
			if !inRange(kv.K.UserKey) {
				break
			}
			if considerKV(kv) {
				_ = it.Close()
				return key, kind, seq
			}
		}
		_ = it.Close()
	}
	var found bool
	p.forEachOverlappingFile(lo, hi, false, false, func(f *manifest.TableMetadata) shouldContinue {
		if found || f.SeqNums.High < after {
			return continueIteration
		}
		iters, err := p.db.newIters(context.TODO(), f,
			&IterOptions{LowerBound: lo, UpperBound: hi}, internalIterOpts{}, iterPointKeys)
		if err != nil {
			return stopIteration
		}
		defer func() { _ = iters.CloseAll() }()
		pit := iters.Point()
		for kv := pit.SeekGE(lo, base.SeekGEFlagsNone); kv != nil; kv = pit.Next() {
			if !inRange(kv.K.UserKey) {
				break
			}
			if considerKV(kv) {
				found = true
				return stopIteration
			}
		}
		return continueIteration
	})
	return key, kind, seq
}

// findNewerSpan returns the largest key sequence number in (after, limit]
// attached to a span overlapping the condition: the exact point lo when point
// is true, or [lo, hi) otherwise (either bound nil means open-ended).
func (p *condProbe) findNewerSpan(
	which condSpanKind, lo, hi []byte, point bool, after SeqNum,
) (start, end []byte, seq SeqNum) {
	checkIter := func(it keyspan.FragmentIterator) {
		if it == nil {
			return
		}
		var span *keyspan.Span
		var err error
		if lo == nil {
			span, err = it.First()
		} else {
			span, err = it.SeekGE(lo)
		}
		if err != nil {
			return
		}
		for ; span != nil; span, err = it.Next() {
			if err != nil {
				return
			}
			if point {
				// Point: the first span with end > lo is the only candidate
				// that can cover lo.
				if p.cmp(span.Start, lo) > 0 || p.cmp(span.End, lo) <= 0 {
					return
				}
			} else {
				if hi != nil && p.cmp(span.Start, hi) >= 0 {
					return
				}
				if lo != nil && p.cmp(span.End, lo) <= 0 {
					continue
				}
			}
			var best SeqNum
			for i := range span.Keys {
				if s := span.Keys[i].SeqNum(); s >= after && s < p.limit && s > best {
					best = s
				}
			}
			if best > seq {
				seq = best
				start = span.Start
				end = span.End
			}
			if point {
				return
			}
		}
	}
	for _, j := range p.visibleMemIndexes() {
		if which == spanRangeDelete {
			checkIter(p.memRDel(j))
		} else {
			checkIter(p.memRKey(j))
		}
	}
	p.forEachOverlappingFile(lo, hi, which == spanRangeKey, point, func(f *manifest.TableMetadata) shouldContinue {
		if f.SeqNums.High < after {
			return continueIteration
		}
		var kinds iterKinds
		if which == spanRangeDelete {
			kinds = iterRangeDeletions
		} else {
			kinds = iterRangeKeys
		}
		iters, err := p.db.newIters(context.TODO(), f, nil, internalIterOpts{}, kinds)
		if err != nil {
			return stopIteration
		}
		defer func() { _ = iters.CloseAll() }()
		if which == spanRangeDelete {
			checkIter(iters.rangeDeletion)
		} else {
			checkIter(iters.rangeKey)
		}
		return continueIteration
	})
	return start, end, seq
}

// rangePointDigest computes an approximate version count and the largest
// point sequence number visible at the probe limit within [lo, hi).
func (p *condProbe) rangePointDigest(lo, hi []byte) (count uint64, max SeqNum) {
	consider := func(s SeqNum) {
		if s < p.limit && s > max {
			max = s
		}
	}
	for _, j := range p.visibleMemIndexes() {
		it := p.mems[j].newIter(&IterOptions{LowerBound: lo, UpperBound: hi})
		for kv := it.SeekGE(lo, base.SeekGEFlagsNone); kv != nil; kv = it.Next() {
			if p.cmp(kv.K.UserKey, hi) >= 0 {
				break
			}
			if kv.SeqNum() < p.limit {
				count++
			}
			consider(kv.SeqNum())
		}
		_ = it.Close()
	}
	p.forEachOverlappingFile(lo, hi, false, false, func(f *manifest.TableMetadata) shouldContinue {
		iters, err := p.db.newIters(context.TODO(), f,
			&IterOptions{LowerBound: lo, UpperBound: hi}, internalIterOpts{}, iterPointKeys)
		if err != nil {
			return stopIteration
		}
		defer func() { _ = iters.CloseAll() }()
		pit := iters.Point()
		for kv := pit.SeekGE(lo, base.SeekGEFlagsNone); kv != nil; kv = pit.Next() {
			if p.cmp(kv.K.UserKey, hi) >= 0 {
				break
			}
			if kv.SeqNum() < p.limit {
				count++
			}
			consider(kv.SeqNum())
		}
		return continueIteration
	})
	return count, max
}

// rangeKeyDigest returns the number of fragmented range-key spans overlapping
// [lo, hi) at the probe limit and the largest range-key sequence number.
func (p *condProbe) rangeKeyDigest(lo, hi []byte) (count uint64, max SeqNum) {
	visit := func(it keyspan.FragmentIterator) {
		if it == nil {
			return
		}
		span, err := it.SeekGE(lo)
		for ; span != nil; span, err = it.Next() {
			if err != nil {
				return
			}
			if p.cmp(span.Start, hi) >= 0 {
				return
			}
			if p.cmp(span.End, lo) <= 0 {
				continue
			}
			count++
			for i := range span.Keys {
				if s := span.Keys[i].SeqNum(); s < p.limit && s > max {
					max = s
				}
			}
		}
	}
	for _, j := range p.visibleMemIndexes() {
		visit(p.memRKey(j))
	}
	p.forEachOverlappingFile(lo, hi, true, false, func(f *manifest.TableMetadata) shouldContinue {
		iters, err := p.db.newIters(context.TODO(), f, nil, internalIterOpts{}, iterRangeKeys)
		if err != nil {
			return stopIteration
		}
		defer func() { _ = iters.CloseAll() }()
		visit(iters.rangeKey)
		return continueIteration
	})
	return count, max
}

// forEachOverlappingFile invokes fn for every LSM file that might overlap the
// key bounds [lo, hi). When point is true it is an exact point lookup at lo;
// otherwise lo/hi describe a range and either may be nil for an open-ended
// range. When rangeKeys is true the range-key level organization
// (RangeKeyLevels) is used; otherwise the point-key organization (L0
// sublevels followed by L1+) is used.
func (p *condProbe) forEachOverlappingFile(
	lo, hi []byte, rangeKeys, point bool, fn func(*manifest.TableMetadata) shouldContinue,
) {
	visitIter := func(it manifest.LevelIterator) {
		var f *manifest.TableMetadata
		if lo == nil {
			f = it.First()
		} else {
			f = it.SeekGE(p.cmp, lo)
		}
		for ; f != nil; f = it.Next() {
			ub := f.UserKeyBounds()
			if point {
				if p.cmp(ub.Start, lo) > 0 || !ub.End.IsUpperBoundFor(p.cmp, lo) {
					return
				}
			} else if hi != nil && p.cmp(ub.Start, hi) >= 0 {
				return
			}
			if fn(f) == stopIteration {
				return
			}
		}
	}
	if rangeKeys {
		// Range-key L0 is walked newest-first by the level metadata iterator.
		if !p.vers.RangeKeyLevels[0].Empty() {
			it := p.vers.RangeKeyLevels[0].Iter()
			for f := it.Last(); f != nil; f = it.Prev() {
				ub := f.UserKeyBounds()
				if !userBoundsOverlap(p.cmp, lo, hi, point, &ub) {
					continue
				}
				if fn(f) == stopIteration {
					return
				}
			}
		}
		for level := 1; level < numLevels; level++ {
			if p.vers.RangeKeyLevels[level].Empty() {
				continue
			}
			visitIter(p.vers.RangeKeyLevels[level].Iter())
		}
		return
	}
	for sub := len(p.vers.L0SublevelFiles) - 1; sub >= 0; sub-- {
		visitIter(p.vers.L0SublevelFiles[sub].Iter())
	}
	for level := 1; level < numLevels; level++ {
		if p.vers.Levels[level].Empty() {
			continue
		}
		visitIter(p.vers.Levels[level].Iter())
	}
}

// validate checks every condition of the (frozen) read set against the state
// currently visible at commitSeq (the next sequence number), while d.mu is
// held by the caller. It returns nil when the read set is still satisfied.
func (rs *optimisticReadSet) validate(
	d *DB, commitSeq SeqNum,
) *ConditionalConflictError {
	p := newCondProbe(d, d.mu.versions.currentVersion(), d.mu.mem.queue, commitSeq)
	defer p.close()
	snapSeq := rs.snapSeq
	for i := range rs.conds {
		c := &rs.conds[i]
		// External ingests take precedence: they are not distinguishable
		// from ordinary point writes once installed, so consult the ingest
		// log first.
		if c.kind == condPoint {
			if ingestSeq, ok := d.ingestOverlapsLocked(snapSeq, c.start, nil, true /* point */); ok {
				return rs.conflict(c, ConflictExternalIngest, ingestSeq, commitSeq)
			}
		} else {
			if ingestSeq, ok := d.ingestOverlapsLocked(snapSeq, c.start, c.end, false /* point */); ok {
				return rs.conflict(c, ConflictExternalIngest, ingestSeq, commitSeq)
			}
		}
		switch c.kind {
		case condPoint:
			if _, _, s := p.findNewerSpan(spanRangeDelete, c.start, nil, true, snapSeq); s > 0 {
				return rs.conflict(c, ConflictRangeDelete, s, commitSeq)
			}
			if _, _, s := p.findNewerSpan(spanRangeKey, c.start, nil, true, snapSeq); s > 0 {
				return rs.conflict(c, ConflictRangeKey, s, commitSeq)
			}
			if _, k, s := p.findNewerPoint(c.start, nil, true, snapSeq); s > 0 {
				reason := ConflictPointWrite
				if k == InternalKeyKindMerge {
					reason = ConflictMergeResult
				}
				return rs.conflict(c, reason, s, commitSeq)
			}
		case condPointRange:
			if _, _, s := p.findNewerSpan(spanRangeDelete, c.start, c.end, false, snapSeq); s > 0 {
				return rs.conflict(c, ConflictRangeDelete, s, commitSeq)
			}
			if _, _, s := p.findNewerSpan(spanRangeKey, c.start, c.end, false, snapSeq); s > 0 {
				return rs.conflict(c, ConflictRangeKey, s, commitSeq)
			}
			if _, kind, s := p.findNewerPoint(c.start, c.end, false, snapSeq); s > 0 {
				reason := ConflictPointWrite
				if kind == InternalKeyKindMerge {
					reason = ConflictMergeResult
				}
				return rs.conflict(c, reason, s, commitSeq)
			}
		case condRangeKey:
			if _, _, s := p.findNewerSpan(spanRangeKey, c.start, c.end, false, snapSeq); s > 0 {
				return rs.conflict(c, ConflictRangeKey, s, commitSeq)
			}
		}
	}
	return nil
}

// ConditionalIterator wraps the iterator over a conditional batch's snapshot
// and automatically records the keys (and the gaps between seek targets and
// returned keys) it observes in the read set. The tracking is conservative:
// every returned point key is tracked as a point condition, and every gap in
// which the iterator proved no key exists is tracked as a key range.
//
// A ConditionalIterator is not safe for concurrent use and must be closed.
type ConditionalIterator struct {
	cb    *ConditionalBatch
	iter  *Iterator
	lower []byte
	upper []byte
	last  []byte
	err   error
}

func (c *ConditionalIterator) record(err error) {
	if c.err == nil && err != nil {
		c.err = err
	}
}

func (c *ConditionalIterator) trackPoint(key []byte) {
	c.record(c.cb.trackPointFast(key))
}

func (c *ConditionalIterator) trackRange(lo, hi []byte) {
	c.record(c.cb.trackRangeFast(lo, hi))
}

// SeekGE positions on the first key >= key.
func (c *ConditionalIterator) SeekGE(key []byte) bool {
	ok := c.iter.SeekGE(key)
	if ok {
		k := c.iter.Key()
		if c.cb.db.opts.Comparer.Equal(k, key) {
			c.trackPoint(k)
		} else {
			// The iterator proved [key, k) is empty and then returned k.
			c.trackRange(key, k)
			c.trackPoint(k)
		}
		c.last = append(c.last[:0], k...)
	} else {
		c.trackRange(key, c.upper)
		c.last = c.last[:0]
	}
	return ok
}

// SeekPrefixGE positions at the first key with the given prefix that is >=
// prefix. Only the existence of the exact prefix key is semantically read.
func (c *ConditionalIterator) SeekPrefixGE(prefix []byte) bool {
	ok := c.iter.SeekPrefixGE(prefix)
	c.trackPoint(prefix)
	if ok {
		c.last = append(c.last[:0], c.iter.Key()...)
	} else {
		c.last = c.last[:0]
	}
	return ok
}

// First positions on the smallest key in the iterator's bounds.
func (c *ConditionalIterator) First() bool {
	ok := c.iter.First()
	if ok {
		k := c.iter.Key()
		if c.lower != nil {
			c.trackRange(c.lower, k)
		}
		c.trackPoint(k)
		c.last = append(c.last[:0], k...)
	}
	return ok
}

// Last positions on the largest key in the iterator's bounds.
func (c *ConditionalIterator) Last() bool {
	ok := c.iter.Last()
	if ok {
		k := c.iter.Key()
		c.trackPoint(k)
		if c.upper != nil {
			c.trackRange(k, c.upper)
		}
		c.last = append(c.last[:0], k...)
	}
	return ok
}

// Next advances to the next key.
func (c *ConditionalIterator) Next() bool {
	ok := c.iter.Next()
	if ok {
		k := c.iter.Key()
		if c.last != nil {
			c.trackRange(c.last, k)
		}
		c.trackPoint(k)
		c.last = append(c.last[:0], k...)
	} else if c.last != nil {
		c.trackRange(c.last, c.upper)
		c.last = c.last[:0]
	}
	return ok
}

// Prev moves to the previous key.
func (c *ConditionalIterator) Prev() bool {
	ok := c.iter.Prev()
	if ok {
		k := c.iter.Key()
		c.trackPoint(k)
		if c.last != nil {
			c.trackRange(k, c.last)
		}
		c.last = append(c.last[:0], k...)
	} else if c.last != nil {
		c.trackRange(c.lower, c.last)
		c.last = c.last[:0]
	}
	return ok
}

// SeekLT positions on the last key < key.
func (c *ConditionalIterator) SeekLT(key []byte) bool {
	ok := c.iter.SeekLT(key)
	if ok {
		k := c.iter.Key()
		c.trackRange(k, key)
		c.trackPoint(k)
		c.last = append(c.last[:0], k...)
	} else {
		c.trackRange(c.lower, key)
		c.last = c.last[:0]
	}
	return ok
}

// Valid reports whether the iterator is positioned.
func (c *ConditionalIterator) Valid() bool { return c.iter.Valid() }

// Key returns the current key; do not retain.
func (c *ConditionalIterator) Key() []byte { return c.iter.Key() }

// Value returns the current value; do not retain.
func (c *ConditionalIterator) Value() []byte { return c.iter.Value() }

// ValueAndErr returns the current value and any deferred value error.
func (c *ConditionalIterator) ValueAndErr() ([]byte, error) { return c.iter.ValueAndErr() }

// Error returns accumulated iterator or read-tracking errors.
func (c *ConditionalIterator) Error() error {
	return firstError(c.err, c.iter.Error())
}

// SetBounds updates the iterator bounds.
func (c *ConditionalIterator) SetBounds(lower, upper []byte) {
	c.iter.SetBounds(lower, upper)
	c.lower = append(c.lower[:0], lower...)
	c.upper = append(c.upper[:0], upper...)
	c.last = c.last[:0]
}

// Close releases the underlying snapshot iterator.
func (c *ConditionalIterator) Close() error {
	return c.iter.Close()
}

func (rs *optimisticReadSet) conflict(
	c *readCondition, reason ConflictReason, conflictSeq, commitSeq SeqNum,
) *ConditionalConflictError {
	e := &ConditionalConflictError{
		Reason:         reason,
		SnapshotSeqNum: rs.snapSeq,
		CommitSeqNum:   commitSeq,
		ConflictSeqNum: conflictSeq,
		Start:          c.start,
		End:            c.end,
		Point:          c.kind == condPoint,
	}
	return e
}
