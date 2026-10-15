// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func conditionalTestOptions(fs vfs.FS) *Options {
	return &Options{
		FS:                                       fs,
		FormatMajorVersion:                       FormatNewest,
		ExperimentalOptimisticConditionalBatches: true,
	}
}

func openConditionalDB(t *testing.T, opts *Options) (*DB, vfs.FS) {
	t.Helper()
	fs := vfs.NewMem()
	if opts == nil {
		opts = conditionalTestOptions(fs)
	}
	opts.FS = fs
	d, err := Open("", opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, d.Close())
	})
	return d, fs
}

func conflictErr(t *testing.T, err error) *ConditionalConflictError {
	t.Helper()
	require.Error(t, err)
	var ce *ConditionalConflictError
	require.True(t, errors.As(err, &ce), "expected a conditional conflict, got %v", err)
	require.True(t, errors.Is(err, ErrConditionalConflict))
	return ce
}

func getVal(t *testing.T, d *DB, key string) string {
	t.Helper()
	v, closer, err := d.Get([]byte(key))
	if errors.Is(err, ErrNotFound) {
		return ""
	}
	require.NoError(t, err)
	defer func() { require.NoError(t, closer.Close()) }()
	return string(v)
}

// A simple append merger with a unique name for merge-conflict tests.
var conditionalTestMerger = &Merger{
	Name: "pebble.conditional-test-append",
	Merge: func(key, value []byte) (ValueMerger, error) {
		return &conditionalValueMerger{buf: append([]byte(nil), value...)}, nil
	},
}

type conditionalValueMerger struct {
	buf []byte
}

func (m *conditionalValueMerger) MergeNewer(value []byte) error {
	m.buf = append(m.buf, value...)
	return nil
}

func (m *conditionalValueMerger) MergeOlder(value []byte) error {
	buf := make([]byte, 0, len(value)+len(m.buf))
	buf = append(buf, value...)
	m.buf = append(buf, m.buf...)
	return nil
}

func (m *conditionalValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.buf, nil, nil
}

func TestConditionalBatchDisabledByDefault(t *testing.T) {
	d, _ := openConditionalDB(t, &Options{FS: vfs.NewMem(), FormatMajorVersion: FormatNewest})
	cb, err := d.NewConditionalBatch(nil)
	require.Nil(t, cb)
	require.True(t, errors.Is(err, ErrConditionalDisabled))
}

func TestConditionalBatchCommitSuccess(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))
	require.NoError(t, cb.Commit(context.Background(), &WriteOptions{Sync: true}))
	require.Equal(t, "v2", getVal(t, d, "k"))
	// All writes used a single sequence number.
	require.NotZero(t, cb.CommitSeqNum())
	info := cb.ReadSetInfo()
	require.Equal(t, 1, info.NumPointKeys)
	require.Equal(t, cb.SnapshotSeqNum(), info.SnapshotSeqNum)

	// Committing again must not replay the writes.
	err = cb.Commit(context.Background(), nil)
	require.True(t, errors.Is(err, ErrConditionalBatchAlreadyCommitted), "got %v", err)
	require.Equal(t, "v2", getVal(t, d, "k"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchPointConflict(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("z1"), []byte("should-not-commit")))

	// Concurrent Set conflicts; the conditional writes must be absent.
	require.NoError(t, d.Set([]byte("k"), []byte("v2"), nil))
	ce := conflictErr(t, cb.Commit(context.Background(), nil))
	require.Equal(t, ConflictPointWrite, ce.Reason)
	require.True(t, ce.Point)
	require.Equal(t, "v2", getVal(t, d, "k"))
	require.Equal(t, "", getVal(t, d, "z1"))
	require.NoError(t, cb.Close())

	// Aborted batch cannot be reused for a retry.
	err = cb.TrackPoint([]byte("k"))
	require.True(t, errors.Is(err, ErrConditionalBatchAborted))
}

func TestConditionalBatchUnrelatedWriteNoConflict(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))

	require.NoError(t, d.Set([]byte("other"), []byte("x"), nil))
	require.NoError(t, cb.Commit(context.Background(), nil))
	require.Equal(t, "v2", getVal(t, d, "k"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchDeleteConflict(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))

	require.NoError(t, d.Delete([]byte("k"), nil))
	ce := conflictErr(t, cb.Commit(context.Background(), nil))
	require.Equal(t, ConflictPointWrite, ce.Reason)
	_, _, gerr := d.Get([]byte("k"))
	require.ErrorIs(t, gerr, ErrNotFound)
	require.NoError(t, cb.Close())
}

func TestConditionalBatchRangeDeleteConflict(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))

	require.NoError(t, d.DeleteRange([]byte("a"), []byte("z"), nil))
	ce := conflictErr(t, cb.Commit(context.Background(), nil))
	require.Equal(t, ConflictRangeDelete, ce.Reason)
	// The range deletion already removed k; the conditional write of k=v2
	// must not be applied.
	require.Equal(t, "", getVal(t, d, "k"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchRangeKeyConflict(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))

	require.NoError(t, d.RangeKeySet([]byte("a"), []byte("z"), nil, []byte("rk"), nil))
	ce := conflictErr(t, cb.Commit(context.Background(), nil))
	require.Equal(t, ConflictRangeKey, ce.Reason)
	// The conditional write of k=v2 must not be applied.
	require.Equal(t, "v1", getVal(t, d, "k"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchMergeConflict(t *testing.T) {
	opts := conditionalTestOptions(vfs.NewMem())
	opts.Merger = conditionalTestMerger
	d, _ := openConditionalDB(t, opts)
	require.NoError(t, d.Set([]byte("m"), []byte("base-"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("m")))
	require.NoError(t, cb.Set([]byte("m"), []byte("new")))

	require.NoError(t, d.Merge([]byte("m"), []byte("operand-"), nil))
	ce := conflictErr(t, cb.Commit(context.Background(), nil))
	require.Equal(t, ConflictMergeResult, ce.Reason)
	require.Equal(t, "base-operand-", getVal(t, d, "m"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchTrackRange(t *testing.T) {
	d, _ := openConditionalDB(t, nil)

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackRange([]byte("m"), []byte("t")))
	require.NoError(t, cb.Set([]byte("q"), []byte("v")))

	// Write inside the tracked range.
	require.NoError(t, d.Set([]byte("n"), []byte("new"), nil))
	ce := conflictErr(t, cb.Commit(context.Background(), nil))
	require.Equal(t, ConflictPointWrite, ce.Reason)
	require.False(t, ce.Point)
	require.Equal(t, "", getVal(t, d, "q"))
	require.NoError(t, cb.Close())

	// A write outside the range does not conflict.
	cb2, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb2.TrackRange([]byte("m"), []byte("t")))
	require.NoError(t, cb2.Set([]byte("q"), []byte("v")))
	require.NoError(t, d.Set([]byte("z"), []byte("outside"), nil))
	require.NoError(t, cb2.Commit(context.Background(), nil))
	require.Equal(t, "v", getVal(t, d, "q"))
	require.NoError(t, cb2.Close())

	// Range deletion overlapping the range conflicts.
	cb3, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb3.TrackRange([]byte("a"), []byte("c")))
	require.NoError(t, d.DeleteRange([]byte("b"), []byte("d"), nil))
	ce = conflictErr(t, cb3.Commit(context.Background(), nil))
	require.Equal(t, ConflictRangeDelete, ce.Reason)
	require.NoError(t, cb3.Close())
}

func TestConditionalBatchTrackRangeKey(t *testing.T) {
	d, _ := openConditionalDB(t, nil)

	// A point write within a tracked range-key range is not a conflict.
	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackRangeKey([]byte("a"), []byte("z")))
	require.NoError(t, cb.Set([]byte("out"), []byte("1")))
	require.NoError(t, d.Set([]byte("p"), []byte("point"), nil))
	require.NoError(t, cb.Commit(context.Background(), nil))
	require.NoError(t, cb.Close())

	// A new range key in the range conflicts.
	cb2, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb2.TrackRangeKey([]byte("a"), []byte("z")))
	require.NoError(t, d.RangeKeySet([]byte("c"), []byte("d"), nil, []byte("v"), nil))
	ce := conflictErr(t, cb2.Commit(context.Background(), nil))
	require.Equal(t, ConflictRangeKey, ce.Reason)
	require.NoError(t, cb2.Close())
}

func TestConditionalBatchIngestConflict(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open("", conditionalTestOptions(fs))
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	// Build an external sstable containing key "k".
	require.NoError(t, createExternalSST(fs, "ext", map[string]string{"k": "ingested"}))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("z"), []byte("1")))
	require.NoError(t, d.Ingest(context.Background(), []string{"ext"}))
	ce := conflictErr(t, cb.Commit(context.Background(), nil))
	require.Equal(t, ConflictExternalIngest, ce.Reason)
	require.Equal(t, "ingested", getVal(t, d, "k"))
	require.Equal(t, "", getVal(t, d, "z"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchIngestOutsideReadSet(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open("", conditionalTestOptions(fs))
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	require.NoError(t, createExternalSST(fs, "ext", map[string]string{"z": "ingested"}))
	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("a")))
	require.NoError(t, cb.Set([]byte("a"), []byte("1")))
	require.NoError(t, d.Ingest(context.Background(), []string{"ext"}))
	require.NoError(t, cb.Commit(context.Background(), nil))
	require.Equal(t, "1", getVal(t, d, "a"))
	require.Equal(t, "ingested", getVal(t, d, "z"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchValidationErrors(t *testing.T) {
	d, _ := openConditionalDB(t, nil)

	// Empty read set.
	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.Set([]byte("a"), []byte("b")))
	err = cb.Commit(context.Background(), nil)
	require.True(t, errors.Is(err, ErrConditionalReadSetEmpty), "got %v", err)
	require.NoError(t, cb.Close())

	// Invalid ranges.
	cb2, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.True(t, errors.Is(cb2.TrackRange([]byte("z"), []byte("a")), ErrConditionalInvalidRange))
	require.True(t, errors.Is(cb2.TrackRange(nil, []byte("a")), ErrConditionalInvalidRange))
	require.True(t, errors.Is(cb2.TrackRangeKey([]byte("a"), []byte("a")), ErrConditionalInvalidRange))
	require.NoError(t, cb2.Close())

	// Frozen after commit attempt: a failed validation aborts and rejects
	// further mutation.
	cb3, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb3.TrackPoint([]byte("x")))
	require.NoError(t, d.Set([]byte("x"), []byte("y"), nil))
	require.Error(t, cb3.Commit(context.Background(), nil))
	require.Error(t, cb3.Set([]byte("a"), []byte("b")))
	require.NoError(t, cb3.Close())
}

func TestConditionalBatchContextCanceled(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = cb.Commit(ctx, nil)
	var ce *ConditionalCommitError
	require.True(t, errors.As(err, &ce), "got %v", err)
	require.Equal(t, CommitStageCanceled, ce.Stage)
	require.False(t, ce.MaybePersisted)
	require.False(t, ce.Committed)
	require.Equal(t, "v1", getVal(t, d, "k"))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchClosedDB(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open("", conditionalTestOptions(fs))
	require.NoError(t, err)
	require.NoError(t, d.Close())
	cb, err := d.NewConditionalBatch(nil)
	require.ErrorIs(t, err, ErrClosed)
	require.Nil(t, cb)
}

func TestConditionalBatchFlushNoFalseConflict(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))

	// A flush and compaction must not invalidate the read set on their own.
	require.NoError(t, d.Flush())
	require.NoError(t, cb.Commit(context.Background(), nil))
	require.Equal(t, "v2", getVal(t, d, "k"))
	require.NoError(t, cb.Close())

	// Same after a compaction.
	cb2, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb2.TrackPoint([]byte("k")))
	require.NoError(t, cb2.Set([]byte("k"), []byte("v3")))
	require.NoError(t, d.Compact(context.Background(), []byte{0x00}, []byte{0xff}, false))
	require.NoError(t, cb2.Commit(context.Background(), nil))
	require.Equal(t, "v3", getVal(t, d, "k"))
	require.NoError(t, cb2.Close())
}

func TestConditionalBatchGet(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	v, closer, err := cb.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, "v1", string(v))
	require.NoError(t, closer.Close())
	require.NoError(t, cb.Set([]byte("k"), []byte("v2")))
	require.NoError(t, cb.Commit(context.Background(), nil))
	require.NoError(t, cb.Close())

	// Get tracked the key: a concurrent change before commit would conflict.
	require.NoError(t, d.Set([]byte("other"), []byte("x"), nil))
	cb2, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	_, closer, gerr := cb2.Get([]byte("missing"))
	require.ErrorIs(t, gerr, ErrNotFound)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.NoError(t, d.Set([]byte("missing"), []byte("now"), nil))
	conflictErr(t, cb2.Commit(context.Background(), nil))
	require.NoError(t, cb2.Close())
}

func TestConditionalBatchAutoTrackingIter(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, d.Set([]byte(k), []byte("1"), nil))
	}

	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	it, err := cb.NewIter(nil)
	require.NoError(t, err)
	var seen []string
	for ok := it.First(); ok; ok = it.Next() {
		seen = append(seen, string(it.Key()))
	}
	require.NoError(t, it.Error())
	require.Equal(t, []string{"a", "b", "c"}, seen)
	require.NoError(t, it.Close())
	require.NoError(t, cb.Set([]byte("a"), []byte("2")))

	// Inserting into a gap the iterator scanned conflicts.
	require.NoError(t, d.Set([]byte("aa"), []byte("gap"), nil))
	conflictErr(t, cb.Commit(context.Background(), nil))
	require.NoError(t, cb.Close())

	// Writing outside the scanned prefix region still conflicts via the open
	// gap from "c" onwards (Next proved nothing exists after c).
	cb2, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	it2, err := cb2.NewIter(&IterOptions{LowerBound: []byte("a"), UpperBound: []byte("d")})
	require.NoError(t, err)
	for ok := it2.First(); ok; ok = it2.Next() {
	}
	require.NoError(t, it2.Close())
	require.NoError(t, d.Set([]byte("d"), []byte("outside-bounds"), nil))
	// "d" is outside the iterator bounds, so no conflict.
	require.NoError(t, cb2.Commit(context.Background(), nil))
	require.NoError(t, cb2.Close())
	require.Equal(t, "outside-bounds", getVal(t, d, "d"))
}

func TestConditionalBatchExplicitSnapshot(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))
	snap := d.NewSnapshot()
	defer func() { require.NoError(t, snap.Close()) }()

	// Write after the snapshot was taken.
	require.NoError(t, d.Set([]byte("other"), []byte("x"), nil))

	cb, err := d.NewConditionalBatch(snap)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("other")))
	require.NoError(t, cb.Set([]byte("other"), []byte("y")))
	// The snapshot predates "other", so it is a conflict (absence changed).
	conflictErr(t, cb.Commit(context.Background(), nil))
	require.NoError(t, cb.Close())
}

func TestConditionalBatchReadOnlyValidation(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("a"), []byte("1"), nil))

	// No writes: validation succeeds without consuming a sequence number.
	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	before := d.Metrics().WAL.Size
	require.NoError(t, cb.TrackPoint([]byte("a")))
	require.NoError(t, cb.Commit(context.Background(), nil))
	require.Zero(t, cb.CommitSeqNum())
	require.NoError(t, cb.Close())
	require.Equal(t, before, d.Metrics().WAL.Size)
	require.Equal(t, "1", getVal(t, d, "a"))

	// No writes: a conflict is still reported without side effects.
	cb2, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb2.TrackPoint([]byte("b")))
	require.NoError(t, d.Set([]byte("b"), []byte("x"), nil))
	conflictErr(t, cb2.Commit(context.Background(), nil))
	require.NoError(t, cb2.Close())
}

func TestConditionalBatchReadSetSummary(t *testing.T) {
	d, _ := openConditionalDB(t, nil)
	require.NoError(t, d.Set([]byte("k"), []byte("v1"), nil))
	cb, err := d.NewConditionalBatch(nil)
	require.NoError(t, err)
	require.NoError(t, cb.TrackPoint([]byte("k")))
	require.NoError(t, cb.TrackPoint([]byte("absent")))
	require.NoError(t, cb.TrackRange([]byte("m"), []byte("t")))
	require.NoError(t, cb.TrackRangeKey([]byte("u"), []byte("z")))

	rs := cb.ReadSet()
	require.Len(t, rs, 4)
	require.Equal(t, ConditionPoint, rs[0].Kind)
	require.True(t, rs[0].Observed)
	require.Equal(t, InternalKeyKindSet, rs[0].ObservedKind)
	require.NotZero(t, rs[0].ObservedSeqNum)
	require.False(t, rs[1].Observed)
	require.Equal(t, ConditionKeyRange, rs[2].Kind)
	require.Equal(t, ConditionRangeKey, rs[3].Kind)

	info := cb.ReadSetInfo()
	require.NotEmpty(t, info.Comparer)
	require.Equal(t, DefaultMerger.Name, info.Merger)
	require.NotZero(t, info.FormatMajorVersion)
	require.Equal(t, 2, info.NumPointKeys)
	require.Equal(t, 1, info.NumKeyRanges)
	require.Equal(t, 1, info.NumRangeKeys)
	require.NoError(t, cb.Close())
}

func TestConditionalBatchConcurrent(t *testing.T) {
	d, _ := openConditionalDB(t, nil)

	// Many goroutines perform read-modify-write of counters through
	// conditional batches, retrying conflicts. Plain writers mutate
	// unrelated keys.
	const n = 8
	const iters = 25
	var success atomic.Int64
	var wg sync.WaitGroup
	var failed atomic.Bool
	check := func(cond bool, format string, args ...interface{}) bool {
		if !cond {
			t.Errorf(format, args...)
			failed.Store(true)
		}
		return cond
	}
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("counter-%d", g%3))
			for i := 0; i < iters && !failed.Load(); i++ {
				for {
					cb, err := d.NewConditionalBatch(nil)
					if !check(err == nil, "NewConditionalBatch: %v", err) {
						return
					}
					v, closer, err := cb.Get(key)
					cur := 0
					if errors.Is(err, ErrNotFound) {
						if closer != nil {
							check(closer.Close() == nil, "close: %v", err)
						}
					} else {
						if !check(err == nil, "Get: %v", err) {
							_ = cb.Close()
							return
						}
						cur, err = strconv.Atoi(string(v))
						if !check(err == nil, "Atoi: %v", err) {
							_ = closer.Close()
							_ = cb.Close()
							return
						}
						check(closer.Close() == nil, "close: %v", err)
					}
					if err := cb.Set(key, []byte(strconv.Itoa(cur+1))); err != nil {
						t.Errorf("Set: %v", err)
						failed.Store(true)
						_ = cb.Close()
						return
					}
					err = cb.Commit(context.Background(), &WriteOptions{})
					_ = cb.Close()
					if errors.Is(err, ErrConditionalConflict) {
						continue // expected optimistic retry
					}
					if !check(err == nil, "Commit: %v", err) {
						return
					}
					success.Add(1)
					break
				}
			}
		}(g)
	}
	// Concurrent unrelated plain writes must not interfere.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n*iters && !failed.Load(); i++ {
			if err := d.Set([]byte(fmt.Sprintf("other-%d", i)), []byte("x"), nil); err != nil {
				t.Errorf("plain Set: %v", err)
				failed.Store(true)
				return
			}
		}
	}()
	wg.Wait()
	require.False(t, failed.Load())
	require.EqualValues(t, n*iters, success.Load())
}

func createExternalSST(fs vfs.FS, path string, kvs map[string]string) error {
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := w.Set([]byte(k), []byte(kvs[k])); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}
