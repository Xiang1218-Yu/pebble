// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func planTestOptions(mem vfs.FS) *Options {
	return &Options{
		FS:                           mem,
		ExperimentalBatchIngestPlans: true,
		FormatMajorVersion:           FormatNewest,
		DisableTableStats:            true,
	}
}

// writePlanTestSST writes an sstable with Set entries for the given keys.
func writePlanTestSST(t *testing.T, mem vfs.FS, path string, keys ...string) {
	t.Helper()
	f, err := mem.Create(path, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	for _, k := range keys {
		require.NoError(t, w.Set([]byte(k), []byte("v-"+k)))
	}
	require.NoError(t, w.Close())
}

// writePlanRangeDelSST writes an sstable with point Set entries and range
// tombstones [start,end).
func writePlanRangeDelSST(
	t *testing.T, mem vfs.FS, path string, points []string, rangeDels [][2]string,
) {
	t.Helper()
	f, err := mem.Create(path, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	for _, k := range points {
		require.NoError(t, w.Set([]byte(k), []byte("v-"+k)))
	}
	for _, d := range rangeDels {
		require.NoError(t, w.DeleteRange([]byte(d[0]), []byte(d[1])))
	}
	require.NoError(t, w.Close())
}

func countPlanTableFiles(t *testing.T, fs vfs.FS) int {
	t.Helper()
	list, err := fs.List("")
	require.NoError(t, err)
	n := 0
	for _, name := range list {
		fileType, _, ok := base.ParseFilename(fs, name)
		if ok && fileType == base.FileTypeTable {
			n++
		}
	}
	return n
}

func currentLevelFileCounts(t *testing.T, d *DB) [numLevels]int {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	v := d.mu.versions.currentVersion()
	var counts [numLevels]int
	for l := 0; l < numLevels; l++ {
		counts[l] = v.Levels[l].Len()
	}
	return counts
}

func totalTables(counts [numLevels]int) int {
	n := 0
	for _, c := range counts {
		n += c
	}
	return n
}

func existsOnFS(t *testing.T, fs vfs.FS, path string) bool {
	t.Helper()
	_, err := fs.Stat(path)
	return err == nil
}

func TestIngestPlanDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", &Options{FS: mem})
	require.NoError(t, err)

	_, err = d.PrepareIngestPlan(context.Background(), []string{"ext"})
	require.ErrorIs(t, err, ErrIngestPlansDisabled)
	_, err = d.CommitIngestPlan(context.Background(), &IngestPlan{})
	require.ErrorIs(t, err, ErrIngestPlansDisabled)
	require.NoError(t, d.Close())
}

func TestPrepareIngestPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)

	writePlanTestSST(t, mem, "ext-m", "m1", "m2")
	before := countPlanTableFiles(t, mem)
	require.Equal(t, 0, before)

	p, err := d.PrepareIngestPlan(context.Background(), []string{"ext-m"})
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, d.FormatMajorVersion(), p.FormatMajorVersion)
	require.Len(t, p.Files, 1)
	// Preparation is read-only.
	require.Equal(t, 0, countPlanTableFiles(t, mem))
	require.True(t, existsOnFS(t, mem, "ext-m"))

	f := p.Files[0]
	require.Equal(t, "ext-m", f.Path)
	require.Equal(t, []byte("m1"), f.Smallest)
	require.Equal(t, []byte("m2"), f.Largest)
	require.True(t, f.HasPointKeys)
	require.False(t, f.HasRangeKeys)
	require.Equal(t, SeqNum(0), f.SmallestSeqNum)
	require.Equal(t, SeqNum(0), f.LargestSeqNum)
	require.Equal(t, uint64(2), f.NumEntries)
	require.Equal(t, IngestChecksumSHA256, f.ChecksumAlgorithm)
	require.NotEqual(t, [32]byte{}, f.Checksum)
	require.True(t, f.Size > 0)
	require.NotEqual(t, IngestPlanID{}, p.ID)
	require.NotEqual(t, IngestVersionFingerprint{}, p.VersionFingerprint)
	// Empty LSM: the file can fall all the way to the bottom level.
	require.Equal(t, 6, f.TargetLevel)

	// The ID is stable and deterministic. Re-preparing the same files
	// against the same version returns the same plan.
	p2, err := d.PrepareIngestPlan(context.Background(), []string{"ext-m"})
	require.NoError(t, err)
	require.Equal(t, p.ID, p2.ID)
	require.Same(t, p, p2)

	require.NoError(t, d.Close())
}

func TestPrepareIngestPlanValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	// Zero-byte file.
	empty, err := mem.Create("ext-empty", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, empty.Close())
	_, err = d.PrepareIngestPlan(ctx, []string{"ext-empty"})
	require.ErrorContains(t, err, "size 0")

	// Overlapping ranges.
	writePlanTestSST(t, mem, "ext-ac", "a", "c")
	writePlanTestSST(t, mem, "ext-b", "b")
	_, err = d.PrepareIngestPlan(ctx, []string{"ext-ac", "ext-b"})
	require.ErrorContains(t, err, "overlapping ranges")

	// Duplicate path.
	_, err = d.PrepareIngestPlan(ctx, []string{"ext-ac", "ext-ac"})
	require.ErrorContains(t, err, "duplicate file path")

	// Duplicate file contents under different paths.
	require.NoError(t, vfs.Copy(mem, "ext-ac", "ext-ac-copy"))
	_, err = d.PrepareIngestPlan(ctx, []string{"ext-ac", "ext-ac-copy"})
	require.ErrorContains(t, err, "duplicate files")

	// Range deletion conflicting with point keys in another file of the
	// batch.
	writePlanRangeDelSST(t, mem, "ext-del", []string{"a"}, [][2]string{{"c", "e"}})
	writePlanTestSST(t, mem, "ext-d", "d")
	_, err = d.PrepareIngestPlan(ctx, []string{"ext-del", "ext-d"})
	require.ErrorContains(t, err, "conflicts with point keys")

	// Adjacent range deletion (exclusive end) and point key are allowed.
	writePlanRangeDelSST(t, mem, "ext-del-adj", []string{"a"}, [][2]string{{"x", "z"}})
	writePlanTestSST(t, mem, "ext-z", "z")
	_, err = d.PrepareIngestPlan(ctx, []string{"ext-del-adj", "ext-z"})
	require.NoError(t, err)

	// No side effects on failure: no linked tables, sources intact.
	require.Equal(t, 0, countPlanTableFiles(t, mem))
	for _, p := range []string{"ext-empty", "ext-ac", "ext-b", "ext-ac-copy", "ext-del", "ext-d"} {
		require.True(t, existsOnFS(t, mem, p), p)
	}
	require.NoError(t, d.Close())
}

func TestCommitIngestPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	writePlanTestSST(t, mem, "ext-m", "m")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a", "ext-m"})
	require.NoError(t, err)
	for i := range p.Files {
		require.Equal(t, 6, p.Files[i].TargetLevel)
	}

	stats, err := d.CommitIngestPlan(ctx, p)
	require.NoError(t, err)
	require.Equal(t, 2, stats.LevelFiles[6])
	require.Equal(t, 2, totalTables(currentLevelFileCounts(t, d)))
	require.False(t, existsOnFS(t, mem, "ext-a"))
	require.False(t, existsOnFS(t, mem, "ext-m"))

	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("v-a"), val)
	require.NoError(t, closer.Close())
	val, closer, err = d.Get([]byte("m"))
	require.NoError(t, err)
	require.Equal(t, []byte("v-m"), val)
	require.NoError(t, closer.Close())

	// A second plan against the changed version can be prepared and
	// committed.
	writePlanTestSST(t, mem, "ext-z", "z")
	p2, err := d.PrepareIngestPlan(ctx, []string{"ext-z"})
	require.NoError(t, err)
	require.Equal(t, 6, p2.Files[0].TargetLevel)
	require.NotEqual(t, p.VersionFingerprint, p2.VersionFingerprint)
	_, err = d.CommitIngestPlan(ctx, p2)
	require.NoError(t, err)
	require.Equal(t, 3, totalTables(currentLevelFileCounts(t, d)))

	require.NoError(t, d.Close())
}

func TestCommitIngestPlanIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)
	stats, err := d.CommitIngestPlan(ctx, p)
	require.NoError(t, err)
	require.Equal(t, 1, stats.LevelFiles[6])

	// Duplicate commit is a successful no-op.
	stats2, err := d.CommitIngestPlan(ctx, p)
	require.NoError(t, err)
	require.Equal(t, stats, stats2)
	require.Equal(t, 1, totalTables(currentLevelFileCounts(t, d)))

	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("v-a"), val)
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanConcurrentDuplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = d.CommitIngestPlan(ctx, p)
		}(i)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, 1, totalTables(currentLevelFileCounts(t, d)))
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanStale(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)

	// A flush changes the version fingerprint.
	require.NoError(t, d.Set([]byte("q"), []byte("v-q"), nil))
	require.NoError(t, d.Flush())

	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanExpired)

	// Nothing from the batch entered the LSM, linked files were cleaned up
	// and the source file was preserved.
	require.Equal(t, 1, totalTables(currentLevelFileCounts(t, d)))
	require.True(t, existsOnFS(t, mem, "ext-a"))

	// Re-submitting the expired plan fails explicitly.
	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanExpired)

	// Re-preparing against the current version and committing works.
	p2, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)
	require.NotEqual(t, p.ID, p2.ID)
	_, err = d.CommitIngestPlan(ctx, p2)
	require.NoError(t, err)
	require.Equal(t, 2, totalTables(currentLevelFileCounts(t, d)))
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanStaleConcurrentIngest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	writePlanTestSST(t, mem, "ext-z", "z")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)

	// A disjoint regular ingest changes the version between prepare and
	// commit.
	require.NoError(t, d.Ingest(ctx, []string{"ext-z"}))

	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanExpired)
	require.True(t, existsOnFS(t, mem, "ext-a"))
	require.Equal(t, 1, totalTables(currentLevelFileCounts(t, d)))
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanFileModified(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)

	// Replace the source file with different contents.
	writePlanTestSST(t, mem, "ext-a", "a", "different")

	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanFileModified)
	require.Equal(t, 0, totalTables(currentLevelFileCounts(t, d)))

	// The plan is permanently invalid.
	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanFileModified)
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanVerificationRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	opts := planTestOptions(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	writePlanTestSST(t, mem, "ext-m", "m")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a", "ext-m"})
	require.NoError(t, err)

	// Force the commit-time verification gate (invoked after files have been
	// linked but before the manifest is updated) to fail. Open clones the
	// provided Options, so the hook must be installed on d.opts.
	d.opts.private.testingIngestPlanVerifyFunc = func() error {
		return errors.Newf("injected failure")
	}
	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanExpired)
	require.ErrorContains(t, err, "injected failure")

	// Complete rollback: no table from the batch is visible, linked files
	// were removed, sources were preserved.
	require.Equal(t, 0, totalTables(currentLevelFileCounts(t, d)))
	require.Equal(t, 0, countPlanTableFiles(t, mem))
	require.True(t, existsOnFS(t, mem, "ext-a"))
	require.True(t, existsOnFS(t, mem, "ext-m"))

	// The plan is expired; a fresh plan after removing the hook can commit.
	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanExpired)
	d.opts.private.testingIngestPlanVerifyFunc = nil
	p2, err := d.PrepareIngestPlan(ctx, []string{"ext-a", "ext-m"})
	require.NoError(t, err)
	_, err = d.CommitIngestPlan(ctx, p2)
	require.NoError(t, err)
	require.Equal(t, 2, totalTables(currentLevelFileCounts(t, d)))
	require.NoError(t, d.Close())
}

func TestIngestPlanReopen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	opts := planTestOptions(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)
	require.NoError(t, d.Close())

	// Plans are not persisted; committing after reopen yields a clear error.
	d2, err := Open("", opts)
	require.NoError(t, err)
	_, err = d2.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanNotFound)
	require.True(t, existsOnFS(t, mem, "ext-a"))
	require.NoError(t, d2.Close())
}

func TestIngestPlanAtomicVisibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	opts := planTestOptions(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	ctx := context.Background()

	paths := []string{"ext-a", "ext-m", "ext-z"}
	keys := []string{"a", "m", "z"}
	writePlanTestSST(t, mem, paths[0], "a")
	writePlanTestSST(t, mem, paths[1], "m")
	writePlanTestSST(t, mem, paths[2], "z")
	p, err := d.PrepareIngestPlan(ctx, paths)
	require.NoError(t, err)

	// Immediately before the version edit is applied, an iterator must see
	// none of the batch's keys. Open clones Options, so install the hook on
	// d.opts.
	d.opts.private.testingBeforeIngestApplyFunc = func() {
		iter, ierr := d.NewIter(nil)
		require.NoError(t, ierr)
		require.False(t, iter.First(), "iterator observed keys during ingest")
		require.NoError(t, iter.Close())
	}
	_, err = d.CommitIngestPlan(ctx, p)
	require.NoError(t, err)

	// After commit, all keys are visible at once.
	iter, err := d.NewIter(nil)
	require.NoError(t, err)
	var got []string
	for valid := iter.First(); valid; valid = iter.Next() {
		got = append(got, string(iter.Key()))
	}
	require.NoError(t, iter.Close())
	require.Equal(t, keys, got)
	require.NoError(t, d.Close())
}

func TestPrepareIngestPlanFormatIncompatible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	minFmt := d.FormatMajorVersion().MinTableFormat()
	if minFmt <= sstable.TableFormatLevelDB {
		// Nothing older to test on this format version.
		require.NoError(t, d.Close())
		return
	}
	f, err := mem.Create("ext-old", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		TableFormat: sstable.TableFormatLevelDB,
	})
	require.NoError(t, w.Set([]byte("a"), []byte("v-a")))
	require.NoError(t, w.Close())

	_, err = d.PrepareIngestPlan(context.Background(), []string{"ext-old"})
	require.ErrorContains(t, err, "table format")
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanUnknown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	_, err = d.CommitIngestPlan(context.Background(), &IngestPlan{})
	require.ErrorIs(t, err, ErrIngestPlanNotFound)
	require.NoError(t, d.Close())
}

func TestIngestPlanSnapshotVisibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)

	// A snapshot taken before the commit observes the pre-ingest state even
	// after the commit becomes visible.
	snap := d.NewSnapshot()
	_, err = d.CommitIngestPlan(ctx, p)
	require.NoError(t, err)

	_, closer, err := snap.Get([]byte("a"))
	require.ErrorIs(t, err, ErrNotFound)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.NoError(t, snap.Close())

	// A fresh reader observes the post-ingest state.
	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("v-a"), val)
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}

func TestPrepareIngestPlanEmptyTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	// A well-formed but empty (zero entries) sstable.
	f, err := mem.Create("ext-none", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	require.NoError(t, w.Close())
	_, err = d.PrepareIngestPlan(context.Background(), []string{"ext-none"})
	require.ErrorContains(t, err, "empty sstable")
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanTargetL0(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	// Put a key at "a" into the LSM so a new file covering "a" has data
	// overlap and must land at L0.
	require.NoError(t, d.Set([]byte("a"), []byte("old"), nil))
	require.NoError(t, d.Flush())

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)
	require.Equal(t, 0, p.Files[0].TargetLevel)
	stats, err := d.CommitIngestPlan(ctx, p)
	require.NoError(t, err)
	require.Equal(t, 1, stats.LevelFiles[0])

	// The ingested value shadows the flushed one.
	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("v-a"), val)
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanRangeDeletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, d.Set([]byte("y"), []byte("old"), nil))
	require.NoError(t, d.Flush())

	// Planned file with a point key and a range deletion covering "y".
	writePlanRangeDelSST(t, mem, "ext-del", []string{"a"}, [][2]string{{"x", "z"}})
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-del"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), p.Files[0].NumRangeDeletions)
	_, err = d.CommitIngestPlan(ctx, p)
	require.NoError(t, err)

	// The range deletion shadows the flushed point inside the span.
	_, closer, err := d.Get([]byte("y"))
	require.ErrorIs(t, err, ErrNotFound)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("v-a"), val)
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}

func TestCommitIngestPlanMemtableOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	p, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)

	// An unflushed write overlapping the batch forces a flush during commit;
	// the flush changes the version, so the optimistic commit expires.
	require.NoError(t, d.Set([]byte("a"), []byte("live"), nil))
	_, err = d.CommitIngestPlan(ctx, p)
	require.ErrorIs(t, err, ErrIngestPlanExpired)
	require.True(t, existsOnFS(t, mem, "ext-a"))

	// After the flush, a fresh plan can be prepared and committed.
	p2, err := d.PrepareIngestPlan(ctx, []string{"ext-a"})
	require.NoError(t, err)
	require.NotEqual(t, p.ID, p2.ID)
	_, err = d.CommitIngestPlan(ctx, p2)
	require.NoError(t, err)
	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("v-a"), val)
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}

func TestIngestPlanOptionsNotSerialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	opts := planTestOptions(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	// The experimental switch must not leak into the OPTIONS text.
	s := d.opts.String()
	require.NotContains(t, s, "batch_ingest_plans")
	require.NotContains(t, s, "BatchIngestPlans")
	require.NoError(t, d.Close())
}

// TestIngestPlanRegularIngestUnaffected sanity-checks that regular Ingest
// keeps working with the feature enabled (nil verification hook path).
func TestIngestPlanRegularIngestUnaffected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("", planTestOptions(mem))
	require.NoError(t, err)
	ctx := context.Background()

	writePlanTestSST(t, mem, "ext-a", "a")
	require.NoError(t, d.Ingest(ctx, []string{"ext-a"}))
	require.False(t, existsOnFS(t, mem, "ext-a"))
	require.Equal(t, 1, totalTables(currentLevelFileCounts(t, d)))

	// The registry stays empty for regular ingests.
	d.mu.Lock()
	n := len(d.mu.ingestPlans.plans)
	d.mu.Unlock()
	require.Equal(t, 0, n)
	require.NoError(t, d.Close())
}
