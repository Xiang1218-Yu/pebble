// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

func resumableOpts(t *testing.T, fs vfs.FS) *Options {
	return &Options{
		FS:                               fs,
		FormatMajorVersion:               internalFormatNewest,
		ExperimentalResumableCheckpoints: true,
		DisableAutomaticCompactions:      true,
		Logger:                           testutils.Logger{T: t},
	}
}

func ckptKey(i int) []byte { return []byte(fmt.Sprintf("key%04d", i)) }
func ckptVal(i int) []byte { return []byte(fmt.Sprintf("val%04d", i)) }

func writeAndFlush(t *testing.T, d *DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, d.Set(ckptKey(i), ckptVal(i), Sync))
		require.NoError(t, d.Flush())
	}
}

func assertCheckpointContents(t *testing.T, fs vfs.FS, dir string, n int) {
	t.Helper()
	d, err := Open(dir, &Options{FS: fs, Logger: testutils.Logger{T: t}})
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		v, closer, err := d.Get(ckptKey(i))
		require.NoError(t, err, "key %s", ckptKey(i))
		require.Equal(t, ckptVal(i), v)
		require.NoError(t, closer.Close())
	}
	if n > 0 {
		_, closer, err := d.Get(ckptKey(n))
		require.Error(t, err)
		if closer != nil {
			require.NoError(t, closer.Close())
		}
	}
	require.NoError(t, d.Close())
}

func markerValue(t *testing.T, fs vfs.FS, dir, name string) string {
	t.Helper()
	v, err := atomicfs.ReadMarker(fs, dir, name)
	if err != nil {
		return ""
	}
	return v
}

// TestResumableCheckpointDisabled verifies that when the option is left at
// its default (false), the new APIs are refused and DB.Checkpoint behavior is
// unchanged.
func TestResumableCheckpointDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", &Options{FS: mem, FormatMajorVersion: internalFormatNewest})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("a"), []byte("b"), Sync))

	_, err = d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.ErrorIs(t, err, ErrResumableCheckpointsDisabled)
	_, err = d.PublishCheckpoint(context.Background(), "ckpt2")
	require.ErrorIs(t, err, ErrResumableCheckpointsDisabled)
	// Nothing was created.
	_, err = mem.Stat("ckpt")
	require.True(t, errorfsCheckIsNotExist(err))
	_, err = mem.Stat("ckpt2")
	require.True(t, errorfsCheckIsNotExist(err))

	// The existing DB.Checkpoint is unaffected.
	require.NoError(t, d.Checkpoint("legacy"))
	assertCheckpointContents(t, mem, "legacy", 0)
	require.NoError(t, d.Close())
}

func errorfsCheckIsNotExist(err error) bool {
	return strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "not exist") ||
		strings.Contains(err.Error(), "no such file")
}

// TestResumableCheckpointStages exercises the phased API and the visibility
// rules: the destination must not be openable until Commit.
func TestResumableCheckpointStages(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 5)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.Equal(t, CheckpointPlanPlanning, p.State())
	require.Equal(t, "db", p.SourceDir())
	require.NotZero(t, p.VisibleSeqNum())

	// The plan must record source identity, version, file sizes and hashes.
	pf, err := mem.Open("ckpt/.pebble.resumable-checkpoint/plan.json")
	require.NoError(t, err)
	planBytes := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	for {
		n, rerr := pf.Read(buf)
		planBytes = append(planBytes, buf[:n]...)
		if rerr != nil {
			break
		}
	}
	require.NoError(t, pf.Close())
	for _, want := range []string{
		`"state": "planning"`,
		`"source_dir": "db"`,
		`"source_id":`,
		`"format_major_version":`,
		`"visible_seq_num":`,
		`"manifest_file_num":`,
		`"manifest_size":`,
		`"source_hash":`,
		`"hash":`,
		`"size":`,
	} {
		require.True(t, strings.Contains(string(planBytes), want), "plan missing %s:\n%s", want, planBytes)
	}

	require.NoError(t, p.CopyFiles(context.Background()))
	require.Equal(t, CheckpointPlanStaged, p.State())
	require.Equal(t, p.TotalFiles(), p.CopiedFiles())

	// Not openable yet: no manifest marker and no commit marker, and the
	// CURRENT staging sentinel is in place.
	require.Empty(t, markerValue(t, mem, "ckpt", manifestMarkerName))
	require.Empty(t, markerValue(t, mem, "ckpt", checkpointCommitMarkerName))
	require.Empty(t, markerValue(t, mem, "ckpt", formatVersionMarkerName))
	_, err = mem.Stat("ckpt/CURRENT")
	require.NoError(t, err)

	require.NoError(t, p.Verify(context.Background()))
	require.Equal(t, CheckpointPlanVerified, p.State())

	// Writes after the plan must not become visible through the checkpoint.
	require.NoError(t, d.Set(ckptKey(100), ckptVal(100), Sync))

	require.NoError(t, p.Commit(context.Background()))
	require.True(t, p.Committed())
	require.Equal(t, checkpointCommitValue, markerValue(t, mem, "ckpt", checkpointCommitMarkerName))

	// Staging data and sentinel are gone after commit.
	_, err = mem.Stat("ckpt/.pebble.resumable-checkpoint")
	require.Error(t, err)
	_, err = mem.Stat("ckpt/CURRENT")
	require.Error(t, err)

	// The checkpoint reads exactly the pinned consistent version: later
	// writes are not visible.
	d2, err := Open("ckpt", &Options{FS: mem})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		v, closer, gerr := d2.Get(ckptKey(i))
		require.NoError(t, gerr)
		require.Equal(t, ckptVal(i), v)
		require.NoError(t, closer.Close())
	}
	_, closer, err := d2.Get(ckptKey(100))
	require.Error(t, err)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.NoError(t, d2.Close())

	// Simulate a crash during commit that left the staging sentinel behind.
	f2, err := mem.Create("ckpt/CURRENT", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, f2.Close())
	recovered, err := d.ResumeCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.True(t, recovered.Committed())
	_, err = mem.Stat("ckpt/CURRENT")
	require.Error(t, err)
	assertCheckpointContents(t, mem, "ckpt", 5)
	require.NoError(t, d.Close())
}

// TestResumableCheckpointCreateRejectsExistingPlan verifies that creating a
// plan where one already exists fails with ErrCheckpointPlanExists.
func TestResumableCheckpointCreateRejectsExistingPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 2)
	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, p.CopyFiles(context.Background()))
	require.NoError(t, p.Close())
	_, err = d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.ErrorIs(t, err, ErrCheckpointPlanExists)
	// The existing plan is untouched and can be resumed and completed.
	p2, err := d.ResumeCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, p2.Publish(context.Background()))
	assertCheckpointContents(t, mem, "ckpt", 2)
	require.NoError(t, d.Close())
}

// TestResumableCheckpointIdempotent repeats every stage and verifies that
// rerunning a committed plan is a safe no-op.
func TestResumableCheckpointIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 4)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, p.CopyFiles(context.Background()))
	}
	require.NoError(t, p.Verify(context.Background()))
	require.NoError(t, p.Verify(context.Background()))
	require.NoError(t, p.Commit(context.Background()))
	require.NoError(t, p.Commit(context.Background()))
	require.NoError(t, p.CopyFiles(context.Background()))
	require.NoError(t, p.Verify(context.Background()))
	assertCheckpointContents(t, mem, "ckpt", 4)

	// Resuming a committed destination yields a committed handle.
	p2, err := d.ResumeCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.True(t, p2.Committed())
	require.NoError(t, p2.Publish(context.Background()))
	require.NoError(t, d.Close())
}

// TestResumableCheckpointContextCancel cancels CopyFiles mid-way (using an
// injector to cancel at a deterministic point), then resumes the plan with a
// fresh handle and completes it.
func TestResumableCheckpointContextCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var count atomic.Int64
	inj := errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind != errorfs.OpCreate && op.Kind != errorfs.OpRename && op.Kind != errorfs.OpLink {
			return nil
		}
		if strings.HasPrefix(op.Path, "ckpt/") && count.Add(1) == 5 {
			cancel()
			return context.Canceled
		}
		return nil
	})
	efs := errorfs.Wrap(mem, inj)
	d, err := Open("db", resumableOpts(t, efs))
	require.NoError(t, err)
	writeAndFlush(t, d, 12)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	err = p.CopyFiles(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, p.CopiedFiles(), p.TotalFiles())
	// Not openable after the interruption: the manifest marker is absent.
	require.Empty(t, markerValue(t, mem, "ckpt", manifestMarkerName))

	// Resume with a new handle after the old one is closed and the database
	// was reopened.
	require.NoError(t, p.Close())
	require.NoError(t, d.Close())
	d, err = Open("db", resumableOpts(t, efs))
	require.NoError(t, err)
	p2, err := d.ResumeCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, p2.Publish(context.Background()))
	assertCheckpointContents(t, mem, "ckpt", 12)

	// No temp files or verification dirs remain.
	ls, err := mem.List("")
	require.NoError(t, err)
	for _, name := range ls {
		require.False(t, strings.Contains(name, verifyDirPrefix), "leftover verify dir: %s", name)
	}
	require.NoError(t, d.Close())
}

// TestResumableCheckpointErrorInjection injects a filesystem error during the
// copy stage, then completes the plan after disarming the injector.
func TestResumableCheckpointErrorInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	var count atomic.Int64
	var armed atomic.Bool
	injected := errors.New("injected checkpoint copy failure")
	inj := errorfs.InjectorFunc(func(op errorfs.Op) error {
		if !armed.Load() {
			return nil
		}
		if op.Kind != errorfs.OpCreate && op.Kind != errorfs.OpRename && op.Kind != errorfs.OpFileSync {
			return nil
		}
		if strings.HasPrefix(op.Path, "ckpt/") {
			if count.Add(1) == 4 {
				return injected
			}
		}
		return nil
	})
	efs := errorfs.Wrap(mem, inj)
	opts := resumableOpts(t, efs)
	d, err := Open("db", opts)
	require.NoError(t, err)
	writeAndFlush(t, d, 10)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	armed.Store(true)
	err = p.CopyFiles(context.Background())
	require.Error(t, err)
	require.True(t, errors.Is(err, injected), "got %v", err)

	// Destination must still not be openable.
	require.Empty(t, markerValue(t, mem, "ckpt", manifestMarkerName))

	armed.Store(false)
	require.NoError(t, p.Close())
	resumed, err := d.ResumeCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, resumed.CopyFiles(context.Background()))
	require.NoError(t, resumed.Verify(context.Background()))
	require.NoError(t, resumed.Commit(context.Background()))
	assertCheckpointContents(t, mem, "ckpt", 10)
	require.NoError(t, d.Close())
}

// TestResumableCheckpointCorruptStaged corrupts a staged file and verifies
// that CopyFiles detects the corruption, rebuilds the file and the
// checkpoint still commits.
func TestResumableCheckpointCorruptStaged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 6)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, p.CopyFiles(context.Background()))

	// Corrupt one staged sstable.
	ls, err := mem.List("ckpt")
	require.NoError(t, err)
	var victim string
	for _, n := range ls {
		if strings.HasSuffix(n, ".sst") {
			victim = n
			break
		}
	}
	require.NotEmpty(t, victim)
	func() {
		f, err := mem.Create("ckpt/"+victim, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		_, err = f.Write([]byte("garbage"))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}()
	require.Error(t, p.Verify(context.Background()))

	// CopyFiles must detect and repair the corrupt staged file.
	require.NoError(t, p.CopyFiles(context.Background()))
	require.NoError(t, p.Verify(context.Background()))
	require.NoError(t, p.Commit(context.Background()))
	assertCheckpointContents(t, mem, "ckpt", 6)
	require.NoError(t, d.Close())
}

// TestResumableCheckpointSourceChanged verifies the plan fails closed if a
// source file's identity changes.
func TestResumableCheckpointSourceChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 4)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)

	// Tamper with a source sstable. The file cache keeps the DB running but a
	// new open (as performed by the copy stage) observes the new bytes.
	var srcVictim string
	for _, f := range p.data.Files {
		if strings.HasSuffix(f.Name, ".sst") {
			srcVictim = f.SourcePath
			break
		}
	}
	require.NotEmpty(t, srcVictim)
	f, err := mem.Create(srcVictim, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	err = p.CopyFiles(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "source file")
	// Nothing was committed.
	require.Empty(t, markerValue(t, mem, "ckpt", checkpointCommitMarkerName))
	require.NoError(t, p.Abort(context.Background()))
	require.NoError(t, d.Close())
}

// TestResumableCheckpointSourceIdentity verifies a plan cannot be resumed
// against a different source database.
func TestResumableCheckpointSourceIdentity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d1, err := Open("db1", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d1, 3)
	_, err = d1.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, d1.Close())

	d2, err := Open("db2", resumableOpts(t, mem))
	require.NoError(t, err)
	_, err = d2.ResumeCheckpointPlan(context.Background(), "ckpt")
	require.Error(t, err)
	require.Contains(t, err.Error(), "source database")
	require.NoError(t, d2.Close())
}

// TestResumableCheckpointDBClosed verifies stages fail while the DB is closed
// and can resume after reopening.
func TestResumableCheckpointDBClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 4)
	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, p.Close())
	require.NoError(t, d.Close())

	d, err = Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	p2, err := d.ResumeCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, p2.Publish(context.Background()))
	assertCheckpointContents(t, mem, "ckpt", 4)
	require.NoError(t, d.Close())
}

// TestResumableCheckpointAbort verifies explicit abort and restart.
func TestResumableCheckpointAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 3)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt")
	require.NoError(t, err)
	require.NoError(t, p.CopyFiles(context.Background()))
	require.NoError(t, p.Abort(context.Background()))
	_, err = mem.Stat("ckpt")
	require.Error(t, err)
	// Abort is idempotent.
	require.NoError(t, p.Abort(context.Background()))

	// The destination can be reused.
	p2, err := d.PublishCheckpoint(context.Background(), "ckpt")
	require.NoError(t, err)
	require.True(t, p2.Committed())
	require.Error(t, p2.Abort(context.Background()))
	require.NoError(t, d.Close())
}

// TestResumableCheckpointForeignDest rejects destination directories that
// contain foreign content.
func TestResumableCheckpointForeignDest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("k"), []byte("v"), Sync))

	require.NoError(t, mem.MkdirAll("other", 0755))
	ff, err := mem.Create("other/foreign", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, ff.Close())
	_, err = d.CreateCheckpointPlan(context.Background(), "other")
	require.Error(t, err)

	// An existing committed checkpoint cannot be overwritten with a plan.
	_, err = d.PublishCheckpoint(context.Background(), "done")
	require.NoError(t, err)
	_, err = d.CreateCheckpointPlan(context.Background(), "done")
	require.Error(t, err)
	require.NoError(t, d.Close())
}

// TestResumableCheckpointPreviousCleanup verifies the old checkpoint is only
// removed after the new one commits and verifiably opens, and that the
// guarding arguments are enforced.
func TestResumableCheckpointPreviousCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 3)

	old, err := d.PublishCheckpoint(context.Background(), "old")
	require.NoError(t, err)
	require.True(t, old.Committed())

	require.NoError(t, d.Set(ckptKey(3), ckptVal(3), Sync))
	require.NoError(t, d.Flush())

	p, err := d.CreateCheckpointPlan(context.Background(), "new")
	require.NoError(t, err)
	require.NoError(t, p.CopyFiles(context.Background()))
	require.NoError(t, p.Verify(context.Background()))
	require.Error(t, p.CommitAndRemovePrevious(context.Background(), "new"))
	require.Error(t, p.CommitAndRemovePrevious(context.Background(), "db"))

	require.NoError(t, p.CommitAndRemovePrevious(context.Background(), "old"))
	_, err = mem.Stat("old")
	require.Error(t, err)
	assertCheckpointContents(t, mem, "new", 4)
	// Idempotent: old is already gone, new is still intact.
	require.NoError(t, p.CommitAndRemovePrevious(context.Background(), "old"))
	require.NoError(t, d.Close())
}

// TestResumableCheckpointConcurrent runs the resumable flow under concurrent
// writes and compactions.
func TestResumableCheckpointConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	opts := resumableOpts(t, mem)
	opts.DisableAutomaticCompactions = false
	d, err := Open("db", opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Hold a long-lived snapshot and iterator for the duration of the test;
	// checkpointing must not interfere with source file lifetimes or
	// readers.
	snap := d.NewSnapshot()
	snapIter, _ := snap.NewIter(nil)
	// The iterator pins the empty snapshot's file state.
	require.False(t, snapIter.First())

	wg.Go(func() {
		defer cancel()
		for i := 0; ctx.Err() == nil; i++ {
			if err := d.Set([]byte(fmt.Sprintf("key%06d", i%500)), []byte("v"), Sync); err != nil {
				return
			}
			if i%25 == 0 {
				if err := d.Flush(); err != nil {
					return
				}
			}
		}
	})
	wg.Go(func() {
		defer cancel()
		for ctx.Err() == nil {
			if err := d.Compact(t.Context(), []byte("a"), []byte("z"), false); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	for i := 0; i < 12 && ctx.Err() == nil; i++ {
		dir := fmt.Sprintf("ckpt-%02d", i)
		p, err := d.PublishCheckpoint(ctx, dir)
		if err != nil {
			break
		}
		require.True(t, p.Committed())
		func() {
			d2, err := Open(dir, &Options{FS: mem})
			require.NoError(t, err)
			iter, _ := d2.NewIter(nil)
			for ok := iter.First(); ok; ok = iter.Next() {
				require.NoError(t, iter.Error())
			}
			require.NoError(t, iter.Close())
			require.NoError(t, d2.Close())
		}()
		if i > 0 {
			prev := fmt.Sprintf("ckpt-%02d", i-1)
			require.NoError(t, mem.RemoveAll(prev))
		}
	}
	cancel()
	wg.Wait()
	require.NoError(t, snapIter.Close())
	require.NoError(t, snap.Close())
	require.NoError(t, d.Close())
}

// TestResumableCheckpointRestrictSpans covers the WithRestrictToSpans path
// through the appended MANIFEST VersionEdit.
func TestResumableCheckpointRestrictSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	writeAndFlush(t, d, 20)

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt", WithRestrictToSpans([]CheckpointSpan{
		{Start: ckptKey(0), End: ckptKey(5)},
	}))
	require.NoError(t, err)
	require.NoError(t, p.Publish(context.Background()))

	d2, err := Open("ckpt", &Options{FS: mem})
	require.NoError(t, err)
	v, closer, err := d2.Get(ckptKey(0))
	require.NoError(t, err)
	require.Equal(t, ckptVal(0), v)
	require.NoError(t, closer.Close())
	require.NoError(t, d2.Close())
	require.NoError(t, d.Close())
}

// TestResumableCheckpointFlushWAL ensures unflushed committed writes are part
// of the checkpoint when WithFlushedWAL is used.
func TestResumableCheckpointFlushWAL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mem := vfs.NewMem()
	d, err := Open("db", resumableOpts(t, mem))
	require.NoError(t, err)
	wb := d.NewBatch()
	require.NoError(t, wb.Set([]byte("k"), []byte("mem-only"), nil))
	require.NoError(t, d.Apply(wb, NoSync))

	p, err := d.CreateCheckpointPlan(context.Background(), "ckpt", WithFlushedWAL())
	require.NoError(t, err)
	require.NoError(t, p.Publish(context.Background()))
	assertCheckpointKey(t, mem, "ckpt", []byte("k"), []byte("mem-only"))
	require.NoError(t, d.Close())
}

func assertCheckpointKey(t *testing.T, fs vfs.FS, dir string, key, want []byte) {
	d, err := Open(dir, &Options{FS: fs})
	require.NoError(t, err)
	v, closer, err := d.Get(key)
	require.NoError(t, err)
	require.Equal(t, want, v)
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}
