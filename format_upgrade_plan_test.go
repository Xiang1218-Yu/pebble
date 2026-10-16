// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/stretchr/testify/require"
)

const upgradeTestDir = ""

func upgradeTestOpts(fs vfs.FS, enablePlan bool) *Options {
	opts := &Options{
		FS:                      fs,
		FormatMajorVersion:      FormatMinSupported,
		EnableFormatUpgradePlan: enablePlan,
	}
	opts.WithFSDefaults()
	return opts
}

func openUpgradeDB(t *testing.T, fs vfs.FS, enablePlan bool) *DB {
	t.Helper()
	d, err := Open(upgradeTestDir, upgradeTestOpts(fs, enablePlan))
	require.NoError(t, err)
	require.Equal(t, FormatMinSupported, d.FormatMajorVersion())
	return d
}

func TestFormatUpgradePlanDisabledByDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	opts := &Options{FS: fs}
	opts.WithFSDefaults()
	d, err := Open(upgradeTestDir, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	// With the capability disabled, writes, flushes and ratcheting keep
	// their historical semantics.
	require.NoError(t, d.Set([]byte("a"), []byte("v"), Sync))
	require.NoError(t, d.Flush())
	require.NoError(t, d.RatchetFormatMajorVersion(FormatWALSyncChunks))
	require.Equal(t, FormatWALSyncChunks, d.FormatMajorVersion())

	_, err = d.NewFormatUpgradePlan(FormatNewest)
	require.ErrorIs(t, err, ErrFormatUpgradeDisabled)
	require.ErrorIs(t, d.ExecuteFormatUpgradePlan(context.Background(), nil), ErrFormatUpgradeDisabled)
}

func TestFormatUpgradePlanContentsAndEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()
	require.NoError(t, d.Set([]byte("k"), []byte("v"), Sync))
	require.NoError(t, d.Flush())

	p, err := d.NewFormatUpgradePlan(FormatWALSyncChunks)
	require.NoError(t, err)
	require.NotEmpty(t, p.PlanID)
	require.Equal(t, FormatMinSupported, p.FromVersion)
	require.Equal(t, FormatWALSyncChunks, p.ToVersion)
	require.Equal(t, int(FormatWALSyncChunks-FormatMinSupported), len(p.Steps))
	require.Equal(t, 0, p.IrreversibleStage)
	for i, s := range p.Steps {
		require.Equal(t, p.FromVersion+FormatMajorVersion(i), s.FromVersion)
		require.Equal(t, s.FromVersion+1, s.ToVersion)
		require.True(t, s.Irreversible)
		require.NotEmpty(t, s.Description)
	}
	require.NotEmpty(t, p.ComparerName)
	require.NotEmpty(t, p.OptionsFingerprint)
	require.NotEmpty(t, p.FileSet.Fingerprint)
	require.GreaterOrEqual(t, len(p.FileSet.Tables), 1)
	require.False(t, p.WALState.SyncChunks)

	// The affected-files list must cover the recovery marker, the format
	// version marker and the MANIFEST.
	var sawRecoveryMarker, sawVersionMarker, sawManifest bool
	for _, f := range p.Files {
		switch f.FileType {
		case "recovery-marker":
			sawRecoveryMarker = true
		case "format-version-marker":
			sawVersionMarker = true
		case "manifest":
			sawManifest = true
		}
	}
	require.True(t, sawRecoveryMarker && sawVersionMarker && sawManifest)
	require.NotEmpty(t, p.Prerequisites)

	// Plans are immutable values with stable digests.
	digest := p.Digest()
	require.Len(t, digest, 32)
	p2, err := d.NewFormatUpgradePlan(FormatWALSyncChunks)
	require.NoError(t, err)
	require.Equal(t, digest, p2.Digest())

	data, err := p.Encode()
	require.NoError(t, err)
	decoded, err := DecodeFormatUpgradePlan(data)
	require.NoError(t, err)
	require.Equal(t, digest, decoded.Digest())
	require.Equal(t, p.Steps, decoded.Steps)
	require.Equal(t, p.FromVersion, decoded.FromVersion)
	require.Equal(t, p.ToVersion, decoded.ToVersion)
	require.Equal(t, p.FileSet.Fingerprint, decoded.FileSet.Fingerprint)
	require.Len(t, decoded.FileSet.Tables, len(p.FileSet.Tables))

	// Tampering with the encoding is detected.
	bad := append([]byte(nil), data...)
	bad[len(bad)-20] ^= 0xff
	_, err = DecodeFormatUpgradePlan(bad)
	require.ErrorIs(t, err, ErrFormatUpgradeInvalidPlan)

	// A zero-step plan (target == current version).
	same, err := d.NewFormatUpgradePlan(FormatMinSupported)
	require.NoError(t, err)
	require.Empty(t, same.Steps)
	require.Equal(t, -1, same.IrreversibleStage)
}

func TestFormatUpgradePlanExecute(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	require.NoError(t, d.Set([]byte("a"), []byte("1"), Sync))
	require.NoError(t, d.Set([]byte("b"), []byte("2"), Sync))

	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)

	st := d.FormatUpgradePlanState()
	require.False(t, st.Active)

	require.NoError(t, d.ExecuteFormatUpgradePlan(context.Background(), p))
	require.Equal(t, FormatNewest, d.FormatMajorVersion())

	// The recovery marker files must have been removed.
	ls, err := fs.List(upgradeTestDir)
	require.NoError(t, err)
	for _, f := range ls {
		require.False(t, strings.HasPrefix(f, "marker."+formatUpgradePlanMarkerName),
			"recovery marker file %s was not cleaned up", f)
	}

	// Data remains readable and writes work at the new version.
	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, "1", string(val))
	require.NoError(t, closer.Close())
	require.NoError(t, d.Set([]byte("c"), []byte("3"), Sync))
	require.NoError(t, d.Flush())

	// Idempotent resubmission.
	require.NoError(t, d.ExecuteFormatUpgradePlan(context.Background(), p))
	require.Equal(t, FormatNewest, d.FormatMajorVersion())

	require.NoError(t, d.Close())

	// Reopen and confirm the upgrade is durable.
	d2, err := Open(upgradeTestDir, upgradeTestOpts(fs, true))
	require.NoError(t, err)
	defer func() { require.NoError(t, d2.Close()) }()
	require.Equal(t, FormatNewest, d2.FormatMajorVersion())
	require.False(t, d2.FormatUpgradePlanState().Pending)
}

func TestFormatUpgradePlanStaleFileSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()

	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("k"), []byte("v"), Sync))
	require.NoError(t, d.Flush())

	err = d.ExecuteFormatUpgradePlan(context.Background(), p)
	require.ErrorIs(t, err, ErrFormatUpgradeFileSetMismatch)
	// The failed validation must not have created a recovery marker.
	st := d.FormatUpgradePlanState()
	require.False(t, st.Active)
}

func TestFormatUpgradePlanMismatchedInputs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()

	cases := []struct {
		name   string
		mutate func(p *FormatUpgradePlan)
		want   *FormatUpgradeError
	}{
		{"identity", func(p *FormatUpgradePlan) { p.DBIdentity = "/somewhere/else" }, ErrFormatUpgradeIdentityMismatch},
		{"stale version", func(p *FormatUpgradePlan) { p.FromVersion-- }, ErrFormatUpgradePlanStale},
		{"comparer", func(p *FormatUpgradePlan) { p.ComparerName = "other" }, ErrFormatUpgradeComparerMismatch},
		{"merger", func(p *FormatUpgradePlan) { p.MergerName = "other-merger" }, ErrFormatUpgradeMergerMismatch},
		{"keyschema", func(p *FormatUpgradePlan) { p.KeySchemaName = "other-schema" }, ErrFormatUpgradeKeySchemaMismatch},
		{"options fingerprint", func(p *FormatUpgradePlan) { p.OptionsFingerprint = "deadbeef" }, ErrFormatUpgradeOptionsFingerprintMatch},
		{"file set fingerprint", func(p *FormatUpgradePlan) { p.FileSet.Fingerprint = "deadbeef" }, ErrFormatUpgradeFileSetMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := d.NewFormatUpgradePlan(FormatWALSyncChunks)
			require.NoError(t, err)
			tc.mutate(p)
			err = d.ExecuteFormatUpgradePlan(context.Background(), p)
			require.ErrorIs(t, err, tc.want)
			var fue *FormatUpgradeError
			require.True(t, errors.As(err, &fue))
			require.NotEmpty(t, fue.Error())
		})
	}
}

func TestFormatUpgradePlanUnsupportedTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()
	_, err := d.NewFormatUpgradePlan(internalFormatNewest + 1)
	require.ErrorIs(t, err, ErrFormatUpgradeUnsupportedTarget)
}

func TestFormatUpgradePlanInProcessFailureAndResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()
	require.NoError(t, d.Set([]byte("k"), []byte("v"), Sync))
	require.NoError(t, d.Flush())

	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)

	// Fail the first stage after its prepared marker is written but before
	// the migration runs.
	d.formatUpgradeTestBeforeStage = func(v FormatMajorVersion) error {
		return errors.Newf("injected failure")
	}
	err = d.ExecuteFormatUpgradePlan(context.Background(), p)
	require.Error(t, err)
	require.Contains(t, err.Error(), "injected failure")
	require.Equal(t, FormatMinSupported, d.FormatMajorVersion())

	st := d.FormatUpgradePlanState()
	require.True(t, st.Active)
	require.True(t, st.Pending)
	require.Equal(t, p.Digest(), st.PlanDigest)
	require.Equal(t, 0, st.NextStage)

	// All mutating operations are blocked while pending.
	require.ErrorIs(t, d.Set([]byte("x"), []byte("y"), Sync), ErrFormatUpgradeInProgress)
	_, err = d.AsyncFlush()
	require.ErrorIs(t, err, ErrFormatUpgradeInProgress)
	require.ErrorIs(t, d.Compact(context.Background(), []byte("a"), []byte("z"), false),
		ErrFormatUpgradeInProgress)
	require.ErrorIs(t, d.Checkpoint("/ckpt"), ErrFormatUpgradeInProgress)
	require.ErrorIs(t, d.Ingest(context.Background(), []string{"/nonexistent.sst"}),
		ErrFormatUpgradeInProgress)
	require.ErrorIs(t, d.RatchetFormatMajorVersion(FormatNewest), ErrFormatUpgradeInProgress)

	// Reads and snapshots follow the version rule and remain available.
	snap := d.NewSnapshot()
	defer snap.Close()
	_, closer, err := snap.Get([]byte("k"))
	if err == nil {
		require.NoError(t, closer.Close())
	}
	iter, _ := d.NewIter(nil)
	require.NoError(t, iter.Close())

	// Resubmitting the same plan resumes and completes.
	d.formatUpgradeTestBeforeStage = nil
	require.NoError(t, d.ExecuteFormatUpgradePlan(context.Background(), p))
	require.Equal(t, FormatNewest, d.FormatMajorVersion())
	require.False(t, d.FormatUpgradePlanState().Active)
	require.NoError(t, d.Set([]byte("x"), []byte("y"), Sync))
}

func TestFormatUpgradePlanAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()

	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	d.formatUpgradeTestBeforeStage = func(v FormatMajorVersion) error {
		return errors.Newf("injected failure")
	}
	require.Error(t, d.ExecuteFormatUpgradePlan(context.Background(), p))

	// A different plan cannot abort.
	require.ErrorIs(t, d.AbortFormatUpgradePlan(context.Background(), &FormatUpgradePlan{
		PlanID: "other", FromVersion: FormatMinSupported, ToVersion: FormatNewest,
	}), ErrFormatUpgradePlanMismatch)

	// Abort before the irreversible commit unblocks the database.
	require.NoError(t, d.AbortFormatUpgradePlan(context.Background(), p))
	require.False(t, d.FormatUpgradePlanState().Active)
	require.Equal(t, FormatMinSupported, d.FormatMajorVersion())
	require.NoError(t, d.Set([]byte("k"), []byte("v"), Sync))

	ls, err := fs.List(upgradeTestDir)
	require.NoError(t, err)
	for _, f := range ls {
		require.False(t, strings.HasPrefix(f, "marker."+formatUpgradePlanMarkerName))
	}

	// Abort with nothing in progress is an idempotent no-op.
	require.NoError(t, d.AbortFormatUpgradePlan(context.Background(), p))
}

func TestFormatUpgradePlanCannotAbortAfterCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()

	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)

	// Fail immediately after the first stage's commit point is durable,
	// before the committed marker is written.
	d.formatUpgradeTestAfterStage = func(v FormatMajorVersion) error {
		if v == FormatMinSupported+1 {
			return errors.Newf("injected crash after commit")
		}
		return nil
	}
	err = d.ExecuteFormatUpgradePlan(context.Background(), p)
	require.Error(t, err)
	require.Equal(t, FormatMinSupported+1, d.FormatMajorVersion())

	require.ErrorIs(t, d.AbortFormatUpgradePlan(context.Background(), p),
		ErrFormatUpgradeCannotAbort)
}

func TestFormatUpgradePlanCrashRecoveryResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()

	// Phase 1: fail before the first migration runs, then close.
	d := openUpgradeDB(t, fs, true)
	require.NoError(t, d.Set([]byte("k"), []byte("v"), Sync))
	require.NoError(t, d.Flush())
	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	data, err := p.Encode()
	require.NoError(t, err)
	d.formatUpgradeTestBeforeStage = func(v FormatMajorVersion) error {
		return errors.Newf("injected failure")
	}
	require.Error(t, d.ExecuteFormatUpgradePlan(context.Background(), p))
	require.NoError(t, d.Close())

	// Reopening with the capability disabled must safely reject.
	_, err = Open(upgradeTestDir, upgradeTestOpts(fs, false))
	require.ErrorIs(t, err, ErrFormatUpgradeRecoveryRequired)
	require.Contains(t, err.Error(), p.Digest())

	// Read-only opens must also reject.
	ro := upgradeTestOpts(fs, true)
	ro.ReadOnly = true
	_, err = Open(upgradeTestDir, ro)
	require.ErrorIs(t, err, ErrFormatUpgradeRecoveryRequired)

	// Reopening enabled enters the pending state.
	d2, err := Open(upgradeTestDir, upgradeTestOpts(fs, true))
	require.NoError(t, err)
	st := d2.FormatUpgradePlanState()
	require.True(t, st.Pending)
	require.Equal(t, FormatMinSupported, st.CurrentVersion)
	require.Equal(t, 0, st.NextStage)

	// Writes and plan generation remain blocked after reopen.
	require.ErrorIs(t, d2.Set([]byte("x"), []byte("y"), Sync), ErrFormatUpgradeInProgress)
	_, err = d2.NewFormatUpgradePlan(FormatNewest)
	require.ErrorIs(t, err, ErrFormatUpgradeInProgress)

	// Decode the original plan and resubmit it.
	decoded, err := DecodeFormatUpgradePlan(data)
	require.NoError(t, err)
	require.NoError(t, d2.ExecuteFormatUpgradePlan(context.Background(), decoded))
	require.Equal(t, FormatNewest, d2.FormatMajorVersion())
	require.NoError(t, d2.Set([]byte("z"), []byte("1"), Sync))
	require.NoError(t, d2.Close())

	// Final reopen: no pending state, marker cleaned up.
	d3, err := Open(upgradeTestDir, upgradeTestOpts(fs, false))
	require.NoError(t, err)
	require.Equal(t, FormatNewest, d3.FormatMajorVersion())
	require.NoError(t, d3.Close())
}

func TestFormatUpgradePlanCrashAfterCommitPoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()

	d := openUpgradeDB(t, fs, true)
	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	data, err := p.Encode()
	require.NoError(t, err)
	d.formatUpgradeTestAfterStage = func(v FormatMajorVersion) error {
		if v == FormatMinSupported+1 {
			return errors.Newf("injected crash after commit")
		}
		return nil
	}
	require.Error(t, d.ExecuteFormatUpgradePlan(context.Background(), p))
	require.NoError(t, d.Close())

	// Reopen: the first stage must be recognized as already committed and
	// skipped.
	d2, err := Open(upgradeTestDir, upgradeTestOpts(fs, true))
	require.NoError(t, err)
	st := d2.FormatUpgradePlanState()
	require.True(t, st.Pending)
	require.Equal(t, FormatMinSupported+1, st.CurrentVersion)
	require.Equal(t, 1, st.NextStage)

	decoded, err := DecodeFormatUpgradePlan(data)
	require.NoError(t, err)
	require.NoError(t, d2.ExecuteFormatUpgradePlan(context.Background(), decoded))
	require.Equal(t, FormatNewest, d2.FormatMajorVersion())
	require.NoError(t, d2.Close())
}

func TestFormatUpgradePlanCorruptRecoveryMarker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()

	d := openUpgradeDB(t, fs, true)
	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	d.formatUpgradeTestBeforeStage = func(v FormatMajorVersion) error {
		return errors.Newf("injected failure")
	}
	require.Error(t, d.ExecuteFormatUpgradePlan(context.Background(), p))
	require.NoError(t, d.Close())

	// Replace the marker with a high-iteration marker carrying a bad
	// checksum. Marker state is encoded in the filename.
	ls, err := fs.List(upgradeTestDir)
	require.NoError(t, err)
	for _, f := range ls {
		if strings.HasPrefix(f, "marker."+formatUpgradePlanMarkerName) {
			require.NoError(t, fs.Remove(fs.PathJoin(upgradeTestDir, f)))
		}
	}
	badName := fmt.Sprintf("marker.%s.%06d.%s", formatUpgradePlanMarkerName, 99,
		"v1.0123456789abcdef0123456789abcdef.13.30.17.0.p.deadbeef")
	f, err := fs.Create(badName, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Open(upgradeTestDir, upgradeTestOpts(fs, true))
	require.ErrorIs(t, err, ErrFormatUpgradeCorruptRecoveryMarker)
	// Even disabling the feature does not silently open the database.
	_, err = Open(upgradeTestDir, upgradeTestOpts(fs, false))
	require.ErrorIs(t, err, ErrFormatUpgradeCorruptRecoveryMarker)
}

func TestFormatUpgradePlanDoneMarkerCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()

	d := openUpgradeDB(t, fs, true)
	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	require.NoError(t, d.ExecuteFormatUpgradePlan(context.Background(), p))
	require.NoError(t, d.Close())

	// Simulate a crash after the terminal done marker was written but before
	// the marker files were removed: place a well-formed done marker and
	// confirm a reopen cleans it up without entering the pending state.
	m, _, err := atomicfs.LocateMarker(fs, upgradeTestDir, formatUpgradePlanMarkerName)
	require.NoError(t, err)
	rec := recoveryMarkerValue{
		digest: p.Digest(), from: p.FromVersion, to: p.ToVersion,
		numStages: len(p.Steps), stage: len(p.Steps), state: recoveryStateDone,
	}
	require.NoError(t, m.Move(encodeFormatUpgradeRecoveryMarker(rec)))
	require.NoError(t, m.Close())

	d2, err := Open(upgradeTestDir, upgradeTestOpts(fs, false))
	require.NoError(t, err)
	require.False(t, d2.FormatUpgradePlanState().Pending)
	require.Equal(t, FormatNewest, d2.FormatMajorVersion())
	require.NoError(t, d2.Close())

	ls, err := fs.List(upgradeTestDir)
	require.NoError(t, err)
	for _, name := range ls {
		require.False(t, strings.HasPrefix(name, "marker."+formatUpgradePlanMarkerName))
	}
}

func TestFormatUpgradePlanIncompleteFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()
	require.NoError(t, d.Set([]byte("k"), []byte("v"), Sync))
	require.NoError(t, d.Flush())

	// Find the live SSTable on the memfs and truncate it to zero bytes,
	// bypassing the object provider bookkeeping.
	ls, err := fs.List(upgradeTestDir)
	require.NoError(t, err)
	var sst string
	for _, f := range ls {
		if strings.HasPrefix(f, "00") && strings.HasSuffix(f, ".sst") {
			sst = f
		}
	}
	require.NotEmpty(t, sst)
	path := fs.PathJoin(upgradeTestDir, sst)
	require.NoError(t, fs.Remove(path))
	zf, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	require.NoError(t, zf.Close())

	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	err = d.ExecuteFormatUpgradePlan(context.Background(), p)
	require.ErrorIs(t, err, ErrFormatUpgradeIncompleteFiles)
}

func TestFormatUpgradePlanContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.NewMem()
	d := openUpgradeDB(t, fs, true)
	defer func() { require.NoError(t, d.Close()) }()
	p, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = d.ExecuteFormatUpgradePlan(ctx, p)
	require.ErrorIs(t, err, context.Canceled)
	// Cancellation before any stage begins leaves no recovery marker, so the
	// database is not blocked.
	require.False(t, d.FormatUpgradePlanState().Active)
	require.NoError(t, d.Set([]byte("after-cancel"), []byte("v"), Sync))
	// The plan can still be executed (its file-set validation re-checks the
	// current state; regenerate since a write just occurred).
	p2, err := d.NewFormatUpgradePlan(FormatNewest)
	require.NoError(t, err)
	require.NoError(t, d.ExecuteFormatUpgradePlan(context.Background(), p2))
	require.Equal(t, FormatNewest, d.FormatMajorVersion())
}

func TestFormatUpgradeRecoveryMarkerCodec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := recoveryMarkerValue{
		digest: "0123456789abcdef0123456789abcdef",
		from:   FormatMinSupported, to: FormatNewest,
		numStages: int(FormatNewest - FormatMinSupported),
		stage:     3, state: recoveryStatePrepared,
	}
	encoded := encodeFormatUpgradeRecoveryMarker(r)
	decoded, err := parseFormatUpgradeRecoveryMarker(encoded)
	require.NoError(t, err)
	require.Equal(t, r, decoded)

	for _, bad := range []string{
		"v1.bad",
		"v1.0123456789abcdef0123456789abcdef.13.30.17.0.p.deadbeef",
		"v2.0123456789abcdef0123456789abcdef.13.30.17.0.p.00000000",
	} {
		_, err := parseFormatUpgradeRecoveryMarker(bad)
		require.Error(t, err, bad)
	}
}
