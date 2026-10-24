// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/cockroachdb/pebble/wal"
)

// resumable checkpoint staging layout (all within the destination directory):
//
//	<destDir>/<stagingDirName>/plan.json       immutable plan + progress
//	<destDir>/<stagingDirName>/tmp/...         temporary, never linked into place
//	<destDir>/<regular db file names>          staged OPTIONS/MANIFEST/sstables/...
//
// The manifest marker (and the checkpoint-commit marker) are only created at
// commit time, so an interrupted or failed checkpoint can never be opened.
// Verification before commit is performed against an isolated temporary
// directory populated from the staged files.
const (
	resumableStagingDirName = ".pebble.resumable-checkpoint"
	resumablePlanFileName   = "plan.json"
	resumableTmpDirName     = "tmp"

	// checkpointCommitMarkerName is the atomic marker whose presence denotes a
	// fully committed, openable checkpoint.
	checkpointCommitMarkerName = "checkpoint-commit"
	checkpointCommitValue      = "committed"

	// Mirrors the unexported constants in
	// objstorage/objstorageprovider/remoteobjcat.
	remoteCatalogMarkerName   = "remote-obj-catalog"
	remoteCatalogFilenameBase = "REMOTE-OBJ-CATALOG"

	resumablePlanVersion = 1

	verifyDirPrefix = ".pebble-verify-"

	// stagingSentinelName is a legacy v1 "CURRENT" file placed in the
	// destination while a checkpoint is being built. Modern Pebble refuses to
	// open any directory containing a CURRENT file without a format-version
	// marker, so its presence makes it impossible to open a half-finished
	// checkpoint. It is removed only after the commit markers are durable.
	stagingSentinelName    = "CURRENT"
	stagingSentinelContent = "pebble resumable checkpoint staging area; not openable until committed\n"
)

// Checkpoint plan states.
const (
	// CheckpointPlanPlanning: the plan was created but no files have been
	// copied yet.
	CheckpointPlanPlanning = "planning"
	// CheckpointPlanCopying: some files may have been copied; the checkpoint
	// is not complete.
	CheckpointPlanCopying = "copying"
	// CheckpointPlanStaged: every planned file was copied and verified
	// against the plan.
	CheckpointPlanStaged = "staged"
	// CheckpointPlanVerified: the staged files were validated as a consistent
	// openable checkpoint, but the commit marker has not been created.
	CheckpointPlanVerified = "verified"
	// CheckpointPlanCommitted: the checkpoint was committed and may be
	// opened.
	CheckpointPlanCommitted = "committed"
)

// File kinds recorded in a checkpoint plan.
const (
	planFileKindOptions  = "options"
	planFileKindManifest = "manifest"
	planFileKindTable    = "table"
	planFileKindWAL      = "wal"
	planFileKindCatalog  = "remote-catalog"
)

// ErrResumableCheckpointsDisabled is returned by the resumable checkpoint
// APIs when Options.ExperimentalResumableCheckpoints is false (the default).
var ErrResumableCheckpointsDisabled = errors.New(
	"pebble: resumable checkpoint publishing is disabled; set Options.ExperimentalResumableCheckpoints to enable it")

// ErrCheckpointPlanExists is returned by CreateCheckpointPlan when the
// destination directory already contains an uncommitted checkpoint plan for
// the same or a different database. ResumeCheckpointPlan should be used to
// continue an existing plan.
var ErrCheckpointPlanExists = errors.New(
	"pebble: a checkpoint plan already exists in the destination directory; use ResumeCheckpointPlan")

// resumablePlanFile is a single file that belongs to a checkpoint.
type resumablePlanFile struct {
	// Name is the file's base name within the destination directory.
	Name string `json:"name"`
	// Kind is one of the planFileKind* constants.
	Kind string `json:"kind"`
	// FileNum is the file number (meaningful for table, manifest, options and
	// wal files).
	FileNum uint64 `json:"file_num,omitempty"`
	// SourcePath is the location of the file in the source database's VFS. It
	// is empty for files that are produced by a transform (wal files) or
	// generated remotely (catalog files).
	SourcePath string `json:"source_path,omitempty"`
	// SourceSize and SourceHash identify the source content (or, for the
	// MANIFEST, the source prefix of size SourceSize that is copied). A resume
	// aborts if the source changes.
	SourceSize int64  `json:"source_size,omitempty"`
	SourceHash uint64 `json:"source_hash,omitempty"`
	// Size and Hash are the expected size and xxhash64 checksum of the staged
	// destination file.
	Size int64  `json:"size,omitempty"`
	Hash uint64 `json:"hash,omitempty"`
	// Copied is true once the file was materialized at Name and matched Size
	// and Hash.
	Copied bool `json:"copied,omitempty"`
}

// resumablePlanData is the persisted plan and progress record.
type resumablePlanData struct {
	// PlanVersion is the on-disk format version of this record.
	PlanVersion int `json:"plan_version"`
	// State is one of the CheckpointPlan* constants.
	State string `json:"state"`

	// SourceDir is the source database directory (VFS relative).
	SourceDir string `json:"source_dir"`
	// SourceID identifies the source database the plan was created from.
	SourceID string `json:"source_id"`
	// SourceOptionsFileNum is the OPTIONS file that was present when the plan
	// was created.
	SourceOptionsFileNum uint64 `json:"source_options_file_num"`

	// FormatMajorVersion pins the format major version of the consistent
	// version this checkpoint represents.
	FormatMajorVersion int `json:"format_major_version"`
	// VisibleSeqNum is the visible sequence number of the consistent version.
	VisibleSeqNum uint64 `json:"visible_seq_num"`

	// ManifestFileNum and ManifestSize pin the MANIFEST file and the prefix
	// that reflects the consistent version.
	ManifestFileNum uint64 `json:"manifest_file_num"`
	ManifestSize    int64  `json:"manifest_size"`

	// FlushWAL records the WithFlushedWAL option (applied at plan creation).
	FlushWAL bool `json:"flush_wal,omitempty"`

	// Files contains every regular file belonging to the checkpoint.
	Files []resumablePlanFile `json:"files"`
	// WALNums are the logical WAL numbers that are copied and truncated at
	// VisibleSeqNum.
	WALNums []uint64 `json:"wal_nums,omitempty"`
	// RemoteFileNums are remote objects referenced through the remote object
	// catalog.
	RemoteFileNums []uint64 `json:"remote_file_nums,omitempty"`
	// ExcludedVersionEdit is the encoded manifest.VersionEdit payload that is
	// appended to the checkpoint's MANIFEST when WithRestrictToSpans excludes
	// files.
	ExcludedVersionEdit []byte `json:"excluded_version_edit,omitempty"`

	// FormatMarkerCreated is true once the format-version marker was created
	// in the destination directory.
	FormatMarkerCreated bool `json:"format_marker_created,omitempty"`

	// CatalogFileNames lists remote object catalog files in the destination
	// (excluding catalog marker files).
	CatalogFileNames []string `json:"catalog_file_names,omitempty"`
}

// CheckpointPlan is a handle on an in-progress resumable checkpoint. The zero
// value is not usable; instances are returned by DB.CreateCheckpointPlan or
// DB.ResumeCheckpointPlan.
//
// A plan pins a single consistent version of the source database. Files are
// copied with CopyFiles (which is idempotent and may be resumed after
// interruptions, context cancellation or errors), verified with Verify and
// published atomically with Commit. A checkpoint cannot be opened until
// Commit succeeds.
//
// CheckpointPlan is not safe for concurrent use by multiple goroutines, but
// concurrent database writes, flushes, compactions, snapshots and iterators
// do not affect the plan's correctness.
type CheckpointPlan struct {
	d    *DB
	fs   vfs.FS // syncing wrapper around d.opts.FS
	base vfs.FS

	destDir    string
	stagingDir string
	tmpDir     string
	planPath   string

	mu     sync.Mutex
	data   resumablePlanData
	closed bool
}

// State returns the plan's current state (one of the CheckpointPlan*
// constants).
func (p *CheckpointPlan) State() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.data.State
}

// DestDir returns the checkpoint destination directory.
func (p *CheckpointPlan) DestDir() string { return p.destDir }

// SourceDir returns the source database directory recorded in the plan.
func (p *CheckpointPlan) SourceDir() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.data.SourceDir
}

// VisibleSeqNum returns the visible sequence number of the consistent version
// the checkpoint represents.
func (p *CheckpointPlan) VisibleSeqNum() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.data.VisibleSeqNum
}

// TotalFiles returns the number of regular files that make up the
// checkpoint, including the OPTIONS file, the MANIFEST and the WAL files.
func (p *CheckpointPlan) TotalFiles() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.data.Files)
}

// CopiedFiles returns the number of files that were already copied and
// verified against the plan.
func (p *CheckpointPlan) CopiedFiles() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for i := range p.data.Files {
		if p.data.Files[i].Copied {
			n++
		}
	}
	return n
}

// Committed reports whether the checkpoint was already committed.
func (p *CheckpointPlan) Committed() bool {
	return p.State() == CheckpointPlanCommitted
}

// Close releases the plan handle. It does not remove staged files or the
// plan; the plan may be resumed later with DB.ResumeCheckpointPlan. It is a
// no-op after Commit removes the staging directory.
func (p *CheckpointPlan) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.d.unregisterCheckpointPlan(p.destDir)
	return nil
}

// CreateCheckpointPlan creates a resumable checkpoint plan for the database's
// current consistent version and stores it in destDir. The plan records the
// source database identity, the format major version, the visible sequence
// number, the complete file set, file sizes and file checksums.
//
// No database files are copied by CreateCheckpointPlan; use the returned
// plan's CopyFiles, Verify and Commit methods (or the Publish helper) to
// materialize and publish the checkpoint.
//
// destDir must either not exist or be empty apart from the staging data of a
// failed CreateCheckpointPlan call. Use ResumeCheckpointPlan to continue an
// existing plan.
//
// ExperimentalResumableCheckpoints must be enabled.
func (d *DB) CreateCheckpointPlan(
	ctx context.Context, destDir string, opts ...CheckpointOption,
) (*CheckpointPlan, error) {
	if !d.opts.ExperimentalResumableCheckpoints {
		return nil, ErrResumableCheckpointsDisabled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.checkDBOpen(); err != nil {
		return nil, err
	}
	opt := &checkpointOptions{}
	for _, fn := range opts {
		fn(opt)
	}
	// Fast-fail if a plan already exists in the destination; callers must use
	// ResumeCheckpointPlan. This is checked before any expensive snapshotting
	// or admission processing.
	existingPlanPath := d.opts.FS.PathJoin(
		d.opts.FS.PathJoin(destDir, resumableStagingDirName), resumablePlanFileName)
	if _, err := d.opts.FS.Stat(existingPlanPath); err == nil {
		return nil, ErrCheckpointPlanExists
	} else if !oserror.IsNotExist(err) {
		return nil, err
	}
	if err := d.registerCheckpointPlan(destDir); err != nil {
		return nil, err
	}
	p := &CheckpointPlan{
		d:          d,
		base:       d.opts.FS,
		destDir:    destDir,
		stagingDir: d.opts.FS.PathJoin(destDir, resumableStagingDirName),
	}
	p.fs = vfs.NewSyncingFS(d.opts.FS, vfs.SyncingFileOptions{
		NoSyncOnClose: d.opts.NoSyncOnClose,
		BytesPerSync:  d.opts.BytesPerSync,
	})
	p.tmpDir = p.fs.PathJoin(p.stagingDir, resumableTmpDirName)
	p.planPath = p.fs.PathJoin(p.stagingDir, resumablePlanFileName)

	cleanup := func(err error) (*CheckpointPlan, error) {
		if err != nil {
			d.unregisterCheckpointPlan(destDir)
		}
		return nil, err
	}

	snap, err := d.takeCheckpointSnapshot(ctx, opt)
	if err != nil {
		return cleanup(err)
	}
	defer snap.current.Unref()

	data, err := buildResumablePlanData(ctx, p.fs, d.dirname, snap)
	if err != nil {
		d.enableCheckpointFileDeletionsAfterSnapshot()
		return cleanup(err)
	}
	p.data = data

	// Admit the destination directory and create the staging hierarchy.
	var dir vfs.File
	dir, err = admitAndCreateStaging(p.fs, destDir, p.stagingDir, p.tmpDir)
	if err != nil {
		d.enableCheckpointFileDeletionsAfterSnapshot()
		return cleanup(err)
	}
	if err = dir.Close(); err != nil {
		d.enableCheckpointFileDeletionsAfterSnapshot()
		return cleanup(err)
	}
	if err = ensureStagingSentinel(p.fs, destDir); err != nil {
		d.enableCheckpointFileDeletionsAfterSnapshot()
		return cleanup(err)
	}
	if err = p.saveLocked(); err != nil {
		d.enableCheckpointFileDeletionsAfterSnapshot()
		return cleanup(err)
	}
	d.enableCheckpointFileDeletionsAfterSnapshot()
	return p, nil
}

// ResumeCheckpointPlan resumes a checkpoint plan previously created in
// destDir with CreateCheckpointPlan. It verifies that the plan belongs to the
// receiving database and that the destination contains only files referenced
// by the plan. Files already staged are only reused if their sizes and
// checksums match the plan.
//
// If the checkpoint was already committed, the returned plan reports
// Committed() == true and CopyFiles, Verify and Commit are no-ops.
//
// ExperimentalResumableCheckpoints must be enabled.
func (d *DB) ResumeCheckpointPlan(ctx context.Context, destDir string) (*CheckpointPlan, error) {
	if !d.opts.ExperimentalResumableCheckpoints {
		return nil, ErrResumableCheckpointsDisabled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.checkDBOpen(); err != nil {
		return nil, err
	}
	fs := vfs.NewSyncingFS(d.opts.FS, vfs.SyncingFileOptions{
		NoSyncOnClose: d.opts.NoSyncOnClose,
		BytesPerSync:  d.opts.BytesPerSync,
	})
	p := &CheckpointPlan{
		d:          d,
		base:       d.opts.FS,
		fs:         fs,
		destDir:    destDir,
		stagingDir: d.opts.FS.PathJoin(destDir, resumableStagingDirName),
	}
	p.tmpDir = p.fs.PathJoin(p.stagingDir, resumableTmpDirName)
	p.planPath = p.fs.PathJoin(p.stagingDir, resumablePlanFileName)

	committed, err := isCheckpointCommitted(fs, destDir)
	if err != nil {
		return nil, err
	}
	if committed {
		// The checkpoint is complete; self-heal any leftovers of a crash
		// during commit (sentinel, staging directory, obsolete markers).
		p.data = resumablePlanData{State: CheckpointPlanCommitted, SourceDir: ""}
		p.finalizeCommittedLocked()
		p.closed = true
		return p, nil
	}

	if err := p.loadLocked(); err != nil {
		return nil, err
	}
	if p.data.PlanVersion != resumablePlanVersion {
		return nil, errors.Errorf("pebble: unsupported checkpoint plan version %d", p.data.PlanVersion)
	}
	if p.data.State == CheckpointPlanCommitted {
		// Defensive: plan file says committed even though the commit marker is
		// missing; do not resume.
		return nil, errors.New("pebble: checkpoint plan is in an inconsistent committed state")
	}
	if p.data.SourceDir != d.dirname {
		return nil, errors.Errorf(
			"pebble: checkpoint plan was created for source database %q, not %q",
			p.data.SourceDir, d.dirname)
	}
	if err := d.registerCheckpointPlan(destDir); err != nil {
		return nil, err
	}
	if err := p.checkDestinationLocked(); err != nil {
		d.unregisterCheckpointPlan(destDir)
		return nil, err
	}
	return p, nil
}

// Publish runs CopyFiles, Verify and Commit to completion, resuming any
// partially copied files. It is equivalent to:
//
//	plan, err := d.CreateCheckpointPlan(ctx, destDir, opts...)
//	err = plan.Publish(ctx)
//
// except that it also transparently resumes an existing plan in destDir.
func (d *DB) PublishCheckpoint(
	ctx context.Context, destDir string, opts ...CheckpointOption,
) (*CheckpointPlan, error) {
	if !d.opts.ExperimentalResumableCheckpoints {
		return nil, ErrResumableCheckpointsDisabled
	}
	planExists, err := checkpointPlanPresent(d.opts.FS, destDir)
	if err != nil {
		return nil, err
	}
	var p *CheckpointPlan
	if planExists {
		p, err = d.ResumeCheckpointPlan(ctx, destDir)
	} else {
		p, err = d.CreateCheckpointPlan(ctx, destDir, opts...)
	}
	if err != nil {
		return nil, err
	}
	if err := p.Publish(ctx); err != nil {
		return p, err
	}
	return p, nil
}

// Publish copies all remaining files, verifies the checkpoint and commits it.
func (p *CheckpointPlan) Publish(ctx context.Context) error {
	if err := p.CopyFiles(ctx); err != nil {
		return err
	}
	if err := p.Verify(ctx); err != nil {
		return err
	}
	return p.Commit(ctx)
}

// CopyFiles copies (or resumes copying) every file referenced by the plan.
// It is idempotent: staged files that match the plan are reused and every
// other file is rebuilt via a temporary file that is renamed into place only
// after its size and checksum match the plan.
//
// CopyFiles fails without modifying the destination's visibility if the
// context is cancelled, the source database is closed, a source file's
// identity changed, a destination file is corrupt, or the file deletion
// strategy cannot protect the source files.
func (p *CheckpointPlan) CopyFiles(ctx context.Context) (retErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed && p.data.State != CheckpointPlanCommitted {
		return errors.New("pebble: checkpoint plan handle is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch p.data.State {
	case CheckpointPlanCommitted:
		return nil
	case CheckpointPlanVerified:
		// Verify remains valid until Commit; nothing else to copy.
		return nil
	}
	if err := p.d.checkDBOpen(); err != nil {
		return err
	}

	// Clean up verification directories left behind by crashed attempts.
	if err := p.removeStaleVerifyDirsLocked(ctx); err != nil {
		return err
	}
	// Repair the not-openable sentinel if a crash happened before it was
	// written.
	if err := ensureStagingSentinel(p.fs, p.destDir); err != nil {
		return err
	}

	// Protect the source files for the duration of this stage. This mirrors
	// DB.Checkpoint: disable deletions and reference the current version.
	p.d.mu.Lock()
	p.d.disableFileDeletions()
	defer func() {
		p.d.mu.Lock()
		defer p.d.mu.Unlock()
		p.d.enableFileDeletions()
	}()
	current := p.d.mu.versions.currentVersion()
	current.Ref()
	defer current.Unref()
	allLogicalLogs := p.d.mu.log.manager.List()
	p.d.mu.Unlock()

	// The plan pins a format major version and source identity. Fail closed
	// if either changed.
	if fmv := p.d.FormatMajorVersion(); int(fmv) != p.data.FormatMajorVersion {
		return errors.Errorf("pebble: source format major version changed from %d to %d after checkpoint plan creation",
			p.data.FormatMajorVersion, int(fmv))
	}
	// Note: the source OPTIONS file number may change across a database close
	// and reopen (every open writes a new OPTIONS file). The staged OPTIONS
	// file's recorded output checksum remains the authoritative identity; a
	// missing staged OPTIONS file is rebuilt and rejected if its regenerated
	// content no longer matches the plan.
	availableLogs := make(map[uint64]wal.LogicalLog, len(allLogicalLogs))
	for _, ll := range allLogicalLogs {
		availableLogs[uint64(ll.Num)] = ll
	}
	copiedWALs := make(map[uint64]struct{})
	for i := range p.data.Files {
		if f := &p.data.Files[i]; f.Kind == planFileKindWAL && f.Copied {
			copiedWALs[f.FileNum] = struct{}{}
		}
	}
	for _, n := range p.data.WALNums {
		if _, copied := copiedWALs[n]; copied {
			// The staged, checksum-verified copy no longer depends on the
			// source log.
			continue
		}
		if _, ok := availableLogs[n]; !ok {
			return errors.Errorf("pebble: source WAL %06d required by checkpoint plan is no longer available", n)
		}
	}

	// Recreate the temporary working directory. Contents there are never
	// visible as checkpoint files.
	if err := p.fs.RemoveAll(p.tmpDir); err != nil {
		return err
	}
	if err := p.fs.MkdirAll(p.tmpDir, 0755); err != nil {
		return err
	}
	// Remove the tmp dir when the stage ends (on success or failure); stale
	// temp files are also swept at the start of the next attempt.
	defer func() { _ = p.fs.RemoveAll(p.tmpDir) }()

	if p.data.State == CheckpointPlanPlanning {
		p.data.State = CheckpointPlanCopying
		if err := p.saveLocked(); err != nil {
			return err
		}
	}

	// Files are copied in the same order DB.Checkpoint uses: OPTIONS,
	// format-version marker, sstables and blob files, MANIFEST, remote
	// catalog, WALs.
	for i := range p.data.Files {
		f := &p.data.Files[i]
		if f.Copied {
			// Confirm the staged file still matches the plan; otherwise
			// rebuild it.
			ok, err := p.finalFileMatchesLocked(f)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			f.Copied = false
			if err := p.saveLocked(); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		switch f.Kind {
		case planFileKindOptions:
			if err := p.buildOptionsFileLocked(ctx, f); err != nil {
				return err
			}
		case planFileKindTable:
			if err := p.buildExactFileLocked(ctx, f); err != nil {
				return err
			}
		case planFileKindManifest:
			if err := p.buildManifestFileLocked(ctx, f); err != nil {
				return err
			}
		case planFileKindCatalog:
			// Handled below together with CheckpointState.
			continue
		case planFileKindWAL:
			if err := p.buildWALFileLocked(ctx, f, availableLogs[f.FileNum]); err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unknown planned file kind %q", f.Kind)
		}
	}

	// No marker files are created in the destination before commit: a
	// directory with a format-version marker but no manifest marker could
	// otherwise be opened as an empty database. The markers are published
	// atomically by Commit.

	// Remote object catalog.
	if len(p.data.RemoteFileNums) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		before := p.destinationEntrySetLocked()
		if err := p.d.objProvider.CheckpointState(p.fs, p.destDir, remoteNumsFromPlan(p.data.RemoteFileNums)); err != nil {
			return err
		}
		if err := p.reconcileCatalogFilesLocked(before); err != nil {
			return err
		}
	}
	// Re-check catalog files after a resume too: ensure each is intact.
	for i := range p.data.Files {
		f := &p.data.Files[i]
		if f.Kind != planFileKindCatalog || f.Copied {
			continue
		}
		if err := p.buildCatalogFileLocked(f); err != nil {
			return err
		}
	}

	for i := range p.data.Files {
		if !p.data.Files[i].Copied {
			return errors.Errorf("pebble: checkpoint file %s was not copied", p.data.Files[i].Name)
		}
	}
	p.data.State = CheckpointPlanStaged
	if err := p.saveLocked(); err != nil {
		return err
	}
	return nil
}

// Verify checks that every staged file is present and matches its recorded
// size and checksum, then opens a read-only copy of the checkpoint
// materialized in an isolated directory to validate all file and version
// relationships. Nothing in destDir is made openable by Verify.
func (p *CheckpointPlan) Verify(ctx context.Context) (retErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed && p.data.State != CheckpointPlanCommitted {
		return errors.New("pebble: checkpoint plan handle is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch p.data.State {
	case CheckpointPlanCommitted:
		return nil
	case CheckpointPlanVerified:
		return nil
	case CheckpointPlanStaged:
	default:
		return errors.New("pebble: checkpoint is not fully staged; call CopyFiles first")
	}
	if err := p.d.checkDBOpen(); err != nil {
		return err
	}
	if err := p.checkDestinationLocked(); err != nil {
		return err
	}
	// Verify each file's content independently.
	for i := range p.data.Files {
		f := &p.data.Files[i]
		ok, err := p.finalFileMatchesLocked(f)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("pebble: staged checkpoint file %s is missing or corrupt", f.Name)
		}
	}

	verifyDir, err := p.createVerifyDirLocked(ctx)
	if err != nil {
		return err
	}
	// The verify dir is always removed; failure leaves destDir untouched and
	// uncommitted.
	cleaned := false
	cleanupVerify := func() {
		if cleaned {
			return
		}
		cleaned = true
		_ = p.fs.RemoveAll(verifyDir)
	}
	defer cleanupVerify()

	vdb, err := Open(verifyDir, p.verifyOptionsLocked())
	if err != nil {
		return errors.Wrap(err, "verifying checkpoint")
	}
	// Scan the entire key space, materializing values, to exercise every
	// version relationship recorded by the MANIFEST.
	iter, _ := vdb.NewIter(nil)
	var count int
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			_ = vdb.Close()
			return errors.Wrap(err, "verifying checkpoint")
		}
		if v := iter.Value(); v != nil {
			// Force the value to be materialized (it may reside in a blob
			// file).
			v = append([]byte(nil), v...)
			_ = v
		}
		count++
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		_ = vdb.Close()
		return errors.Wrap(err, "verifying checkpoint")
	}
	if err := iter.Close(); err != nil {
		_ = vdb.Close()
		return errors.Wrap(err, "verifying checkpoint")
	}
	if err := vdb.Close(); err != nil {
		return errors.Wrap(err, "verifying checkpoint")
	}
	cleanupVerify()

	p.data.State = CheckpointPlanVerified
	if err := p.saveLocked(); err != nil {
		return err
	}
	return nil
}

// Commit atomically publishes the verified checkpoint by creating the
// MANIFEST marker and the checkpoint-commit marker. After a successful
// Commit, destDir contains a complete checkpoint at the plan's consistent
// version and may be opened, and the staging directory and plan are removed.
func (p *CheckpointPlan) Commit(ctx context.Context) (retErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commitLocked(ctx, "")
}

// CommitAndRemovePrevious commits the checkpoint and, only after the new
// checkpoint has been verified to open, removes the old checkpoint directory
// oldCheckpointDir. The removal is idempotent: a repeated call is a no-op
// once the old directory is gone. The old directory must differ from both the
// source database and the new checkpoint directory.
func (p *CheckpointPlan) CommitAndRemovePrevious(
	ctx context.Context, oldCheckpointDir string,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commitLocked(ctx, oldCheckpointDir)
}

func (p *CheckpointPlan) commitLocked(ctx context.Context, oldCheckpointDir string) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch p.data.State {
	case CheckpointPlanCommitted:
		// Idempotent commit: the checkpoint is already published. Finalize any
		// leftovers from a crash during the original commit. A previous call
		// already removed the old checkpoint directory (if any); a missing
		// directory is not an error.
		p.finalizeCommittedLocked()
		if oldCheckpointDir != "" {
			if err := p.fs.RemoveAll(oldCheckpointDir); err != nil && !oserror.IsNotExist(err) {
				return err
			}
		}
		return nil
	case CheckpointPlanVerified:
	default:
		p.mu.Unlock()
		err := p.Verify(ctx)
		p.mu.Lock()
		if err != nil {
			return err
		}
	}
	if err := p.d.checkDBOpen(); err != nil {
		return err
	}

	if oldCheckpointDir != "" {
		if oldCheckpointDir == p.destDir {
			return errors.New("pebble: cannot remove the checkpoint being committed")
		}
		if oldCheckpointDir == p.data.SourceDir {
			return errors.New("pebble: cannot remove the source database directory")
		}
	}

	// Publish the format-version marker first (it is harmless without the
	// manifest marker, and recovery requires both).
	if err := p.ensureFormatMarkerLocked(); err != nil {
		return err
	}

	// Publish the MANIFEST marker (the atomic point at which destDir becomes
	// a valid database directory). Content was already verified in isolation.
	manifestFileNum := base.DiskFileNum(p.data.ManifestFileNum)
	manifestName := base.MakeFilename(base.FileTypeManifest, manifestFileNum)
	if _, err := p.fs.Stat(base.MakeFilepath(p.fs, p.destDir, base.FileTypeManifest, manifestFileNum)); err != nil {
		if oserror.IsNotExist(err) {
			return errors.Errorf("pebble: MANIFEST-%06d missing at commit", uint64(manifestFileNum))
		}
		return err
	}
	manifestMarker, markerValue, err := atomicfs.LocateMarker(p.fs, p.destDir, manifestMarkerName)
	if err != nil {
		return err
	}
	if markerValue != manifestName {
		if err := manifestMarker.SyncDir(); err != nil {
			_ = manifestMarker.Close()
			return err
		}
		if err := manifestMarker.Move(manifestName); err != nil {
			_ = manifestMarker.Close()
			return err
		}
	}
	if err := manifestMarker.Close(); err != nil {
		return err
	}

	// Set the checkpoint-commit marker.
	commitMarker, commitValue, err := atomicfs.LocateMarker(p.fs, p.destDir, checkpointCommitMarkerName)
	if err != nil {
		return err
	}
	if commitValue != checkpointCommitValue {
		if err := commitMarker.Move(checkpointCommitValue); err != nil {
			_ = commitMarker.Close()
			return err
		}
	}
	if err := commitMarker.Close(); err != nil {
		return err
	}

	// Sync the destination directory so that the commit markers are durable.
	dir, err := p.fs.OpenDir(p.destDir)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	if err := dir.Close(); err != nil {
		return err
	}

	// The markers now make this a complete, openable checkpoint; remove the
	// not-openable staging sentinel. Any crash before this point left the
	// sentinel in place (unopenable), and any crash after leaves a complete
	// checkpoint which a later resume finalizes.
	if err := removeStagingSentinel(p.fs, p.destDir); err != nil {
		return err
	}

	p.data.State = CheckpointPlanCommitted
	if err := p.saveLocked(); err != nil {
		return err
	}

	// Only after the new checkpoint is actually openable do we remove an old
	// checkpoint, if requested.
	if oldCheckpointDir != "" {
		if err := p.verifyOpenableLocked(ctx, p.destDir); err != nil {
			return errors.Wrapf(err, "pebble: refusing to remove previous checkpoint %q", oldCheckpointDir)
		}
		if err := p.fs.RemoveAll(oldCheckpointDir); err != nil && !oserror.IsNotExist(err) {
			return err
		}
	}

	// Drop the sentinel, obsolete markers and all staging data.
	p.finalizeCommittedLocked()
	p.closed = true
	p.d.unregisterCheckpointPlan(p.destDir)
	return nil
}

// finalizeCommittedLocked removes leftovers that a crash (or an earlier
// successful commit) may have left in the destination: the staging sentinel,
// obsolete marker iterations, superseded catalog files and the staging
// directory. Errors are best-effort: the checkpoint itself is already
// complete and openable.
func (p *CheckpointPlan) finalizeCommittedLocked() {
	_ = removeStagingSentinel(p.fs, p.destDir)
	_ = p.removeObsoleteMarkerLocked(manifestMarkerName)
	_ = p.removeObsoleteMarkerLocked(formatVersionMarkerName)
	_ = p.removeObsoleteMarkerLocked(checkpointCommitMarkerName)
	if len(p.data.RemoteFileNums) > 0 {
		_ = p.removeObsoleteMarkerLocked(remoteCatalogMarkerName)
		_ = p.pruneCatalogFilesLocked()
	}
	if p.stagingDir != "" {
		if err := p.fs.RemoveAll(p.stagingDir); err != nil && !oserror.IsNotExist(err) {
			return
		}
		if dir, err := p.fs.OpenDir(p.destDir); err == nil {
			_ = errors.CombineErrors(dir.Sync(), dir.Close())
		}
	}
}

// Abort discards an uncommitted checkpoint plan and removes everything that
// was staged in destDir. It is an error to abort a committed checkpoint. It
// is safe to retry; a missing destination directory is not an error.
func (p *CheckpointPlan) Abort(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.data.State == CheckpointPlanCommitted {
		return errors.New("pebble: cannot abort a committed checkpoint")
	}
	if _, err := p.fs.Stat(p.destDir); err != nil {
		if oserror.IsNotExist(err) {
			// Idempotent abort.
			p.closed = true
			p.d.unregisterCheckpointPlan(p.destDir)
			return nil
		}
		return err
	}
	// Defensive: never remove a directory that contains entries the plan
	// doesn't account for.
	if err := p.checkDestinationLocked(); err != nil {
		return err
	}
	if err := p.removeStaleVerifyDirsLocked(ctx); err != nil {
		return err
	}
	if err := p.fs.RemoveAll(p.destDir); err != nil && !oserror.IsNotExist(err) {
		return err
	}
	p.closed = true
	p.d.unregisterCheckpointPlan(p.destDir)
	return nil
}

// checkDBOpen returns the error stored when the database was closed, or nil
// if the database is open.
func (d *DB) checkDBOpen() error {
	if v := d.closed.Load(); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
		return errors.New("pebble: database is closed")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Plan creation: snapshot and plan data.
// ---------------------------------------------------------------------------

// checkpointSnapshot bundles the consistent-version state captured under the
// database locks. File deletions are disabled and current is referenced for
// the lifetime of the snapshot.
type checkpointSnapshot struct {
	current         *manifest.Version
	formatVers      FormatMajorVersion
	manifestFileNum base.DiskFileNum
	manifestSize    int64
	optionsFileNum  base.DiskFileNum
	visibleSeqNum   base.SeqNum
	logs            []wal.LogicalLog
	// minUnflushedLogNum is the smallest WAL number that must be present for
	// the pinned version to open.
	minUnflushedLogNum base.DiskFileNum

	files          []resumablePlanFile
	remoteFileNums []base.DiskFileNum
	excludedEdit   []byte
}

// takeCheckpointSnapshot captures the same consistent state DB.Checkpoint
// captures and enumerates the files that must be copied. File deletions are
// left disabled (and current referenced) until the caller unrefs current and
// calls enableCheckpointFileDeletionsAfterSnapshot.
func (d *DB) takeCheckpointSnapshot(
	ctx context.Context, opt *checkpointOptions,
) (_ *checkpointSnapshot, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opt.flushWAL && !d.opts.DisableWAL {
		if err := d.LogData(nil, Sync); err != nil {
			return nil, err
		}
	}

	d.mu.Lock()
	d.disableFileDeletions()
	d.mu.versions.logLock()
	current := d.mu.versions.currentVersion()
	formatVers := d.FormatMajorVersion()
	manifestFileNum := d.mu.versions.manifestFileNum
	manifestSize := d.mu.versions.manifest.Size()
	optionsFileNum := d.optionsFileNum

	virtualBackingFiles := make(map[base.DiskFileNum]struct{})
	for backing := range d.mu.versions.latest.virtualBackings.All() {
		virtualBackingFiles[backing.DiskFileNum] = struct{}{}
	}
	versionBlobFiles := d.mu.versions.latest.blobFiles.Metadatas()
	allLogicalLogs := d.mu.log.manager.List()

	var flushableIngestFiles []base.DiskFileNum
	for _, entry := range d.mu.mem.queue {
		if fi, ok := entry.flushable.(*ingestedFlushable); ok {
			for _, f := range fi.files {
				flushableIngestFiles = append(flushableIngestFiles, f.TableBacking.DiskFileNum)
			}
		}
	}
	visibleSeqNum := d.mu.versions.visibleSeqNum.Load()
	minUnflushedLogNum := d.mu.versions.minUnflushedLogNum
	current.Ref()
	d.mu.versions.logUnlock()
	d.mu.Unlock()
	// On any failure while building the snapshot, release the acquired
	// protection so that file handles and deletion-disable counts do not
	// leak. On success, the caller unrefs current and re-enables deletions
	// once the plan is fully captured.
	defer func() {
		if retErr != nil {
			current.Unref()
			d.mu.Lock()
			d.enableFileDeletions()
			d.mu.Unlock()
		}
	}()

	snap := &checkpointSnapshot{
		current:            current,
		formatVers:         formatVers,
		manifestFileNum:    manifestFileNum,
		manifestSize:       manifestSize,
		optionsFileNum:     optionsFileNum,
		visibleSeqNum:      visibleSeqNum,
		logs:               allLogicalLogs,
		minUnflushedLogNum: minUnflushedLogNum,
	}

	var excludedTables map[manifest.DeletedTableEntry]*manifest.TableMetadata
	var includedBlobFiles map[base.BlobFileID]struct{}
	requiredVirtualBackingFiles := make(map[base.DiskFileNum]struct{})
	seen := make(map[string]struct{})

	addExactFile := func(typ base.FileType, fileNum base.DiskFileNum) {
		srcPath := base.MakeFilepath(d.opts.FS, d.dirname, typ, fileNum)
		name := d.opts.FS.PathBase(srcPath)
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		// Both sstables and blob files are byte-for-byte copies; their file
		// type is recoverable from their name.
		snap.files = append(snap.files, resumablePlanFile{
			Name:       name,
			Kind:       planFileKindTable,
			FileNum:    uint64(fileNum),
			SourcePath: srcPath,
		})
	}

	for l := range current.Levels {
		iter := current.Levels[l].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if excludeFromCheckpoint(f, opt, d.cmp) {
				if excludedTables == nil {
					excludedTables = make(map[manifest.DeletedTableEntry]*manifest.TableMetadata)
				}
				excludedTables[manifest.DeletedTableEntry{
					Level:   l,
					FileNum: f.TableNum,
				}] = f
				continue
			}
			if len(f.BlobReferences) > 0 {
				if includedBlobFiles == nil {
					includedBlobFiles = make(map[base.BlobFileID]struct{})
				}
				for _, ref := range f.BlobReferences {
					if _, ok := includedBlobFiles[ref.FileID]; !ok {
						includedBlobFiles[ref.FileID] = struct{}{}
						obj, ok := current.BlobFiles.Lookup(ref.FileID)
						if !ok {
							retErr = errors.Errorf("blob file %s not found", ref.FileID)
							return nil, retErr
						}
						typ, fileNum := obj.FileInfo()
						addExactFile(typ, fileNum)
					}
				}
			}
			tableBacking := f.TableBacking
			if f.Virtual {
				if _, ok := requiredVirtualBackingFiles[tableBacking.DiskFileNum]; ok {
					continue
				}
				requiredVirtualBackingFiles[tableBacking.DiskFileNum] = struct{}{}
			}
			addExactFile(base.FileTypeTable, tableBacking.DiskFileNum)
		}
	}

	var removeBackingTables []base.DiskFileNum
	for diskFileNum := range virtualBackingFiles {
		if _, ok := requiredVirtualBackingFiles[diskFileNum]; !ok {
			removeBackingTables = append(removeBackingTables, diskFileNum)
		}
	}
	for _, fileNum := range flushableIngestFiles {
		addExactFile(base.FileTypeTable, fileNum)
	}

	// Remote files are referenced through the remote object catalog.
	for l := range current.Levels {
		iter := current.Levels[l].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if excludeFromCheckpoint(f, opt, d.cmp) {
				continue
			}
			meta, err := d.objProvider.Lookup(base.FileTypeTable, f.TableBacking.DiskFileNum)
			if err != nil {
				retErr = err
				return nil, retErr
			}
			if meta.IsRemote() {
				snap.remoteFileNums = append(snap.remoteFileNums, meta.DiskFileNum)
			}
		}
	}

	var excludedBlobFiles map[manifest.DeletedBlobFileEntry]*manifest.PhysicalBlobFile
	if len(versionBlobFiles) > 0 {
		for _, meta := range versionBlobFiles {
			if _, ok := includedBlobFiles[meta.FileID]; !ok {
				if excludedBlobFiles == nil {
					excludedBlobFiles = make(map[manifest.DeletedBlobFileEntry]*manifest.PhysicalBlobFile)
				}
				excludedBlobFiles[manifest.DeletedBlobFileEntry{
					FileID:  meta.FileID,
					FileNum: meta.Physical.FileNum,
				}] = meta.Physical
			}
		}
	}

	if len(excludedTables) > 0 || len(excludedBlobFiles) > 0 {
		ve := manifest.VersionEdit{
			DeletedTables:        excludedTables,
			RemovedBackingTables: removeBackingTables,
			DeletedBlobFiles:     excludedBlobFiles,
		}
		var buf bytes.Buffer
		if err := ve.Encode(&buf); err != nil {
			return nil, err
		}
		snap.excludedEdit = buf.Bytes()
	}
	return snap, nil
}

// enableCheckpointFileDeletionsAfterSnapshot pairs with
// takeCheckpointSnapshot once the snapshot's files no longer need deletion
// protection.
func (d *DB) enableCheckpointFileDeletionsAfterSnapshot() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enableFileDeletions()
}

// buildResumablePlanData computes source sizes and checksums and assembles
// the persisted plan record.
func buildResumablePlanData(
	ctx context.Context, fs vfs.FS, sourceDir string, snap *checkpointSnapshot,
) (resumablePlanData, error) {
	data := resumablePlanData{
		PlanVersion:          resumablePlanVersion,
		State:                CheckpointPlanPlanning,
		SourceDir:            sourceDir,
		FormatMajorVersion:   int(snap.formatVers),
		VisibleSeqNum:        uint64(snap.visibleSeqNum),
		ManifestFileNum:      uint64(snap.manifestFileNum),
		ManifestSize:         snap.manifestSize,
		SourceOptionsFileNum: uint64(snap.optionsFileNum),
		ExcludedVersionEdit:  snap.excludedEdit,
	}
	// Only WALs at or above the version's minUnflushedLogNum and that contain
	// at least one record are needed to open the checkpoint. Older logs are
	// obsolete, and the empty active log created by a WAL rotation (eg. the
	// rotation that precedes a flush) has no records and may not survive a
	// database restart; excluding it keeps the plan resumable.
	for _, ll := range snap.logs {
		if base.DiskFileNum(ll.Num) < snap.minUnflushedLogNum {
			continue
		}
		hasRecords, err := logicalLogHasRecords(ctx, ll)
		if err != nil {
			return data, err
		}
		if hasRecords {
			data.WALNums = append(data.WALNums, uint64(ll.Num))
		}
	}
	for _, n := range snap.remoteFileNums {
		data.RemoteFileNums = append(data.RemoteFileNums, uint64(n))
	}

	// OPTIONS file identity.
	optionsPath := base.MakeFilepath(fs, data.SourceDir, base.FileTypeOptions, snap.optionsFileNum)
	optionsSize, optionsHash, err := hashFile(ctx, fs, optionsPath, -1)
	if err != nil {
		return data, errors.Wrapf(err, "hashing source OPTIONS file")
	}
	data.Files = append(data.Files, resumablePlanFile{
		Name:       fs.PathBase(optionsPath),
		Kind:       planFileKindOptions,
		FileNum:    uint64(snap.optionsFileNum),
		SourcePath: optionsPath,
		SourceSize: optionsSize,
		SourceHash: optionsHash,
	})

	// Exact-copy files: compute source size and checksum.
	for i := range snap.files {
		if err := ctx.Err(); err != nil {
			return data, err
		}
		f := &snap.files[i]
		size, h, err := hashFile(ctx, fs, f.SourcePath, -1)
		if err != nil {
			return data, errors.Wrapf(err, "hashing source file %s", f.Name)
		}
		f.SourceSize = size
		f.SourceHash = h
		f.Size = size
		f.Hash = h
		data.Files = append(data.Files, *f)
	}

	// MANIFEST identity (prefix).
	manifestPath := base.MakeFilepath(fs, data.SourceDir, base.FileTypeManifest, snap.manifestFileNum)
	manifestHash, err := hashFilePrefixSum(ctx, fs, manifestPath, snap.manifestSize)
	if err != nil {
		return data, errors.Wrap(err, "hashing source MANIFEST")
	}
	data.Files = append(data.Files, resumablePlanFile{
		Name:       fs.PathBase(manifestPath),
		Kind:       planFileKindManifest,
		FileNum:    uint64(snap.manifestFileNum),
		SourcePath: manifestPath,
		SourceSize: snap.manifestSize,
		SourceHash: manifestHash,
	})

	// WAL files (output names and checksums are discovered on the first
	// copy).
	for _, n := range data.WALNums {
		data.Files = append(data.Files, resumablePlanFile{
			Name:    fmt.Sprintf("%s.log", base.DiskFileNum(n).String()),
			Kind:    planFileKindWAL,
			FileNum: n,
		})
	}

	// Source identity: directory + OPTIONS identity.
	data.SourceID = fmt.Sprintf("%s|%06d|%016x", data.SourceDir, data.SourceOptionsFileNum, optionsHash)
	return data, nil
}

// ---------------------------------------------------------------------------
// Staging and file builders.
// ---------------------------------------------------------------------------

// ensureStagingSentinel creates the CURRENT sentinel that prevents the
// staging directory from being opened as a database before commit.
func ensureStagingSentinel(fs vfs.FS, destDir string) error {
	path := fs.PathJoin(destDir, stagingSentinelName)
	if _, err := fs.Stat(path); err == nil {
		return nil
	} else if !oserror.IsNotExist(err) {
		return err
	}
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(f, stagingSentinelContent); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	dir, err := fs.OpenDir(destDir)
	if err != nil {
		return err
	}
	return errors.CombineErrors(dir.Sync(), dir.Close())
}

// removeStagingSentinel removes the CURRENT sentinel after the commit
// markers are durable.
func removeStagingSentinel(fs vfs.FS, destDir string) error {
	if err := fs.Remove(fs.PathJoin(destDir, stagingSentinelName)); err != nil && !oserror.IsNotExist(err) {
		return err
	}
	dir, err := fs.OpenDir(destDir)
	if err != nil {
		return err
	}
	return errors.CombineErrors(dir.Sync(), dir.Close())
}

// admitAndCreateStaging validates destDir and creates the staging hierarchy.
func admitAndCreateStaging(
	fs vfs.FS, destDir, stagingDir, tmpDir string,
) (vfs.File, error) {
	ls, err := fs.List(destDir)
	if err != nil {
		if !oserror.IsNotExist(err) {
			return nil, err
		}
		// Destination does not exist: create it (and sync parents).
		return mkdirAllAndSyncParents(fs, destDir)
	}
	hasStaging := false
	for _, name := range ls {
		switch name {
		case resumableStagingDirName:
			hasStaging = true
		case stagingSentinelName:
			// Adoptable only together with our staging directory.
		default:
			return nil, &os.PathError{
				Op:   "checkpoint-plan",
				Path: destDir,
				Err:  oserror.ErrExist,
			}
		}
	}
	if !hasStaging && len(ls) > 0 {
		return nil, &os.PathError{
			Op:   "checkpoint-plan",
			Path: destDir,
			Err:  oserror.ErrExist,
		}
	}
	info, err := fs.Stat(destDir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &os.PathError{Op: "checkpoint-plan", Path: destDir, Err: oserror.ErrExist}
	}
	if _, err := fs.List(stagingDir); err != nil {
		if !oserror.IsNotExist(err) {
			return nil, err
		}
	}
	dir, err := mkdirAllAndSyncParents(fs, destDir)
	if err != nil {
		return nil, err
	}
	if err := fs.MkdirAll(tmpDir, 0755); err != nil {
		_ = dir.Close()
		return nil, err
	}
	return dir, nil
}

// buildExactFile materializes an sstable or blob file by hard linking (when
// possible) or streaming copy through a temporary file.
func (p *CheckpointPlan) buildExactFileLocked(ctx context.Context, f *resumablePlanFile) error {
	if err := validateSourceIdentity(ctx, p.fs, f); err != nil {
		return err
	}
	tmp := p.uniqueTmpPathLocked(f.Name)
	if err := linkOrCopyChecked(ctx, p.fs, f.SourcePath, tmp, f.Size, f.Hash); err != nil {
		return errors.Wrapf(err, "copying %s", f.Name)
	}
	if err := p.installTmpLocked(tmp, f); err != nil {
		return err
	}
	return nil
}

// buildOptionsFile regenerates the checkpoint OPTIONS file. The planned
// source OPTIONS file is preferred; if the database was reopened and the old
// OPTIONS file was pruned, the current OPTIONS file is used and the
// regenerated output must still match the plan's recorded output checksum.
func (p *CheckpointPlan) buildOptionsFileLocked(ctx context.Context, f *resumablePlanFile) error {
	srcPath := f.SourcePath
	if info, err := p.fs.Stat(f.SourcePath); err != nil || info.Size() != f.SourceSize {
		currentPath := base.MakeFilepath(p.fs, p.data.SourceDir, base.FileTypeOptions, p.d.optionsFileNum)
		if currentPath != f.SourcePath {
			srcPath = currentPath
		} else if err != nil {
			return errors.Wrapf(err, "pebble: source OPTIONS file required by checkpoint plan is unavailable")
		}
	}
	if srcPath == f.SourcePath {
		if err := validateSourceIdentity(ctx, p.fs, f); err != nil {
			return err
		}
	}
	tmp := p.uniqueTmpPathLocked(f.Name)
	if err := copyCheckpointOptions(p.fs, srcPath, tmp); err != nil {
		return err
	}
	return p.installTransformedLocked(tmp, f)
}

// buildManifestFile regenerates the checkpoint MANIFEST file.
func (p *CheckpointPlan) buildManifestFileLocked(ctx context.Context, f *resumablePlanFile) error {
	if err := validateSourceIdentity(ctx, p.fs, f); err != nil {
		return err
	}
	tmp := p.uniqueTmpPathLocked(f.Name)
	if err := copyCheckpointManifestFile(
		p.fs, f.SourcePath, tmp,
		base.DiskFileNum(p.data.ManifestFileNum), p.data.ManifestSize,
		p.data.ExcludedVersionEdit,
	); err != nil {
		return err
	}
	return p.installTransformedLocked(tmp, f)
}

// buildWALFile regenerates a WAL file truncated at the plan's visible sequence
// number.
func (p *CheckpointPlan) buildWALFileLocked(
	ctx context.Context, f *resumablePlanFile, ll wal.LogicalLog,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	walTmpDir := p.fs.PathJoin(p.tmpDir, fmt.Sprintf("wal-%06d", f.FileNum))
	if err := p.fs.MkdirAll(walTmpDir, 0755); err != nil {
		return err
	}
	defer func() { _ = p.fs.RemoveAll(walTmpDir) }()
	formatVers := FormatMajorVersion(p.data.FormatMajorVersion)
	if err := wal.Copy(p.fs, walTmpDir, ll, base.SeqNum(p.data.VisibleSeqNum), record.LogWriterConfig{
		WriteWALSyncOffsets: func() bool { return formatVers > FormatWALSyncChunks },
	}); err != nil {
		return err
	}
	entries, err := p.fs.List(walTmpDir)
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0] != f.Name {
		return errors.Errorf("unexpected WAL copy output for %06d: %v", f.FileNum, entries)
	}
	tmp := p.fs.PathJoin(walTmpDir, entries[0])
	return p.installTransformedLocked(tmp, f)
}

// buildCatalogFile validates (and, after a crash mid-CheckpointState, waits
// for) a remote catalog file.
func (p *CheckpointPlan) buildCatalogFileLocked(f *resumablePlanFile) error {
	size, h, err := hashFile(context.Background(), p.fs, p.fs.PathJoin(p.destDir, f.Name), -1)
	if err != nil {
		return err
	}
	if f.Size != 0 || f.Hash != 0 {
		if size != f.Size || h != f.Hash {
			return errors.Errorf("remote catalog file %s is corrupt", f.Name)
		}
	} else {
		f.Size, f.Hash = size, h
	}
	f.Copied = true
	return p.saveLocked()
}

// installTransformedLocked hashes a transform-produced temp file, records its
// output identity on first production and renames it into place.
func (p *CheckpointPlan) installTransformedLocked(tmp string, f *resumablePlanFile) error {
	size, h, err := hashFile(context.Background(), p.fs, tmp, -1)
	if err != nil {
		return err
	}
	if f.Size != 0 || f.Hash != 0 {
		if size != f.Size || h != f.Hash {
			return errors.Errorf(
				"pebble: regenerated file %s no longer matches the checkpoint plan (size %d/%d, hash %016x/%016x)",
				f.Name, size, f.Size, h, f.Hash)
		}
	} else {
		f.Size, f.Hash = size, h
		// Persist the output identity before the rename so a crash can never
		// leave an untracked file in place.
		if err := p.saveLocked(); err != nil {
			return err
		}
	}
	return p.installTmpLocked(tmp, f)
}

// installTmpLocked renames a verified temporary file to its final name, syncs
// the destination directory and persists progress.
func (p *CheckpointPlan) installTmpLocked(tmp string, f *resumablePlanFile) error {
	final := p.fs.PathJoin(p.destDir, f.Name)
	if err := p.fs.Rename(tmp, final); err != nil {
		return err
	}
	dir, err := p.fs.OpenDir(p.destDir)
	if err != nil {
		return err
	}
	err = errors.CombineErrors(dir.Sync(), dir.Close())
	if err != nil {
		return err
	}
	f.Copied = true
	return p.saveLocked()
}

// ensureFormatMarkerLocked creates the format-version marker.
func (p *CheckpointPlan) ensureFormatMarkerLocked() error {
	marker, value, err := atomicfs.LocateMarker(p.fs, p.destDir, formatVersionMarkerName)
	if err != nil {
		return err
	}
	want := FormatMajorVersion(p.data.FormatMajorVersion).String()
	if value != want {
		if err := marker.Move(want); err != nil {
			_ = marker.Close()
			return err
		}
	}
	if err := marker.Close(); err != nil {
		return err
	}
	p.data.FormatMarkerCreated = true
	return p.saveLocked()
}

// reconcileCatalogFilesLocked records catalog files newly created by
// CheckpointState and drops obsolete catalog files from the plan.
func (p *CheckpointPlan) reconcileCatalogFilesLocked(before map[string]struct{}) error {
	ls, err := p.fs.List(p.destDir)
	if err != nil {
		return err
	}
	var keep []resumablePlanFile
	have := make(map[string]struct{})
	for i := range p.data.Files {
		f := &p.data.Files[i]
		if f.Kind == planFileKindCatalog {
			continue
		}
		keep = append(keep, *f)
	}
	for _, name := range ls {
		if !strings.HasPrefix(name, remoteCatalogFilenameBase+"-") {
			continue
		}
		if _, old := before[name]; old {
			continue
		}
		size, h, err := hashFile(context.Background(), p.fs, p.fs.PathJoin(p.destDir, name), -1)
		if err != nil {
			return err
		}
		keep = append(keep, resumablePlanFile{
			Name: name, Kind: planFileKindCatalog,
			Size: size, Hash: h, Copied: true,
		})
		have[name] = struct{}{}
		p.data.CatalogFileNames = append(p.data.CatalogFileNames, name)
	}
	// Retain previously known catalog files that are still on disk.
	for _, name := range p.data.CatalogFileNames {
		if _, ok := have[name]; ok {
			continue
		}
		if _, err := p.fs.Stat(p.fs.PathJoin(p.destDir, name)); err == nil {
			size, h, err := hashFile(context.Background(), p.fs, p.fs.PathJoin(p.destDir, name), -1)
			if err != nil {
				return err
			}
			keep = append(keep, resumablePlanFile{
				Name: name, Kind: planFileKindCatalog,
				Size: size, Hash: h, Copied: true,
			})
			have[name] = struct{}{}
		} else if !oserror.IsNotExist(err) {
			return err
		}
	}
	// Dedup catalog names.
	var catalogNames []string
	seen := make(map[string]struct{})
	for _, n := range p.data.CatalogFileNames {
		if _, ok := seen[n]; ok {
			continue
		}
		if _, ok := have[n]; ok {
			catalogNames = append(catalogNames, n)
			seen[n] = struct{}{}
		}
	}
	p.data.CatalogFileNames = catalogNames
	p.data.Files = keep
	return p.saveLocked()
}

// pruneCatalogFilesLocked removes catalog files not referenced by the current
// catalog marker after commit.
func (p *CheckpointPlan) pruneCatalogFilesLocked() error {
	if len(p.data.RemoteFileNums) == 0 {
		return nil
	}
	current, err := atomicfs.ReadMarker(p.fs, p.destDir, remoteCatalogMarkerName)
	if err != nil {
		return err
	}
	ls, err := p.fs.List(p.destDir)
	if err != nil {
		return err
	}
	for _, name := range ls {
		if !strings.HasPrefix(name, remoteCatalogFilenameBase+"-") {
			continue
		}
		if name != current {
			if err := p.fs.Remove(p.fs.PathJoin(p.destDir, name)); err != nil && !oserror.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func (p *CheckpointPlan) removeObsoleteMarkerLocked(name string) error {
	m, _, err := atomicfs.LocateMarker(p.fs, p.destDir, name)
	if err != nil {
		return err
	}
	err = m.RemoveObsolete()
	return errors.CombineErrors(err, m.Close())
}

// ---------------------------------------------------------------------------
// Destination integrity.
// ---------------------------------------------------------------------------

// finalFileMatchesLocked reports whether the staged destination file matches
// its recorded size and checksum.
func (p *CheckpointPlan) finalFileMatchesLocked(f *resumablePlanFile) (bool, error) {
	if f.Size == 0 && f.Hash == 0 && f.Kind != planFileKindCatalog {
		// WAL/transform files have no recorded output identity until the
		// first successful build.
		return false, nil
	}
	info, err := p.fs.Stat(p.fs.PathJoin(p.destDir, f.Name))
	if err != nil {
		if oserror.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.Size() != f.Size {
		return false, nil
	}
	_, h, err := hashFile(context.Background(), p.fs, p.fs.PathJoin(p.destDir, f.Name), -1)
	if err != nil {
		return false, err
	}
	return h == f.Hash, nil
}

// destinationEntrySetLocked returns the set of entry names in destDir.
func (p *CheckpointPlan) destinationEntrySetLocked() map[string]struct{} {
	ls, err := p.fs.List(p.destDir)
	if err != nil {
		return nil
	}
	m := make(map[string]struct{}, len(ls))
	for _, n := range ls {
		m[n] = struct{}{}
	}
	return m
}

// checkDestinationLocked fails if destDir contains any entry that the plan
// does not account for. This prevents mixing in files from another
// checkpoint or a newer database version.
func (p *CheckpointPlan) checkDestinationLocked() error {
	ls, err := p.fs.List(p.destDir)
	if err != nil {
		return err
	}
	allowed := map[string]struct{}{
		resumableStagingDirName: {},
		stagingSentinelName:     {},
	}
	for i := range p.data.Files {
		allowed[p.data.Files[i].Name] = struct{}{}
	}
	// Markers may have obsolete iterations; allow any well-known marker
	// prefix we might create. The remote object catalog marker and files may
	// only appear when the plan references remote objects.
	markerPrefixes := []string{
		"marker." + formatVersionMarkerName + ".",
		"marker." + manifestMarkerName + ".",
		"marker." + checkpointCommitMarkerName + ".",
	}
	if len(p.data.RemoteFileNums) > 0 {
		markerPrefixes = append(markerPrefixes, "marker."+remoteCatalogMarkerName+".")
	}
	for _, name := range ls {
		if _, ok := allowed[name]; ok {
			continue
		}
		matched := false
		for _, prefix := range markerPrefixes {
			if strings.HasPrefix(name, prefix) {
				matched = true
				break
			}
		}
		if matched {
			// Ensure the marker is well formed.
			if _, _, _, perr := parseCheckpointMarkerName(name); perr != nil {
				return errors.Wrapf(perr, "pebble: unexpected file %q in checkpoint directory", name)
			}
			continue
		}
		if len(p.data.RemoteFileNums) > 0 && strings.HasPrefix(name, remoteCatalogFilenameBase+"-") {
			continue
		}
		return errors.Errorf("pebble: unexpected file %q in checkpoint directory; refusing to mix checkpoints", name)
	}
	return nil
}

// parseCheckpointMarkerName validates that name is a well-formed atomicfs
// marker file (mirroring atomicfs' internal parsing requirements).
func parseCheckpointMarkerName(name string) (markerName string, iter uint64, value string, err error) {
	const prefix = "marker."
	if !strings.HasPrefix(name, prefix) {
		return "", 0, "", errors.Newf("invalid marker filename: %q", name)
	}
	rest := name[len(prefix):]
	dot := strings.IndexByte(rest, '.')
	if dot < 0 {
		return "", 0, "", errors.Newf("invalid marker filename: %q", name)
	}
	markerName = rest[:dot]
	rest = rest[dot+1:]
	dot = strings.IndexByte(rest, '.')
	if dot < 0 {
		return "", 0, "", errors.Newf("invalid marker filename: %q", name)
	}
	if _, err = fmt.Sscanf(rest[:dot], "%06d", &iter); err != nil {
		return "", 0, "", errors.Newf("invalid marker filename: %q", name)
	}
	return markerName, iter, rest[dot+1:], nil
}

// ---------------------------------------------------------------------------
// Isolated verification directory.
// ---------------------------------------------------------------------------

var verifyDirCounter atomic.Uint64

func (p *CheckpointPlan) uniqueVerifyDirLocked() string {
	n := verifyDirCounter.Add(1)
	baseName := p.fs.PathBase(p.destDir)
	parent := p.fs.PathDir(p.destDir)
	return p.fs.PathJoin(parent, fmt.Sprintf("%s%s%d-%d",
		baseName, verifyDirPrefix, time.Now().UnixNano(), n))
}

func (p *CheckpointPlan) staleVerifyDirsLocked() ([]string, error) {
	parent := p.fs.PathDir(p.destDir)
	baseName := p.fs.PathBase(p.destDir)
	prefix := baseName + verifyDirPrefix
	ls, err := p.fs.List(parent)
	if err != nil {
		if oserror.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, n := range ls {
		if strings.HasPrefix(n, prefix) {
			out = append(out, p.fs.PathJoin(parent, n))
		}
	}
	return out, nil
}

func (p *CheckpointPlan) removeStaleVerifyDirsLocked(_ context.Context) error {
	dirs, err := p.staleVerifyDirsLocked()
	if err != nil {
		return err
	}
	for _, d := range dirs {
		if err := p.fs.RemoveAll(d); err != nil && !oserror.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// createVerifyDirLocked builds an isolated directory containing hard links
// (or copies) of every staged file plus the required markers, suitable for a
// read-only verification Open.
func (p *CheckpointPlan) createVerifyDirLocked(ctx context.Context) (string, error) {
	if err := p.removeStaleVerifyDirsLocked(ctx); err != nil {
		return "", err
	}
	verifyDir := p.uniqueVerifyDirLocked()
	if _, err := mkdirAllAndSyncParents(p.fs, verifyDir); err != nil {
		return "", err
	}
	for i := range p.data.Files {
		f := &p.data.Files[i]
		if err := ctx.Err(); err != nil {
			_ = p.fs.RemoveAll(verifyDir)
			return "", err
		}
		src := p.fs.PathJoin(p.destDir, f.Name)
		dst := p.fs.PathJoin(verifyDir, f.Name)
		if err := vfs.LinkOrCopy(p.fs, src, dst); err != nil {
			_ = p.fs.RemoveAll(verifyDir)
			return "", err
		}
	}
	// Reproduce the brief committed state in which the staging sentinel is
	// still present alongside the markers: it must not prevent the
	// checkpoint from opening.
	if _, err := p.fs.Stat(p.fs.PathJoin(p.destDir, stagingSentinelName)); err == nil {
		if err := vfs.LinkOrCopy(p.fs,
			p.fs.PathJoin(p.destDir, stagingSentinelName),
			p.fs.PathJoin(verifyDir, stagingSentinelName)); err != nil {
			_ = p.fs.RemoveAll(verifyDir)
			return "", err
		}
	} else if !oserror.IsNotExist(err) {
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	// Format version marker.
	vm, _, err := atomicfs.LocateMarker(p.fs, verifyDir, formatVersionMarkerName)
	if err != nil {
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	if err := vm.Move(FormatMajorVersion(p.data.FormatMajorVersion).String()); err != nil {
		_ = vm.Close()
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	if err := vm.Close(); err != nil {
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	// MANIFEST marker.
	mm, _, err := atomicfs.LocateMarker(p.fs, verifyDir, manifestMarkerName)
	if err != nil {
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	if err := mm.SyncDir(); err != nil {
		_ = mm.Close()
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	if err := mm.Move(base.MakeFilename(base.FileTypeManifest, base.DiskFileNum(p.data.ManifestFileNum))); err != nil {
		_ = mm.Close()
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	if err := mm.Close(); err != nil {
		_ = p.fs.RemoveAll(verifyDir)
		return "", err
	}
	return verifyDir, nil
}

func (p *CheckpointPlan) verifyOptionsLocked() *Options {
	o := &Options{
		FS:                    p.base,
		ReadOnly:              true,
		FormatMajorVersion:    FormatMajorVersion(p.data.FormatMajorVersion),
		Comparer:              p.d.opts.Comparer,
		Merger:                p.d.opts.Merger,
		RemoteStorage:         p.d.opts.RemoteStorage,
		CreateOnShared:        p.d.opts.CreateOnShared,
		CreateOnSharedLocator: p.d.opts.CreateOnSharedLocator,
		Logger:                p.d.opts.Logger,
	}
	o.EnsureDefaults()
	return o
}

// verifyOpenableLocked opens the committed checkpoint read-only, scans it
// and closes it.
func (p *CheckpointPlan) verifyOpenableLocked(ctx context.Context, dir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o := p.verifyOptionsLocked()
	d, err := Open(dir, o)
	if err != nil {
		return err
	}
	iter, _ := d.NewIter(nil)
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			_ = d.Close()
			return err
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		_ = d.Close()
		return err
	}
	if err := iter.Close(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// ---------------------------------------------------------------------------
// Plan persistence.
// ---------------------------------------------------------------------------

func (p *CheckpointPlan) saveLocked() error {
	if err := p.fs.MkdirAll(p.tmpDir, 0755); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(&p.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := p.fs.PathJoin(p.tmpDir, resumablePlanFileName)
	f, err := p.fs.Create(tmp, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := p.fs.Rename(tmp, p.planPath); err != nil {
		return err
	}
	dir, err := p.fs.OpenDir(p.stagingDir)
	if err != nil {
		return err
	}
	return errors.CombineErrors(dir.Sync(), dir.Close())
}

func (p *CheckpointPlan) loadLocked() error {
	f, err := p.fs.Open(p.planPath)
	if err != nil {
		return errors.Wrapf(err, "pebble: no checkpoint plan found in %q", p.destDir)
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, &p.data); err != nil {
		return errors.Wrap(err, "decoding checkpoint plan")
	}
	return nil
}

func checkpointPlanPresent(fs vfs.FS, destDir string) (bool, error) {
	planPath := fs.PathJoin(fs.PathJoin(destDir, resumableStagingDirName), resumablePlanFileName)
	if _, err := fs.Stat(planPath); err == nil {
		return true, nil
	} else if !oserror.IsNotExist(err) {
		return false, err
	}
	committed, err := isCheckpointCommitted(fs, destDir)
	return committed, err
}

func isCheckpointCommitted(fs vfs.FS, destDir string) (bool, error) {
	v, err := atomicfs.ReadMarker(fs, destDir, checkpointCommitMarkerName)
	if err != nil {
		if oserror.IsNotExist(err) {
			return false, nil
		}
		// ReadMarker lists the directory; a missing destination is not an
		// existing checkpoint.
		if _, statErr := fs.Stat(destDir); statErr != nil && oserror.IsNotExist(statErr) {
			return false, nil
		}
		return false, err
	}
	return v == checkpointCommitValue, nil
}

// ---------------------------------------------------------------------------
// In-process plan registry and misc helpers.
// ---------------------------------------------------------------------------

func (d *DB) registerCheckpointPlan(destDir string) error {
	d.checkpointPlans.mu.Lock()
	defer d.checkpointPlans.mu.Unlock()
	if d.checkpointPlans.active == nil {
		d.checkpointPlans.active = make(map[string]struct{})
	}
	if _, ok := d.checkpointPlans.active[destDir]; ok {
		return errors.Errorf("pebble: checkpoint plan for %q is already active", destDir)
	}
	d.checkpointPlans.active[destDir] = struct{}{}
	return nil
}

func (d *DB) unregisterCheckpointPlan(destDir string) {
	d.checkpointPlans.mu.Lock()
	defer d.checkpointPlans.mu.Unlock()
	delete(d.checkpointPlans.active, destDir)
}

func remoteNumsFromPlan(nums []uint64) []base.DiskFileNum {
	out := make([]base.DiskFileNum, 0, len(nums))
	for _, n := range nums {
		out = append(out, base.DiskFileNum(n))
	}
	return out
}

func (p *CheckpointPlan) uniqueTmpPathLocked(name string) string {
	n := verifyDirCounter.Add(1)
	return p.fs.PathJoin(p.tmpDir, fmt.Sprintf("%d-%s", n, name))
}

// validateSourceIdentity verifies that a planned source file still exists
// with the exact size and checksum recorded by the plan.
func validateSourceIdentity(ctx context.Context, fs vfs.FS, f *resumablePlanFile) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := fs.Stat(f.SourcePath)
	if err != nil {
		return errors.Wrapf(err, "pebble: source file %s required by checkpoint plan is unavailable", f.Name)
	}
	if info.Size() != f.SourceSize {
		return errors.Errorf(
			"pebble: source file %s changed since checkpoint plan creation (size %d, expected %d)",
			f.Name, info.Size(), f.SourceSize)
	}
	limit := int64(-1)
	if f.Kind == planFileKindManifest {
		limit = f.SourceSize
	}
	_, h, err := hashFile(ctx, fs, f.SourcePath, limit)
	if err != nil {
		return err
	}
	if h != f.SourceHash {
		return errors.Errorf(
			"pebble: source file %s changed since checkpoint plan creation (checksum mismatch)", f.Name)
	}
	return nil
}

// contextReader aborts reads when the context is done.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *contextReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := c.r.Read(p)
	return n, err
}

// linkOrCopyChecked creates dst as a hard link to src or, if hard links are
// unavailable, streams src through a xxhash64 tee into dst. The result must
// match wantSize/wantHash.
func linkOrCopyChecked(
	ctx context.Context, fs vfs.FS, src, dst string, wantSize int64, wantHash uint64,
) (retErr error) {
	if err := fs.Link(src, dst); err == nil {
		info, statErr := fs.Stat(dst)
		if statErr != nil {
			return statErr
		}
		if info.Size() != wantSize {
			return errors.Errorf("linked file %s has size %d, expected %d", dst, info.Size(), wantSize)
		}
		_, h, err := hashFile(ctx, fs, dst, -1)
		if err != nil {
			return err
		}
		if h != wantHash {
			return errors.Errorf("linked file %s checksum mismatch", dst)
		}
		return nil
	} else if oserror.IsExist(err) || oserror.IsNotExist(err) || oserror.IsPermission(err) {
		return err
	}

	in, err := fs.Open(src)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.CombineErrors(retErr, in.Close()) }()
	out, err := fs.Create(dst, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	h := xxhash.New()
	tr := io.TeeReader(&contextReader{ctx: ctx, r: in}, h)
	n, err := io.Copy(out, tr)
	if err != nil {
		_ = out.Close()
		_ = fs.Remove(dst)
		return err
	}
	if n != wantSize {
		_ = out.Close()
		_ = fs.Remove(dst)
		return errors.Errorf("copied file %s has size %d, expected %d", dst, n, wantSize)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = fs.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		_ = fs.Remove(dst)
		return err
	}
	if h.Sum64() != wantHash {
		_ = fs.Remove(dst)
		return errors.Errorf("copied file %s checksum mismatch", dst)
	}
	return nil
}

// hashFile computes the xxhash64 of the first limit bytes of the file (all
// bytes if limit < 0) and returns the number of bytes hashed.
func hashFile(ctx context.Context, fs vfs.FS, path string, limit int64) (int64, uint64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	var r io.Reader = f
	if limit >= 0 {
		r = io.LimitReader(f, limit)
	}
	h := xxhash.New()
	n, err := io.Copy(h, &contextReader{ctx: ctx, r: r})
	if err != nil {
		return n, 0, err
	}
	if limit >= 0 && n != limit {
		return n, h.Sum64(), errors.Errorf("file %s shorter than expected: %d < %d", path, n, limit)
	}
	return n, h.Sum64(), nil
}

func hashFilePrefixSum(ctx context.Context, fs vfs.FS, path string, limit int64) (uint64, error) {
	_, h, err := hashFile(ctx, fs, path, limit)
	return h, err
}

// logicalLogHasRecords reports whether the logical WAL contains at least one
// record. A truncated tail is treated as end-of-log, mirroring WAL replay.
func logicalLogHasRecords(ctx context.Context, ll wal.LogicalLog) (bool, error) {
	r := ll.OpenForRead()
	defer r.Close()
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		_, _, err := r.NextRecord()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, record.ErrUnexpectedEOF) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
}
