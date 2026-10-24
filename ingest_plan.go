// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// This file implements the experimental, default-off two-phase batch ingest
// API: PrepareIngestPlan followed by CommitIngestPlan.
//
// A plan is an immutable description of a batch of external local sstables,
// prepared against the current DB version. Besides per-file metadata and
// checksums, the plan records a fingerprint of the version it was prepared
// against and the target level decided for every file. Committing the plan
// re-verifies (optimistically, while holding the manifest log lock) that the
// version, the source files, in-progress compactions and target level
// decisions still match the plan, then publishes all files through a single
// VersionEdit. Any failure aborts before the edit is logged and cleans up the
// linked files, so no partial batch is ever visible. Plans live only in
// memory: reopening the DB invalidates them.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/overlap"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// Sentinel errors returned by the experimental ingest plan API.
var (
	// ErrIngestPlansDisabled is returned when
	// Options.ExperimentalBatchIngestPlans is false.
	ErrIngestPlansDisabled = errors.New(
		"pebble: batch ingest plans are disabled (set Options.ExperimentalBatchIngestPlans)")
	// ErrIngestPlanNotFound is returned when the submitted plan is not known
	// to this DB instance, including after the database has been closed and
	// reopened (plans are not persisted).
	ErrIngestPlanNotFound = errors.New(
		"pebble: ingest plan not found (unknown plan, or the database was reopened)")
	// ErrIngestPlanExpired is returned when the DB version, target level
	// decisions or in-progress compactions no longer satisfy the plan. The
	// caller may prepare a fresh plan against the current version and retry.
	ErrIngestPlanExpired = errors.New(
		"pebble: ingest plan expired: the db version, target levels or compaction state changed")
	// ErrIngestPlanFileModified is returned when one of the plan's source
	// files has been replaced, removed or modified since the plan was
	// prepared.
	ErrIngestPlanFileModified = errors.New(
		"pebble: ingest plan source file missing, modified or replaced")
)

// IngestChecksumAlgorithm identifies the digest algorithm used to verify the
// contents of a planned sstable.
type IngestChecksumAlgorithm uint8

const (
	// IngestChecksumUnknown is the zero value.
	IngestChecksumUnknown IngestChecksumAlgorithm = 0
	// IngestChecksumSHA256 is SHA-256 over the full file contents.
	IngestChecksumSHA256 IngestChecksumAlgorithm = 1
)

// String implements fmt.Stringer.
func (a IngestChecksumAlgorithm) String() string {
	switch a {
	case IngestChecksumSHA256:
		return "SHA-256"
	default:
		return "unknown"
	}
}

// IngestPlanID is the stable identity of an ingest plan. It is derived
// deterministically from the plan's files (paths, digests and metadata), the
// DB format major version and the fingerprint of the version the plan was
// prepared against. Re-preparing an identical plan against the same version
// yields the same ID.
type IngestPlanID [sha256.Size]byte

// String returns the hex-encoded plan ID.
func (id IngestPlanID) String() string {
	return fmt.Sprintf("%x", id[:])
}

// IngestVersionFingerprint summarizes the set of on-disk tables and blob
// files in a DB version. Any flush, compaction or ingest changes the
// fingerprint.
type IngestVersionFingerprint [sha256.Size]byte

// String returns the hex-encoded fingerprint.
func (f IngestVersionFingerprint) String() string {
	return fmt.Sprintf("%x", f[:])
}

// IngestPlanFile is the immutable description of one external local sstable
// within an IngestPlan.
type IngestPlanFile struct {
	// Path is the filesystem path of the external sstable.
	Path string
	// Size is the size of the file in bytes at plan preparation time.
	Size uint64
	// FormatVersion is the sstable table format.
	FormatVersion sstable.TableFormat
	// Smallest and Largest are the user key bounds of the table. If
	// LargestIsExclusive is true, the upper bound is [Largest); otherwise it
	// is inclusive.
	Smallest           []byte
	Largest            []byte
	LargestIsExclusive bool
	// HasPointKeys is true if the table contains (or may contain) point
	// keys, including range deletions.
	HasPointKeys bool
	// HasRangeKeys is true if the table contains (or may contain) range
	// keys.
	HasRangeKeys bool
	// RangeKeyKinds mirrors manifest.RangeKeyKinds: 0 = no range keys,
	// 1 = only RangeKeyUnset/Delete, 2 = any range keys.
	RangeKeyKinds uint8
	// NumEntries, NumRangeDeletions and NumRangeKeySets come from the
	// sstable properties block.
	NumEntries        uint64
	NumRangeDeletions uint64
	NumRangeKeySets   uint64
	// SmallestSeqNum and LargestSeqNum are the sequence number range of the
	// keys in the external table. External sstables must carry sequence
	// number 0 on all keys, so both values are 0 for a valid plan.
	SmallestSeqNum SeqNum
	LargestSeqNum  SeqNum
	// ChecksumAlgorithm and Checksum identify the file contents. Commit
	// re-hashes the file and requires the digest to match.
	ChecksumAlgorithm IngestChecksumAlgorithm
	Checksum          [sha256.Size]byte
	// TargetLevel is the level that was decided for this file when the plan
	// was prepared. Commit re-derives the level and requires it to match.
	TargetLevel int
}

// IngestPlan is an immutable plan for atomically ingesting a batch of
// external local sstables. Plans are created with DB.PrepareIngestPlan and
// committed with DB.CommitIngestPlan. A plan must not be modified after it
// has been prepared.
type IngestPlan struct {
	// ID is the stable, content-derived identity of the plan.
	ID IngestPlanID
	// FormatMajorVersion is the DB format major version at prepare time.
	FormatMajorVersion FormatMajorVersion
	// VersionFingerprint is the fingerprint of the DB version the target
	// levels were computed against.
	VersionFingerprint IngestVersionFingerprint
	// Files describes every sstable in the batch, sorted by smallest user
	// key.
	Files []IngestPlanFile
	// PreparedAt is the time at which the plan was generated.
	PreparedAt time.Time
}

// ingestPlanStatus is the lifecycle state of a plan in the in-memory
// registry.
type ingestPlanStatus uint8

const (
	ingestPlanPrepared ingestPlanStatus = iota
	ingestPlanCommitted
	ingestPlanExpired
	ingestPlanInvalid
)

// ingestPlanState is the registry entry for a prepared plan.
type ingestPlanState struct {
	plan   *IngestPlan
	status ingestPlanStatus
	// stats is populated once the plan commits successfully and returned by
	// idempotent duplicate commits.
	stats IngestOperationStats
}

// ingestPlanFileAnalysis is the internal per-file analysis result: the plan
// file description plus the loaded table metadata used by batch-wide checks.
type ingestPlanFileAnalysis struct {
	planFile IngestPlanFile
	res      ingestLocalResult
}

// PrepareIngestPlan validates a batch of external local sstables and produces
// an immutable ingest plan against the current DB version. It is read-only
// with respect to the database: it does not link files, modify the manifest
// or consume the input files. It allocates throwaway table numbers from the
// DB's file number counter to isolate block cache state; committing the plan
// allocates fresh numbers.
//
// Preparation verifies file formats, key and sequence number validity,
// non-overlap of the batch, absence of duplicate files, range
// deletion/point-key conflicts and target level constraints, and computes a
// checksum for every file and a fingerprint of the current version.
//
// Commit the returned plan with CommitIngestPlan. Plans are not persisted:
// after the database is reopened, committing an old plan fails with
// ErrIngestPlanNotFound.
func (d *DB) PrepareIngestPlan(
	ctx context.Context, paths []string,
) (*IngestPlan, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if !d.opts.ExperimentalBatchIngestPlans {
		return nil, ErrIngestPlansDisabled
	}
	if d.opts.ReadOnly {
		return nil, ErrReadOnly
	}
	if len(paths) == 0 {
		return nil, errors.New("pebble: cannot prepare an ingest plan without files")
	}
	fmv := d.FormatMajorVersion()

	// Allocate unique table numbers for the read-only analysis. The numbers
	// are never written to the manifest (real numbers are allocated at commit
	// time), but they must be unique so that the block cache cannot serve
	// blocks from a different planned or ingested file. Skipped plans leave
	// harmless gaps in the file number sequence, just like failed ingests do.
	tableNums := make([]base.TableNum, len(paths))
	for i := range paths {
		tableNums[i] = base.TableNum(d.mu.versions.getNextDiskFileNum())
	}

	// Load and verify every file independently.
	analyses := make([]ingestPlanFileAnalysis, 0, len(paths))
	seenPaths := make(map[string]struct{}, len(paths))
	seenSums := make(map[[sha256.Size]byte]string, len(paths))
	for i, path := range paths {
		if _, ok := seenPaths[path]; ok {
			return nil, errors.Newf("pebble: ingest plan contains duplicate file path %q", path)
		}
		seenPaths[path] = struct{}{}

		a, err := d.analyzeSSTForPlan(ctx, fmv, path, tableNums[i])
		if err != nil {
			return nil, err
		}
		if other, ok := seenSums[a.planFile.Checksum]; ok {
			return nil, errors.Newf(
				"pebble: ingest plan contains duplicate files: %q and %q have identical contents",
				other, path)
		}
		seenSums[a.planFile.Checksum] = path
		analyses = append(analyses, a)
	}

	cmp := d.opts.Comparer.Compare
	// Sort by smallest user key so that the plan has a canonical file order.
	sort.Slice(analyses, func(i, j int) bool {
		c := cmp(analyses[i].res.meta.Smallest().UserKey, analyses[j].res.meta.Smallest().UserKey)
		if c != 0 {
			return c < 0
		}
		return analyses[i].planFile.Path < analyses[j].planFile.Path
	})

	// Check range deletions against point keys of the other files of the
	// batch. This runs before the generic overlap check so the more specific
	// diagnostic wins when both conditions hold.
	for i := range analyses {
		path := analyses[i].planFile.Path
		for _, span := range analyses[i].res.rangeDelSpans {
			for j := range analyses {
				if i == j || !analyses[j].res.meta.HasPointKeys {
					continue
				}
				pointBounds := analyses[j].res.meta.UserKeyBoundsByType(manifest.KeyTypePoint)
				if span.Overlaps(cmp, pointBounds) {
					return nil, errors.Newf(
						"pebble: range deletion %s in %q conflicts with point keys in %q",
						span, path, analyses[j].planFile.Path)
				}
			}
		}
	}

	// Replay the suffixed range key boundary validation that the regular
	// local ingest path performs while loading files, but in the canonical
	// sorted order. Commit loads the files in this same order, so a batch
	// that passes preparation cannot fail this check at commit time. The
	// regular loader disables these checks when CreateOnShared is set (see
	// ingestLoad); mirror that predicate.
	if d.opts.CreateOnShared == remote.CreateOnSharedNone {
		var lastRangeKey keyspan.Span
		for i := range analyses {
			validator := validateSuffixedBoundaries(d.opts.Comparer, lastRangeKey)
			var first *keyspan.Span
			if !analyses[i].res.firstRangeKey.Empty() {
				span := analyses[i].res.firstRangeKey
				first = &span
			}
			if err := validator.Validate(first); err != nil {
				return nil, err
			}
			lastRangeKey = analyses[i].res.lastRangeKey
		}
		finalValidator := validateSuffixedBoundaries(d.opts.Comparer, lastRangeKey)
		if err := finalValidator.Validate(nil); err != nil {
			return nil, err
		}
	}

	// Verify the files do not overlap one another, mirroring the regular
	// ingest path's invariants.
	for i := 1; i < len(analyses); i++ {
		if sstableKeyCompare(
			cmp, analyses[i-1].res.meta.Largest(), analyses[i].res.meta.Smallest()) >= 0 {
			return nil, errors.Newf(
				"pebble: ingest plan sstables have overlapping ranges: %q and %q",
				analyses[i-1].planFile.Path, analyses[i].planFile.Path)
		}
	}

	// Compute target levels and the version fingerprint while holding the
	// manifest log lock, so the version cannot change between the overlap
	// analysis and fingerprinting. This deliberately mirrors ingestApply.
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.versions.logLock()
	defer d.mu.versions.logUnlock()

	current := d.mu.versions.currentVersion()
	overlapChecker := &overlapChecker{
		comparer: d.opts.Comparer,
		newIters: d.newIters,
		opts: IterOptions{
			logger:   d.opts.Logger,
			Category: categoryIngest,
		},
		v:           current,
		objProvider: d.objProvider,
	}
	baseLevel := d.mu.versions.picker.getBaseLevel()

	planFiles := make([]IngestPlanFile, len(analyses))
	for i := range analyses {
		m := analyses[i].res.meta
		var lsmOverlap overlap.WithLSM
		if err := func() error {
			// We can release DB.mu while probing the LSM (this may perform
			// I/O), but we continue to hold the manifest log lock, so the
			// current version cannot change.
			d.mu.Unlock()
			defer d.mu.Lock()
			var err error
			lsmOverlap, err = overlapChecker.DetermineLSMOverlap(ctx, m.UserKeyBounds())
			return err
		}(); err != nil {
			return nil, err
		}
		targetLevel, splitFile, err := ingestTargetLevel(
			ctx, cmp, lsmOverlap, baseLevel, d.mu.compact.inProgress, m,
			false, /* suggestSplit */
		)
		if err != nil {
			return nil, err
		}
		if splitFile != nil {
			return nil, base.AssertionFailedf(
				"pebble: ingest plan target level computation returned a split file")
		}
		if targetLevel < 0 || targetLevel >= numLevels {
			return nil, base.AssertionFailedf(
				"pebble: ingest plan target level %d out of range", targetLevel)
		}
		analyses[i].planFile.TargetLevel = targetLevel
		planFiles[i] = analyses[i].planFile
	}

	plan := &IngestPlan{
		FormatMajorVersion: fmv,
		VersionFingerprint: computeVersionFingerprint(current),
		Files:              planFiles,
		PreparedAt:         d.opts.private.timeNow(),
	}
	plan.ID = plan.computeID()

	if d.mu.ingestPlans.plans == nil {
		d.mu.ingestPlans.plans = make(map[IngestPlanID]*ingestPlanState)
	}
	// Re-preparing the same batch against the same version is deterministic.
	// A prepared or committed plan is returned as-is (a committed plan's
	// sources have been consumed); a plan that expired or was invalidated
	// without changing the version is replaced with a fresh prepared plan,
	// allowing the caller to retry a failed commit.
	if existing, ok := d.mu.ingestPlans.plans[plan.ID]; ok &&
		(existing.status == ingestPlanPrepared || existing.status == ingestPlanCommitted) {
		return existing.plan, nil
	}
	d.mu.ingestPlans.plans[plan.ID] = &ingestPlanState{
		plan:   plan,
		status: ingestPlanPrepared,
	}
	return plan, nil
}

// CommitIngestPlan atomically ingests the batch described by the plan.
//
// Commit first re-hashes every source file and compares its identity and
// checksum with the plan. It then runs the regular ingest machinery, extended
// by an optimistic verification step performed while holding the manifest
// log lock: the current version fingerprint, the target level of every file
// and in-progress compactions must still match the plan. All files are then
// published in a single VersionEdit; if any step fails, no version edit is
// logged and all linked files are cleaned up, so the LSM either contains the
// complete batch or none of it.
//
// Committing the same plan more than once is idempotent: after a successful
// commit, subsequent commits succeed and return the original
// IngestOperationStats without ingesting anything again. Expired plans,
// modified source files, unknown plans and plans from before a DB reopen
// return distinct errors.
func (d *DB) CommitIngestPlan(
	ctx context.Context, plan *IngestPlan,
) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if !d.opts.ExperimentalBatchIngestPlans {
		return IngestOperationStats{}, ErrIngestPlansDisabled
	}
	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}
	if plan == nil {
		return IngestOperationStats{}, errors.New("pebble: cannot commit a nil ingest plan")
	}

	// Serialize plan commits so concurrent duplicates are observed as
	// idempotent retries rather than racing versions.
	d.ingestPlanCommitMu.Lock()
	defer d.ingestPlanCommitMu.Unlock()

	// The state entry is looked up under DB.mu. It is subsequently read
	// without DB.mu, which is safe: status transitions only happen in this
	// function (serialized by ingestPlanCommitMu), and re-preparation never
	// mutates an existing entry but replaces it with a new state object.
	d.mu.Lock()
	st := d.mu.ingestPlans.plans[plan.ID]
	d.mu.Unlock()
	if st == nil {
		return IngestOperationStats{},
			errors.Newf("%w: plan %s", ErrIngestPlanNotFound, plan.ID)
	}
	// Only the plan object returned by PrepareIngestPlan may be committed.
	if st.plan != plan {
		return IngestOperationStats{},
			errors.Newf("%w: plan %s was not prepared by this database instance",
				ErrIngestPlanNotFound, plan.ID)
	}
	switch st.status {
	case ingestPlanCommitted:
		// Idempotent duplicate commit.
		return st.stats, nil
	case ingestPlanExpired:
		return IngestOperationStats{},
			errors.Newf("%w: plan %s", ErrIngestPlanExpired, plan.ID)
	case ingestPlanInvalid:
		return IngestOperationStats{},
			errors.Newf("%w: plan %s", ErrIngestPlanFileModified, plan.ID)
	}

	// Re-confirm every source file still has the identity and contents
	// recorded in the plan, before linking anything.
	for i := range plan.Files {
		fp := &plan.Files[i]
		sum, size, err := digestExternalSST(d.opts.FS, fp.Path)
		if err != nil {
			d.setIngestPlanStatus(plan.ID, ingestPlanInvalid)
			return IngestOperationStats{}, errors.Newf(
				"%w: cannot verify %q: %v", ErrIngestPlanFileModified, fp.Path, err)
		}
		if size != fp.Size || sum != fp.Checksum {
			d.setIngestPlanStatus(plan.ID, ingestPlanInvalid)
			return IngestOperationStats{}, errors.Newf(
				"%w: %q", ErrIngestPlanFileModified, fp.Path)
		}
	}

	local := make([]LocalSST, len(plan.Files))
	for i := range plan.Files {
		local[i] = LocalSST{Path: plan.Files[i].Path}
	}
	stats, err := d.ingest(ctx, ingestArgs{
		Local:       local,
		noFlushable: true,
		planVerification: func(
			ve *manifest.VersionEdit, lr ingestLoadResult, current *manifest.Version,
		) error {
			return d.verifyIngestPlanForCommit(plan, ve, lr, current)
		},
	})
	if err != nil {
		if errors.Is(err, ErrIngestPlanExpired) {
			d.setIngestPlanStatus(plan.ID, ingestPlanExpired)
		}
		return IngestOperationStats{}, err
	}
	d.setIngestPlanStatus(plan.ID, ingestPlanCommitted)
	d.setIngestPlanStats(plan.ID, stats)
	return stats, nil
}

// verifyIngestPlanForCommit is the optimistic commit-time gate. It runs
// inside UpdateVersionLocked (DB.mu and the manifest log lock held), after the
// target level for every new table has been decided but before the version
// edit is logged. A non-nil error prevents the edit from being applied.
func (d *DB) verifyIngestPlanForCommit(
	plan *IngestPlan,
	ve *manifest.VersionEdit,
	lr ingestLoadResult,
	current *manifest.Version,
) error {
	// The version must be exactly the one the plan was prepared against.
	if computeVersionFingerprint(current) != plan.VersionFingerprint {
		return errors.Newf(
			"%w: db version fingerprint changed since plan %s was prepared",
			ErrIngestPlanExpired, plan.ID)
	}

	if len(ve.NewTables) != len(plan.Files) {
		return errors.Newf(
			"%w: plan %s contains %d files but the version edit contains %d new tables",
			ErrIngestPlanExpired, plan.ID, len(plan.Files), len(ve.NewTables))
	}

	// Map the freshly allocated table metadata back to source paths.
	pathByTableNum := make(map[base.TableNum]string, lr.sstCount())
	for i := range lr.local {
		pathByTableNum[lr.local[i].TableNum] = lr.local[i].local.Path
	}
	plannedLevel := make(map[string]int, len(plan.Files))
	for i := range plan.Files {
		plannedLevel[plan.Files[i].Path] = plan.Files[i].TargetLevel
	}
	for _, e := range ve.NewTables {
		path, ok := pathByTableNum[e.Meta.TableNum]
		if !ok {
			return errors.Newf(
				"%w: table %s is not part of plan %s",
				ErrIngestPlanExpired, e.Meta.TableNum, plan.ID)
		}
		want, ok := plannedLevel[path]
		if !ok {
			return errors.Newf("%w: %q is not part of plan %s",
				ErrIngestPlanExpired, path, plan.ID)
		}
		if e.Level != want {
			return errors.Newf(
				"%w: target level for %q changed from %d to %d",
				ErrIngestPlanExpired, path, want, e.Level)
		}
	}

	// Re-confirm that no in-progress compaction is about to produce a table
	// overlapping a planned file at the planned target level. (ingestTargetLevel
	// already avoided such levels when deciding e.Level, so a conflict here
	// implies the decision would differ from the plan.)
	for c := range d.mu.compact.inProgress {
		tblCompaction, ok := c.(*tableCompaction)
		if !ok || tblCompaction.outputLevel == nil {
			continue
		}
		cb := c.Bounds()
		if cb == nil {
			continue
		}
		for i := range plan.Files {
			f := &plan.Files[i]
			if f.TargetLevel != tblCompaction.outputLevel.level {
				continue
			}
			if cb.Overlaps(d.cmp, planFileUserBounds(f)) {
				return errors.Newf(
					"%w: in-progress compaction at L%d overlaps planned file %q",
					ErrIngestPlanExpired, tblCompaction.outputLevel.level, f.Path)
			}
		}
	}

	if hook := d.opts.private.testingIngestPlanVerifyFunc; hook != nil {
		if err := hook(); err != nil {
			return errors.Newf("%w: injected verification failure: %v",
				ErrIngestPlanExpired, err)
		}
	}
	return nil
}

// analyzeSSTForPlan performs the read-only per-file analysis: it computes a
// full-content checksum and loads the table metadata (which validates the
// format against the DB format major version and key/sequence number rules).
func (d *DB) analyzeSSTForPlan(
	ctx context.Context,
	fmv FormatMajorVersion,
	path string,
	tableNum base.TableNum,
) (ingestPlanFileAnalysis, error) {
	sum, size, err := digestExternalSST(d.opts.FS, path)
	if err != nil {
		return ingestPlanFileAnalysis{}, err
	}
	if size == 0 {
		return ingestPlanFileAnalysis{},
			errors.Newf("pebble: cannot ingest external file %q with size 0", path)
	}

	f, err := d.opts.FS.Open(path)
	if err != nil {
		return ingestPlanFileAnalysis{}, err
	}
	readable, err := objstorage.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		return ingestPlanFileAnalysis{}, err
	}
	res, err := ingestLoad1(
		ctx, d.opts, fmv, readable, d.cacheHandle, &d.compressionCounters,
		tableNum, disableRangeKeyChecks(), nil, /* blobFiles */
		nil, /* blobFileNums */
	)
	if err != nil {
		return ingestPlanFileAnalysis{}, err
	}
	if res.meta == nil {
		return ingestPlanFileAnalysis{},
			errors.Newf("pebble: cannot ingest empty sstable %q", path)
	}

	m := res.meta
	pf := IngestPlanFile{
		Path:               path,
		Size:               size,
		FormatVersion:      res.tableFormat,
		Smallest:           slicesClone(m.Smallest().UserKey),
		Largest:            slicesClone(m.Largest().UserKey),
		LargestIsExclusive: m.Largest().IsExclusiveSentinel(),
		HasPointKeys:       m.HasPointKeys,
		HasRangeKeys:       m.HasRangeKeys,
		RangeKeyKinds:      uint8(m.RangeKeyKinds),
		ChecksumAlgorithm:  IngestChecksumSHA256,
		Checksum:           sum,
	}
	if res.props != nil {
		pf.NumEntries = res.props.NumEntries
		pf.NumRangeDeletions = res.props.NumRangeDeletions
		pf.NumRangeKeySets = res.props.NumRangeKeySets
	}
	pf.SmallestSeqNum, pf.LargestSeqNum = externalSSTSeqNumRange(m)
	return ingestPlanFileAnalysis{
		planFile: pf,
		res:      res,
	}, nil
}

// externalSSTSeqNumRange returns the sequence number range carried by the
// boundary keys of an external sstable. All keys in an external sstable must
// have sequence number 0 (enforced while loading), so both returned values
// are 0 for any file that loads successfully.
func externalSSTSeqNumRange(m *manifest.TableMetadata) (SeqNum, SeqNum) {
	var lo, hi SeqNum
	have := false
	consider := func(k base.InternalKey) {
		if k.IsExclusiveSentinel() {
			return
		}
		sn := k.SeqNum()
		if !have {
			lo, hi, have = sn, sn, true
			return
		}
		if sn < lo {
			lo = sn
		}
		if sn > hi {
			hi = sn
		}
	}
	if m.HasPointKeys {
		consider(m.PointKeyBounds.Smallest())
		consider(m.PointKeyBounds.Largest())
	}
	if m.HasRangeKeys {
		consider(m.RangeKeyBounds.Smallest())
		consider(m.RangeKeyBounds.Largest())
	}
	return lo, hi
}

func planFileUserBounds(f *IngestPlanFile) base.UserKeyBounds {
	return base.UserKeyBoundsEndExclusiveIf(f.Smallest, f.Largest, f.LargestIsExclusive)
}

// setIngestPlanStatus updates the lifecycle state of a plan. DB.mu must not be
// held by the caller.
func (d *DB) setIngestPlanStatus(id IngestPlanID, status ingestPlanStatus) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if st, ok := d.mu.ingestPlans.plans[id]; ok {
		st.status = status
	}
}

// setIngestPlanStats records the stats of a successful commit. DB.mu must not
// be held by the caller.
func (d *DB) setIngestPlanStats(id IngestPlanID, stats IngestOperationStats) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if st, ok := d.mu.ingestPlans.plans[id]; ok {
		st.stats = stats
	}
}

// digestExternalSST opens path, hashes its full contents with SHA-256 and
// returns the digest and size.
func digestExternalSST(
	fs vfs.FS, path string,
) (sum [sha256.Size]byte, size uint64, err error) {
	f, err := fs.Open(path)
	if err != nil {
		return sum, 0, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return sum, 0, err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return sum, 0, err
	}
	if n < 0 || uint64(n) != uint64(st.Size()) {
		return sum, 0, errors.Newf(
			"pebble: file %q changed size while being read (stat: %d, read: %d)",
			path, st.Size(), n)
	}
	copy(sum[:], h.Sum(nil))
	return sum, uint64(n), nil
}

// computeVersionFingerprint summarizes the tables and blob files of a
// version. Iteration orders are deterministic (sorted within each level), so
// identical versions produce identical fingerprints and any flush,
// compaction or ingestion changes the digest.
func computeVersionFingerprint(v *manifest.Version) IngestVersionFingerprint {
	h := sha256.New()
	var buf [8]byte
	writeU64 := func(x uint64) {
		binary.BigEndian.PutUint64(buf[:], x)
		_, _ = h.Write(buf[:])
	}
	writeBytes := func(b []byte) {
		binary.BigEndian.PutUint32(buf[:4], uint32(len(b)))
		_, _ = h.Write(buf[:4])
		_, _ = h.Write(b)
	}
	writeKey := func(k base.InternalKey) {
		writeBytes(k.UserKey)
		writeU64(uint64(k.Trailer))
	}
	for level := 0; level < manifest.NumLevels; level++ {
		writeU64(uint64(level))
		for f := range v.Levels[level].All() {
			writeU64(uint64(f.TableNum))
			writeU64(uint64(f.TableBacking.DiskFileNum))
			writeU64(f.Size)
			if f.Virtual {
				writeBytes([]byte{1})
			} else {
				writeBytes([]byte{0})
			}
			if f.HasPointKeys {
				writeKey(f.PointKeyBounds.Smallest())
				writeKey(f.PointKeyBounds.Largest())
			}
			if f.HasRangeKeys {
				writeKey(f.RangeKeyBounds.Smallest())
				writeKey(f.RangeKeyBounds.Largest())
			}
		}
	}
	for bf := range v.BlobFiles.All() {
		writeU64(uint64(bf.Physical.FileNum))
		writeU64(bf.Physical.Size)
	}
	var fp IngestVersionFingerprint
	copy(fp[:], h.Sum(nil))
	return fp
}

// computeID derives the stable plan ID from the DB format major version, the
// version fingerprint and every file's canonical description.
func (p *IngestPlan) computeID() IngestPlanID {
	h := sha256.New()
	var buf [8]byte
	writeU64 := func(x uint64) {
		binary.BigEndian.PutUint64(buf[:], x)
		_, _ = h.Write(buf[:])
	}
	writeBytes := func(b []byte) {
		binary.BigEndian.PutUint32(buf[:4], uint32(len(b)))
		_, _ = h.Write(buf[:4])
		_, _ = h.Write(b)
	}
	writeU64(uint64(p.FormatMajorVersion))
	_, _ = h.Write(p.VersionFingerprint[:])
	writeU64(uint64(len(p.Files)))
	for i := range p.Files {
		f := &p.Files[i]
		writeBytes([]byte(f.Path))
		writeU64(f.Size)
		writeU64(uint64(f.FormatVersion))
		writeBytes(f.Smallest)
		writeBytes(f.Largest)
		flags := byte(0)
		if f.LargestIsExclusive {
			flags |= 1 << 0
		}
		if f.HasPointKeys {
			flags |= 1 << 1
		}
		if f.HasRangeKeys {
			flags |= 1 << 2
		}
		_, _ = h.Write([]byte{flags, f.RangeKeyKinds, byte(f.ChecksumAlgorithm)})
		writeU64(f.NumEntries)
		writeU64(f.NumRangeDeletions)
		writeU64(f.NumRangeKeySets)
		writeU64(uint64(f.SmallestSeqNum))
		writeU64(uint64(f.LargestSeqNum))
		_, _ = h.Write(f.Checksum[:])
		writeU64(uint64(f.TargetLevel))
	}
	var id IngestPlanID
	copy(id[:], h.Sum(nil))
	return id
}

// slicesClone is a small helper that avoids importing slices solely for
// Clone.
func slicesClone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
