// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

// Format upgrade plans provide an explicit, recoverable way to ratchet a
// database's FormatMajorVersion. The capability is opt-in: it must be enabled
// through Options.EnableFormatUpgradePlan. When the capability is not enabled,
// Open, writes, recovery and RatchetFormatMajorVersion behave exactly as they
// did before the capability existed.
//
// A FormatUpgradePlan is an immutable description of:
//
//   - the database state the plan was generated against (format major
//     version, Comparer, Merger, KeySchema, WAL state, MANIFEST and the live
//     collection of SSTable/blob files),
//   - the ordered list of upgrade steps that will be executed,
//   - the persistent files expected to be touched,
//   - compatibility prerequisites re-checked immediately before execution,
//   - the first irreversible stage,
//   - the recovery marker persisted alongside the database.
//
// Execution proceeds as a sequence of staged commits. Before each migration
// runs, a recovery marker is durably moved to record the stage as prepared.
// The migration itself commits by atomically moving the format-version
// marker; afterwards the recovery marker is moved to record the stage as
// committed. Either marker is an atomicfs marker, so at no point can an
// on-disk state exist that is not recognizable as either the old or the new
// format major version.
//
// If the database is closed or the process crashes mid-upgrade, reopening the
// database detects the recovery marker. When the plan capability is enabled in
// Options, the database opens in a blocked "upgrade pending" state and the
// caller may resubmit the same plan to continue; stages whose format-version
// commit point is already durable are skipped. When the capability is disabled
// (or the database is opened read-only), Open safely rejects opening rather
// than silently interpreting the database under an older format. A corrupted
// recovery marker always causes Open to fail with a structured error.
//
// During an active or pending upgrade:
//
//   - writes (Apply, Set, Delete, Merge, ...), Flush, manual Compact, Ingest,
//     IngestExternalFiles, Download and Checkpoint fail with
//     ErrFormatUpgradeInProgress,
//   - Snapshots and Iterators follow the version rule: they observe the most
//     recently committed stage only; no partially committed stage is ever
//     visible,
//   - background compactions required by the migrations themselves are the
//     only compactions that run.
//
// Submitting the same plan more than once is idempotent: once the plan's
// target version is reached, resubmission is a no-op, and an interrupted plan
// resumes at the first stage whose commit point is not yet durable.

const (
	// formatUpgradePlanMarkerName is the name of the atomicfs marker used to
	// durably record format upgrade plan progress. Marker files on disk are
	// named marker.format-upgrade-plan.<iter>.<value>.
	formatUpgradePlanMarkerName = "format-upgrade-plan"

	// formatUpgradePlanEncoding is the header of an encoded FormatUpgradePlan.
	formatUpgradePlanEncoding = "PBLFMTUP1"

	// Recovery marker value states.
	recoveryStatePrepared  = "p"
	recoveryStateCommitted = "c"
	recoveryStateDone      = "d"
)

// FormatUpgradeErrorReason identifies why a format upgrade plan operation
// failed. Every reason has a corresponding ErrFormatUpgrade* sentinel error;
// use errors.Is to match on the reason and errors.As to retrieve the
// structured *FormatUpgradeError.
type FormatUpgradeErrorReason int8

const (
	// FormatUpgradeDisabledReason is returned when the format upgrade plan
	// capability is not enabled through Options.EnableFormatUpgradePlan.
	FormatUpgradeDisabledReason FormatUpgradeErrorReason = iota
	// FormatUpgradeReadOnlyReason is returned when an operation that requires
	// read-write access is attempted on a database opened read-only.
	FormatUpgradeReadOnlyReason
	// FormatUpgradeInProgressReason is returned when an operation that is
	// blocked while a format upgrade plan is executing or is awaiting
	// continuation is attempted.
	FormatUpgradeInProgressReason
	// FormatUpgradeInvalidPlanReason is returned when a plan is malformed,
	// fails its checksum or references steps that this binary does not
	// support.
	FormatUpgradeInvalidPlanReason
	// FormatUpgradePlanStaleReason is returned when the plan's originating
	// format major version no longer matches the database's current version.
	FormatUpgradePlanStaleReason
	// FormatUpgradeIdentityMismatchReason is returned when the plan is
	// executed against a different database directory than the one it was
	// generated for.
	FormatUpgradeIdentityMismatchReason
	// FormatUpgradeComparerMismatchReason is returned when the configured
	// Comparer is not the Comparer recorded in the plan.
	FormatUpgradeComparerMismatchReason
	// FormatUpgradeMergerMismatchReason is returned when the configured Merger
	// is not the Merger recorded in the plan.
	FormatUpgradeMergerMismatchReason
	// FormatUpgradeKeySchemaMismatchReason is returned when the configured
	// KeySchema is not the KeySchema recorded in the plan.
	FormatUpgradeKeySchemaMismatchReason
	// FormatUpgradeOptionsFingerprintMismatchReason is returned when the
	// Options fingerprint captured by the plan no longer matches the
	// database's effective Options.
	FormatUpgradeOptionsFingerprintMismatchReason
	// FormatUpgradeFileSetMismatchReason is returned when the MANIFEST, live
	// SSTable/blob file collection or live WAL file collection has changed
	// since the plan was generated.
	FormatUpgradeFileSetMismatchReason
	// FormatUpgradeCorruptRecoveryMarkerReason is returned when the persisted
	// recovery marker cannot be parsed or fails its checksum.
	FormatUpgradeCorruptRecoveryMarkerReason
	// FormatUpgradeInconsistentRecoveryMarkerReason is returned when the
	// persisted recovery marker contradicts the on-disk format-version
	// marker (for example, a stage is recorded as committed but the format
	// version was not ratcheted).
	FormatUpgradeInconsistentRecoveryMarkerReason
	// FormatUpgradeIncompleteFilesReason is returned when a live file
	// referenced by the MANIFEST is missing on disk, is empty, or has a size
	// that disagrees with the MANIFEST.
	FormatUpgradeIncompleteFilesReason
	// FormatUpgradePlanMismatchReason is returned when a plan is submitted to
	// continue an in-progress upgrade started by a different plan.
	FormatUpgradePlanMismatchReason
	// FormatUpgradeUnsupportedTargetReason is returned when the plan targets
	// a format major version this binary does not support.
	FormatUpgradeUnsupportedTargetReason
	// FormatUpgradeCannotAbortReason is returned when an abort is requested
	// after an irreversible stage has committed.
	FormatUpgradeCannotAbortReason
	// FormatUpgradeRecoveryRequiredReason is returned from Open when an
	// interrupted format upgrade plan is found on disk but the database cannot
	// be continued under the current Open options (capability disabled or
	// read-only mode). The database must be reopened read-write with the plan
	// capability enabled and the plan resubmitted.
	FormatUpgradeRecoveryRequiredReason
)

// FormatUpgradeError is the structured error returned for all format upgrade
// plan failures.
type FormatUpgradeError struct {
	// Reason classifies the failure.
	Reason FormatUpgradeErrorReason
	// Detail carries additional human-readable context.
	Detail string
	// PlanID is the ID of the plan involved, when known.
	PlanID string
}

// Error implements error.
func (e *FormatUpgradeError) Error() string {
	var b strings.Builder
	b.WriteString("pebble: format upgrade plan")
	if e.PlanID != "" {
		fmt.Fprintf(&b, " %s", e.PlanID)
	}
	b.WriteString(": ")
	b.WriteString(formatUpgradeReasonMessage(e.Reason))
	if e.Detail != "" {
		b.WriteString(": ")
		b.WriteString(e.Detail)
	}
	return b.String()
}

// Is enables errors.Is matching against the reason-specific sentinel errors.
func (e *FormatUpgradeError) Is(target error) bool {
	t, ok := target.(*FormatUpgradeError)
	return ok && t.Reason == e.Reason
}

func formatUpgradeReasonMessage(r FormatUpgradeErrorReason) string {
	switch r {
	case FormatUpgradeDisabledReason:
		return "capability is not enabled (set Options.EnableFormatUpgradePlan)"
	case FormatUpgradeReadOnlyReason:
		return "database is opened read-only"
	case FormatUpgradeInProgressReason:
		return "upgrade is in progress; the operation is blocked"
	case FormatUpgradeInvalidPlanReason:
		return "invalid plan"
	case FormatUpgradePlanStaleReason:
		return "plan is stale"
	case FormatUpgradeIdentityMismatchReason:
		return "database identity mismatch"
	case FormatUpgradeComparerMismatchReason:
		return "comparer mismatch"
	case FormatUpgradeMergerMismatchReason:
		return "merger mismatch"
	case FormatUpgradeKeySchemaMismatchReason:
		return "key schema mismatch"
	case FormatUpgradeOptionsFingerprintMismatchReason:
		return "options fingerprint mismatch"
	case FormatUpgradeFileSetMismatchReason:
		return "persistent file set has changed"
	case FormatUpgradeCorruptRecoveryMarkerReason:
		return "corrupt recovery marker"
	case FormatUpgradeInconsistentRecoveryMarkerReason:
		return "recovery marker is inconsistent with the format version"
	case FormatUpgradeIncompleteFilesReason:
		return "incomplete persistent files"
	case FormatUpgradePlanMismatchReason:
		return "the submitted plan does not match the in-progress plan"
	case FormatUpgradeUnsupportedTargetReason:
		return "unsupported target format major version"
	case FormatUpgradeCannotAbortReason:
		return "the plan cannot be aborted after an irreversible stage has committed"
	case FormatUpgradeRecoveryRequiredReason:
		return "an interrupted format upgrade requires recovery; reopen read-write with Options.EnableFormatUpgradePlan and resubmit the plan"
	default:
		return "unknown error"
	}
}

// Sentinel errors for the individual failure reasons. errors.Is may be used
// with any returned error.
var (
	ErrFormatUpgradeDisabled                 = &FormatUpgradeError{Reason: FormatUpgradeDisabledReason}
	ErrFormatUpgradeReadOnly                 = &FormatUpgradeError{Reason: FormatUpgradeReadOnlyReason}
	ErrFormatUpgradeInProgress               = &FormatUpgradeError{Reason: FormatUpgradeInProgressReason}
	ErrFormatUpgradeInvalidPlan              = &FormatUpgradeError{Reason: FormatUpgradeInvalidPlanReason}
	ErrFormatUpgradePlanStale                = &FormatUpgradeError{Reason: FormatUpgradePlanStaleReason}
	ErrFormatUpgradeIdentityMismatch         = &FormatUpgradeError{Reason: FormatUpgradeIdentityMismatchReason}
	ErrFormatUpgradeComparerMismatch         = &FormatUpgradeError{Reason: FormatUpgradeComparerMismatchReason}
	ErrFormatUpgradeMergerMismatch           = &FormatUpgradeError{Reason: FormatUpgradeMergerMismatchReason}
	ErrFormatUpgradeKeySchemaMismatch        = &FormatUpgradeError{Reason: FormatUpgradeKeySchemaMismatchReason}
	ErrFormatUpgradeOptionsFingerprintMatch  = &FormatUpgradeError{Reason: FormatUpgradeOptionsFingerprintMismatchReason}
	ErrFormatUpgradeFileSetMismatch          = &FormatUpgradeError{Reason: FormatUpgradeFileSetMismatchReason}
	ErrFormatUpgradeCorruptRecoveryMarker    = &FormatUpgradeError{Reason: FormatUpgradeCorruptRecoveryMarkerReason}
	ErrFormatUpgradeInconsistentRecoveryMark = &FormatUpgradeError{Reason: FormatUpgradeInconsistentRecoveryMarkerReason}
	ErrFormatUpgradeIncompleteFiles          = &FormatUpgradeError{Reason: FormatUpgradeIncompleteFilesReason}
	ErrFormatUpgradePlanMismatch             = &FormatUpgradeError{Reason: FormatUpgradePlanMismatchReason}
	ErrFormatUpgradeUnsupportedTarget        = &FormatUpgradeError{Reason: FormatUpgradeUnsupportedTargetReason}
	ErrFormatUpgradeCannotAbort              = &FormatUpgradeError{Reason: FormatUpgradeCannotAbortReason}
	ErrFormatUpgradeRecoveryRequired         = &FormatUpgradeError{Reason: FormatUpgradeRecoveryRequiredReason}
)

// AsFormatUpgradeError extracts a *FormatUpgradeError from err, if err is (or
// wraps) one.
func AsFormatUpgradeError(err error) (*FormatUpgradeError, bool) {
	var fue *FormatUpgradeError
	if errors.As(err, &fue) {
		return fue, true
	}
	return nil, false
}

// formatUpgradeError constructs a structured format upgrade error. Instances
// are matched with errors.Is against the ErrFormatUpgrade* sentinels through
// (*FormatUpgradeError).Is, which works with both the standard library and
// cockroachdb/errors Is implementations.
func formatUpgradeError(
	reason FormatUpgradeErrorReason, detailFormat string, args ...interface{},
) error {
	return &FormatUpgradeError{Reason: reason, Detail: fmt.Sprintf(detailFormat, args...)}
}

// wrapFormatUpgradeError wraps a structured error while preserving the
// reason-based errors.Is chain.
func wrapFormatUpgradeError(
	reason FormatUpgradeErrorReason, cause error, detailFormat string, args ...interface{},
) error {
	detail := fmt.Sprintf(detailFormat, args...)
	if cause != nil {
		detail = detail + ": " + cause.Error()
	}
	return errors.WithStackDepth(&FormatUpgradeError{Reason: reason, Detail: detail}, 2)
}

// formatUpgradeErrorFromErr reports cause's message as the detail.
func formatUpgradeErrorFromErr(reason FormatUpgradeErrorReason, cause error) error {
	return errors.WithStackDepth(&FormatUpgradeError{Reason: reason, Detail: cause.Error()}, 2)
}

// FormatUpgradeStep describes a single one-version ratchet within a
// FormatUpgradePlan.
type FormatUpgradeStep struct {
	// FromVersion is the format major version the database must be at for the
	// step to begin.
	FromVersion FormatMajorVersion
	// ToVersion is the format major version committed by the step.
	ToVersion FormatMajorVersion
	// Description summarizes the migration performed by the step.
	Description string
	// RewritesFiles is true if the migration compacts away (rewrites)
	// existing SSTables.
	RewritesFiles bool
	// MarksFiles is true if the migration marks existing SSTables for
	// compaction in the MANIFEST.
	MarksFiles bool
	// Irreversible is true if committing the step prevents older Pebble
	// binaries from opening the database. Every format version bump is
	// irreversible in this sense.
	Irreversible bool
}

// FormatUpgradeAffectedFile identifies a persistent file (or class of files)
// that plan execution reads, updates, rewrites or deletes.
type FormatUpgradeAffectedFile struct {
	// FileType is one of "sstable", "blob", "manifest", "wal", "options",
	// "format-version-marker" and "recovery-marker".
	FileType string
	// Identifier is the file number, filename or marker name.
	Identifier string
	// Action is one of "created", "updated", "rewritten", "deleted-obsolete",
	// "read" or "none".
	Action string
	// Detail provides human-readable context.
	Detail string
}

// FormatUpgradeFileRef identifies one live physical file.
type FormatUpgradeFileRef struct {
	// FileNum is the physical file number.
	FileNum base.DiskFileNum
	// Size is the file's size in bytes as recorded by the MANIFEST.
	Size uint64
}

// FormatUpgradeFileSet is the immutable collection of persistent files the
// plan was generated against.
type FormatUpgradeFileSet struct {
	// ManifestFileNum is the file number of the active MANIFEST.
	ManifestFileNum base.DiskFileNum
	// Tables are the live physical SSTable backing files, sorted by file
	// number.
	Tables []FormatUpgradeFileRef
	// BlobFiles are the live physical blob files, sorted by file number.
	BlobFiles []FormatUpgradeFileRef
	// WALFiles are the live WAL file numbers, sorted.
	WALFiles []base.DiskFileNum
	// Fingerprint is a hash over the above contents.
	Fingerprint string
}

// FormatUpgradeWALState describes the WAL state captured when the plan was
// generated.
type FormatUpgradeWALState struct {
	// Disabled is true if the WAL is disabled (Options.DisableWAL).
	Disabled bool
	// SyncChunks is true if the current format major version writes WAL sync
	// chunks.
	SyncChunks bool
	// MinUnflushedLogNum is the smallest WAL file number that may still
	// contain unflushed records.
	MinUnflushedLogNum base.DiskFileNum
	// LogNums are the WAL file numbers backing the in-memory flushable queue.
	LogNums []base.DiskFileNum
}

// FormatUpgradePlan is an immutable description of a format major version
// upgrade. Plans are produced by DB.NewFormatUpgradePlan and consumed by
// DB.ExecuteFormatUpgradePlan. The exported fields must not be modified.
type FormatUpgradePlan struct {
	// PlanID uniquely identifies the plan instance.
	PlanID string
	// CreatedAt is when the plan was generated.
	CreatedAt time.Time
	// DBIdentity is the database directory the plan was generated for.
	DBIdentity string
	// FromVersion is the format major version at plan generation time.
	FromVersion FormatMajorVersion
	// ToVersion is the format major version the plan ratchets to.
	ToVersion FormatMajorVersion
	// ComparerName is the name of the Comparer that must remain configured.
	ComparerName string
	// MergerName is the name of the Merger that must remain configured. The
	// empty string denotes no Merger.
	MergerName string
	// KeySchemaName is the name of the KeySchema that must remain
	// configured.
	KeySchemaName string
	// OptionsFingerprint is a fingerprint of the effective Options at plan
	// generation time.
	OptionsFingerprint string
	// WALState is the WAL state at plan generation time.
	WALState FormatUpgradeWALState
	// FileSet is the persistent file collection at plan generation time.
	FileSet FormatUpgradeFileSet
	// Steps are the ordered upgrade steps.
	Steps []FormatUpgradeStep
	// Files lists the persistent files expected to be touched.
	Files []FormatUpgradeAffectedFile
	// Prerequisites lists compatibility prerequisites re-checked immediately
	// before execution.
	Prerequisites []string
	// IrreversibleStage is the index into Steps of the first stage after
	// which rollback is no longer possible, or -1 if the plan contains no
	// steps.
	IrreversibleStage int
}

// Digest returns a stable 128-bit identifier for the plan (as 32 hex
// characters) computed over the database state and steps it describes. Two
// plans generated against identical state have identical digests. The digest
// is recorded in the recovery marker and is used to match plan resubmissions.
func (p *FormatUpgradePlan) Digest() string {
	return fingerprintBytes(emitPlanIdentity(p))
}

// NumStages returns the number of upgrade steps in the plan.
func (p *FormatUpgradePlan) NumStages() int { return len(p.Steps) }

// Encode serializes the plan into a self-contained, checksummed byte string.
// DecodeFormatUpgradePlan reverses the encoding and verifies the checksum.
func (p *FormatUpgradePlan) Encode() ([]byte, error) {
	var b strings.Builder
	b.WriteString(formatUpgradePlanEncoding)
	b.WriteByte('\n')
	fmt.Fprintf(&b, "plan_id %s\n", oneLine(p.PlanID))
	fmt.Fprintf(&b, "created_unix_nano %d\n", p.CreatedAt.UnixNano())
	for _, line := range emitPlanIdentity(p) {
		b.WriteString(line)
		b.WriteByte('\n')
	}
	fmt.Fprintf(&b, "digest %s\n", p.Digest())
	// The checksum covers everything preceding the checksum line, including
	// the digest line.
	checksum := sha256.Sum256([]byte(b.String()))
	fmt.Fprintf(&b, "checksum %s\n", hex.EncodeToString(checksum[:]))
	return []byte(b.String()), nil
}

// DecodeFormatUpgradePlan parses a plan encoded by FormatUpgradePlan.Encode.
// It rejects inputs with bad checksums or whose recorded digest does not
// recompute from the plan contents.
func DecodeFormatUpgradePlan(data []byte) (*FormatUpgradePlan, error) {
	text := strings.TrimSuffix(string(data), "\n")
	lines := strings.Split(text, "\n")
	if len(lines) < 3 || lines[0] != formatUpgradePlanEncoding {
		return nil, formatUpgradeError(FormatUpgradeInvalidPlanReason,
			"unrecognized encoding")
	}
	// The checksum covers everything except the checksum line. The digest
	// line is part of the checksummed body.
	checksumLine := lines[len(lines)-1]
	bodyLines := lines[:len(lines)-1]
	body := strings.Join(bodyLines, "\n") + "\n"
	wantSum, err := parseTaggedLine(checksumLine, "checksum")
	if err != nil {
		return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
	}
	gotSum := sha256.Sum256([]byte(body))
	if wantSum != hex.EncodeToString(gotSum[:]) {
		return nil, formatUpgradeError(FormatUpgradeInvalidPlanReason,
			"checksum mismatch")
	}

	p := &FormatUpgradePlan{IrreversibleStage: -1}
	var digestLine string
	for _, line := range bodyLines[1:] {
		key, value, ok := strings.Cut(line, " ")
		if !ok {
			return nil, formatUpgradeError(FormatUpgradeInvalidPlanReason,
				"malformed line %q", line)
		}
		parseNum := func() (uint64, error) {
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return 0, errors.Wrapf(err, "malformed numeric value %q", value)
			}
			return n, nil
		}
		switch key {
		case "plan_id":
			p.PlanID = value
		case "created_unix_nano":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.CreatedAt = time.Unix(0, n)
		case "db_identity":
			p.DBIdentity = value
		case "from":
			n, err := parseNum()
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.FromVersion = FormatMajorVersion(n)
		case "to":
			n, err := parseNum()
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.ToVersion = FormatMajorVersion(n)
		case "comparer":
			p.ComparerName = value
		case "merger":
			if value == "-" {
				p.MergerName = ""
			} else {
				p.MergerName = value
			}
		case "keyschema":
			p.KeySchemaName = value
		case "options_fp":
			p.OptionsFingerprint = value
		case "fileset_fp":
			p.FileSet.Fingerprint = value
		case "manifest":
			n, err := parseNum()
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.FileSet.ManifestFileNum = base.DiskFileNum(n)
		case "wal_disabled":
			p.WALState.Disabled = value == "1"
		case "wal_sync_chunks":
			p.WALState.SyncChunks = value == "1"
		case "wal_min":
			n, err := parseNum()
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.WALState.MinUnflushedLogNum = base.DiskFileNum(n)
		case "wal":
			n, err := parseNum()
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.FileSet.WALFiles = append(p.FileSet.WALFiles, base.DiskFileNum(n))
			p.WALState.LogNums = append(p.WALState.LogNums, base.DiskFileNum(n))
		case "table":
			fn, size, err := parseFileRef(value)
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.FileSet.Tables = append(p.FileSet.Tables, FormatUpgradeFileRef{FileNum: fn, Size: size})
		case "blob":
			fn, size, err := parseFileRef(value)
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.FileSet.BlobFiles = append(p.FileSet.BlobFiles, FormatUpgradeFileRef{FileNum: fn, Size: size})
		case "step":
			step, err := parseStep(value)
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.Steps = append(p.Steps, step)
		case "file":
			af, err := parseAffectedFile(value)
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason, err)
			}
			p.Files = append(p.Files, af)
		case "prereq":
			p.Prerequisites = append(p.Prerequisites, value)
		case "irreversible_stage":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, formatUpgradeErrorFromErr(FormatUpgradeInvalidPlanReason,
					errors.Wrapf(err, "malformed irreversible_stage %q", value))
			}
			p.IrreversibleStage = n
		case "digest":
			digestLine = value
		default:
			return nil, formatUpgradeError(FormatUpgradeInvalidPlanReason,
				"unknown field %q", key)
		}
	}
	if digestLine == "" {
		return nil, formatUpgradeError(FormatUpgradeInvalidPlanReason,
			"missing digest")
	}
	if got := (&FormatUpgradePlan{
		DBIdentity:         p.DBIdentity,
		FromVersion:        p.FromVersion,
		ToVersion:          p.ToVersion,
		ComparerName:       p.ComparerName,
		MergerName:         p.MergerName,
		KeySchemaName:      p.KeySchemaName,
		OptionsFingerprint: p.OptionsFingerprint,
		WALState:           p.WALState,
		FileSet:            p.FileSet,
		Steps:              p.Steps,
		Files:              p.Files,
		Prerequisites:      p.Prerequisites,
		IrreversibleStage:  p.IrreversibleStage,
	}).Digest(); got != digestLine {
		return nil, formatUpgradeError(FormatUpgradeInvalidPlanReason,
			"digest does not match plan contents")
	}
	return p, nil
}

// emitPlanIdentity renders the plan state covered by Digest, in a fixed order.
func emitPlanIdentity(p *FormatUpgradePlan) []string {
	lines := make([]string, 0, 16+len(p.FileSet.Tables)+len(p.FileSet.BlobFiles)+
		len(p.FileSet.WALFiles)+len(p.Steps)+len(p.Files)+len(p.Prerequisites))
	lines = append(lines, "db_identity "+oneLine(p.DBIdentity))
	lines = append(lines, fmt.Sprintf("from %d", uint64(p.FromVersion)))
	lines = append(lines, fmt.Sprintf("to %d", uint64(p.ToVersion)))
	lines = append(lines, "comparer "+oneLine(p.ComparerName))
	merger := p.MergerName
	if merger == "" {
		merger = "-"
	}
	lines = append(lines, "merger "+oneLine(merger))
	lines = append(lines, "keyschema "+oneLine(p.KeySchemaName))
	lines = append(lines, "options_fp "+oneLine(p.OptionsFingerprint))
	lines = append(lines, "fileset_fp "+oneLine(p.FileSet.Fingerprint))
	lines = append(lines, fmt.Sprintf("manifest %d", uint64(p.FileSet.ManifestFileNum)))
	lines = append(lines, fmt.Sprintf("wal_disabled %d", bool01(p.WALState.Disabled)))
	lines = append(lines, fmt.Sprintf("wal_sync_chunks %d", bool01(p.WALState.SyncChunks)))
	lines = append(lines, fmt.Sprintf("wal_min %d", uint64(p.WALState.MinUnflushedLogNum)))
	for _, fn := range p.FileSet.WALFiles {
		lines = append(lines, fmt.Sprintf("wal %d", uint64(fn)))
	}
	for _, t := range p.FileSet.Tables {
		lines = append(lines, fmt.Sprintf("table %d %d", uint64(t.FileNum), t.Size))
	}
	for _, bf := range p.FileSet.BlobFiles {
		lines = append(lines, fmt.Sprintf("blob %d %d", uint64(bf.FileNum), bf.Size))
	}
	for _, s := range p.Steps {
		lines = append(lines, fmt.Sprintf("step %d %d %d %d %d %s",
			uint64(s.FromVersion), uint64(s.ToVersion),
			bool01(s.RewritesFiles), bool01(s.MarksFiles), bool01(s.Irreversible),
			oneLine(s.Description)))
	}
	for _, f := range p.Files {
		lines = append(lines, fmt.Sprintf("file %s %s %s %s",
			oneLine(f.FileType), oneLine(f.Identifier), oneLine(f.Action), oneLine(f.Detail)))
	}
	for _, pre := range p.Prerequisites {
		lines = append(lines, "prereq "+oneLine(pre))
	}
	lines = append(lines, fmt.Sprintf("irreversible_stage %d", p.IrreversibleStage))
	return lines
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

func bool01(b bool) int {
	if b {
		return 1
	}
	return 0
}

func parseTaggedLine(line, wantKey string) (string, error) {
	key, value, ok := strings.Cut(line, " ")
	if !ok || key != wantKey {
		return "", errors.Newf("pebble: expected %q line, got %q", wantKey, line)
	}
	return value, nil
}

func parseFileRef(v string) (base.DiskFileNum, uint64, error) {
	fnS, sizeS, ok := strings.Cut(v, " ")
	if !ok {
		return 0, 0, errors.Newf("malformed file ref %q", v)
	}
	fn, err := strconv.ParseUint(fnS, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "malformed file ref %q", v)
	}
	size, err := strconv.ParseUint(sizeS, 10, 64)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "malformed file ref %q", v)
	}
	return base.DiskFileNum(fn), size, nil
}

func parseStep(v string) (FormatUpgradeStep, error) {
	fields := strings.SplitN(v, " ", 6)
	if len(fields) != 6 {
		return FormatUpgradeStep{}, errors.Newf("malformed step %q", v)
	}
	from, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return FormatUpgradeStep{}, errors.Wrapf(err, "malformed step %q", v)
	}
	to, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return FormatUpgradeStep{}, errors.Wrapf(err, "malformed step %q", v)
	}
	return FormatUpgradeStep{
		FromVersion:   FormatMajorVersion(from),
		ToVersion:     FormatMajorVersion(to),
		RewritesFiles: fields[2] == "1",
		MarksFiles:    fields[3] == "1",
		Irreversible:  fields[4] == "1",
		Description:   fields[5],
	}, nil
}

func parseAffectedFile(v string) (FormatUpgradeAffectedFile, error) {
	fields := strings.SplitN(v, " ", 4)
	if len(fields) != 4 {
		return FormatUpgradeAffectedFile{}, errors.Newf("malformed affected file %q", v)
	}
	return FormatUpgradeAffectedFile{
		FileType:   fields[0],
		Identifier: fields[1],
		Action:     fields[2],
		Detail:     fields[3],
	}, nil
}

func fingerprintBytes(lines []string) string {
	h := sha256.New()
	for _, l := range lines {
		_, _ = io.WriteString(h, l)
		_, _ = io.WriteString(h, "\n")
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// formatUpgradeStepDetail records static per-version plan metadata.
type formatUpgradeStepDetail struct {
	description string
	rewrites    bool
	marks       bool
}

var formatUpgradeStepDetails = map[FormatMajorVersion]formatUpgradeStepDetail{
	FormatFlushableIngest: {
		description: "enables flushable ingests; requires WAL format support for flushable ingestion records",
	},
	FormatPrePebblev1MarkedCompacted: {
		description: "compacts all SSTables marked for compaction so that no user keys are split across marked files",
		rewrites:    true,
	},
	FormatDeleteSizedAndObsolete: {
		description: "enables sized deletion tombstones and keys marked obsolete",
	},
	FormatVirtualSSTables: {
		description: "enables virtual SSTables persisted through new MANIFEST fields",
	},
	FormatSyntheticPrefixSuffix: {
		description: "enables synthetic prefix/suffix replacement recorded in the MANIFEST",
	},
	FormatFlushableIngestExcises: {
		description: "enables excises written as flushable ingestions",
	},
	FormatColumnarBlocks: {
		description: "enables the TableFormatPebblev5 columnar block format",
	},
	FormatWALSyncChunks: {
		description: "enables WAL sync chunks that disambiguate corruption from logical WAL EOF",
	},
	FormatTableFormatV6: {
		description: "enables the TableFormatPebblev6 sstable format",
	},
	formatDeprecatedExperimentalValueSeparation: {
		description: "enables the deprecated experimental value separation format",
	},
	formatFooterAttributes: {
		description: "enables persisting sstable Attributes in sstable footers",
	},
	FormatValueSeparation: {
		description: "enables value separation using blob files",
	},
	FormatExciseBoundsRecord: {
		description: "enables persisting excise bounds records in the MANIFEST",
	},
	FormatV2BlobFiles: {
		description: "enables the V2 blob file format with compression statistics",
	},
	FormatBackingValueSize: {
		description: "enables persisting the backing sstable value size in the MANIFEST",
	},
	FormatMarkForCompactionInVersionEdit: {
		description: "enables marking tables for compaction through VersionEdits without MANIFEST rotation",
	},
	FormatIngestBlobFiles: {
		description: "enables ingesting blob files through local ingestions",
	},
	FormatRowblkMarkedForCompaction: {
		description: "marks all sstables with row-based data blocks for compaction in the MANIFEST",
		marks:       true,
	},
}

func defaultFormatUpgradeStepDetail(v FormatMajorVersion) formatUpgradeStepDetail {
	if d, ok := formatUpgradeStepDetails[v]; ok {
		return d
	}
	return formatUpgradeStepDetail{
		description: fmt.Sprintf("ratchets the format major version to %d", uint64(v)),
	}
}

// NewFormatUpgradePlan generates an immutable plan for ratcheting the opened
// database from its current FormatMajorVersion to target.
//
// The call does not modify the database. Options.EnableFormatUpgradePlan must
// be set and the database must be opened read-write. Every aspect of the
// returned plan is re-validated by ExecuteFormatUpgradePlan immediately before
// execution begins.
func (d *DB) NewFormatUpgradePlan(target FormatMajorVersion) (*FormatUpgradePlan, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if !d.opts.EnableFormatUpgradePlan {
		return nil, ErrFormatUpgradeDisabled
	}
	if d.opts.ReadOnly {
		return nil, ErrFormatUpgradeReadOnly
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mu.formatVers.ratcheting || d.mu.formatUpgrade.pending != nil || d.mu.formatUpgrade.running != nil {
		return nil, errors.WithHintf(ErrFormatUpgradeInProgress,
			"cannot generate a new plan while a format upgrade is in progress or awaiting recovery")
	}
	current := d.FormatMajorVersion()
	if target > internalFormatNewest {
		return nil, &FormatUpgradeError{
			Reason: FormatUpgradeUnsupportedTargetReason,
			Detail: fmt.Sprintf("target version %d is newer than the newest supported version %d",
				uint64(target), uint64(internalFormatNewest)),
		}
	}
	if target < current {
		return nil, errors.Newf("pebble: target format major version %d is below the current version %d",
			uint64(target), uint64(current))
	}

	steps := make([]FormatUpgradeStep, 0, int(target-current))
	for v := current + 1; v <= target; v++ {
		if formatMajorVersionMigrations[v] == nil {
			return nil, &FormatUpgradeError{
				Reason: FormatUpgradeUnsupportedTargetReason,
				Detail: fmt.Sprintf("this binary does not provide a migration for format major version %d", uint64(v)),
			}
		}
		detail := defaultFormatUpgradeStepDetail(v)
		steps = append(steps, FormatUpgradeStep{
			FromVersion:   v - 1,
			ToVersion:     v,
			Description:   detail.description,
			RewritesFiles: detail.rewrites,
			MarksFiles:    detail.marks,
			Irreversible:  true,
		})
	}

	fileSet := d.snapshotFormatUpgradeFileSetLocked()
	walState := d.snapshotFormatUpgradeWALLocked(current)

	// Identify SSTables that a compaction migration will rewrite.
	var markedTables []FormatUpgradeFileRef
	curVersion := d.mu.versions.currentVersion()
	for f := range curVersion.MarkedForCompaction.Ascending() {
		markedTables = append(markedTables, FormatUpgradeFileRef{
			FileNum: f.TableBacking.DiskFileNum,
			Size:    f.TableBacking.Size,
		})
	}
	sort.Slice(markedTables, func(i, j int) bool { return markedTables[i].FileNum < markedTables[j].FileNum })

	files := d.describeFormatUpgradeAffectedFilesLocked(current, target, steps, fileSet, markedTables)
	prereqs := d.describeFormatUpgradePrerequisitesLocked(current, target, fileSet)

	irreversibleStage := -1
	if len(steps) > 0 {
		irreversibleStage = 0
	}

	var planID [8]byte
	if _, err := cryptorand.Read(planID[:]); err != nil {
		return nil, err
	}
	p := &FormatUpgradePlan{
		PlanID:             hex.EncodeToString(planID[:]),
		CreatedAt:          d.opts.private.timeNow(),
		DBIdentity:         d.dirname,
		FromVersion:        current,
		ToVersion:          target,
		ComparerName:       d.opts.Comparer.Name,
		MergerName:         mergerName(d.opts.Merger),
		KeySchemaName:      d.opts.KeySchema,
		OptionsFingerprint: d.formatUpgradeOptionsFingerprint(),
		WALState:           walState,
		FileSet:            fileSet,
		Steps:              steps,
		Files:              files,
		Prerequisites:      prereqs,
		IrreversibleStage:  irreversibleStage,
	}
	return p, nil
}

func mergerName(m *Merger) string {
	if m == nil {
		return ""
	}
	return m.Name
}

func (d *DB) formatUpgradeOptionsFingerprint() string {
	sum := sha256.Sum256([]byte(d.opts.String()))
	return hex.EncodeToString(sum[:])
}

// snapshotFormatUpgradeFileSetLocked captures the live persistent file
// collection. DB.mu must be held.
func (d *DB) snapshotFormatUpgradeFileSetLocked() FormatUpgradeFileSet {
	vs := d.mu.versions
	current := vs.currentVersion()

	tables := make(map[base.DiskFileNum]uint64)
	for l := range current.Levels {
		for f := range current.Levels[l].All() {
			// Physical backing files may be shared by multiple virtual tables.
			tables[f.TableBacking.DiskFileNum] = f.TableBacking.Size
		}
	}
	blobs := make(map[base.DiskFileNum]uint64)
	for bf := range current.BlobFiles.All() {
		blobs[bf.Physical.FileNum] = bf.Physical.Size
	}
	walNums := make(map[base.DiskFileNum]struct{})
	if vs.minUnflushedLogNum != 0 {
		walNums[vs.minUnflushedLogNum] = struct{}{}
	}
	for _, e := range d.mu.mem.queue {
		if e.logNum != 0 {
			walNums[e.logNum] = struct{}{}
		}
	}

	fs := FormatUpgradeFileSet{
		ManifestFileNum: vs.manifestFileNum,
		Tables:          make([]FormatUpgradeFileRef, 0, len(tables)),
		BlobFiles:       make([]FormatUpgradeFileRef, 0, len(blobs)),
		WALFiles:        make([]base.DiskFileNum, 0, len(walNums)),
	}
	for fn, size := range tables {
		fs.Tables = append(fs.Tables, FormatUpgradeFileRef{FileNum: fn, Size: size})
	}
	sort.Slice(fs.Tables, func(i, j int) bool { return fs.Tables[i].FileNum < fs.Tables[j].FileNum })
	for fn, size := range blobs {
		fs.BlobFiles = append(fs.BlobFiles, FormatUpgradeFileRef{FileNum: fn, Size: size})
	}
	sort.Slice(fs.BlobFiles, func(i, j int) bool { return fs.BlobFiles[i].FileNum < fs.BlobFiles[j].FileNum })
	for fn := range walNums {
		fs.WALFiles = append(fs.WALFiles, fn)
	}
	sort.Slice(fs.WALFiles, func(i, j int) bool { return fs.WALFiles[i] < fs.WALFiles[j] })

	var b strings.Builder
	fmt.Fprintf(&b, "manifest %d\n", uint64(fs.ManifestFileNum))
	for _, fn := range fs.WALFiles {
		fmt.Fprintf(&b, "wal %d\n", uint64(fn))
	}
	for _, t := range fs.Tables {
		fmt.Fprintf(&b, "table %d %d\n", uint64(t.FileNum), t.Size)
	}
	for _, bf := range fs.BlobFiles {
		fmt.Fprintf(&b, "blob %d %d\n", uint64(bf.FileNum), bf.Size)
	}
	sum := sha256.Sum256([]byte(b.String()))
	fs.Fingerprint = hex.EncodeToString(sum[:])
	return fs
}

func (d *DB) snapshotFormatUpgradeWALLocked(current FormatMajorVersion) FormatUpgradeWALState {
	st := FormatUpgradeWALState{
		Disabled:           d.opts.DisableWAL,
		SyncChunks:         current >= FormatWALSyncChunks,
		MinUnflushedLogNum: d.mu.versions.minUnflushedLogNum,
	}
	for _, e := range d.mu.mem.queue {
		if e.logNum != 0 {
			st.LogNums = append(st.LogNums, e.logNum)
		}
	}
	sort.Slice(st.LogNums, func(i, j int) bool { return st.LogNums[i] < st.LogNums[j] })
	return st
}

func (d *DB) describeFormatUpgradeAffectedFilesLocked(
	current, target FormatMajorVersion,
	steps []FormatUpgradeStep,
	fileSet FormatUpgradeFileSet,
	markedTables []FormatUpgradeFileRef,
) []FormatUpgradeAffectedFile {
	var files []FormatUpgradeAffectedFile
	files = append(files, FormatUpgradeAffectedFile{
		FileType:   "recovery-marker",
		Identifier: "marker." + formatUpgradePlanMarkerName,
		Action:     "created",
		Detail:     "records prepared/committed stage state; removed once the plan completes",
	})
	files = append(files, FormatUpgradeAffectedFile{
		FileType:   "format-version-marker",
		Identifier: "marker." + formatVersionMarkerName,
		Action:     "updated",
		Detail: fmt.Sprintf("atomically moved once per committed stage, from version %d to %d",
			uint64(current), uint64(target)),
	})
	files = append(files, FormatUpgradeAffectedFile{
		FileType:   "manifest",
		Identifier: fmt.Sprintf("MANIFEST-%06d", uint64(fileSet.ManifestFileNum)),
		Action:     "updated",
		Detail:     "records VersionEdits from migrations that mark files for compaction; the MANIFEST may be rotated",
	})
	for _, mt := range markedTables {
		files = append(files, FormatUpgradeAffectedFile{
			FileType:   "sstable",
			Identifier: base.MakeFilename(base.FileTypeTable, mt.FileNum),
			Action:     "rewritten",
			Detail:     fmt.Sprintf("marked for compaction; %d bytes rewritten into new sstables and the old file is deleted as obsolete", mt.Size),
		})
	}
	if stepInRange(steps, FormatPrePebblev1MarkedCompacted) && len(markedTables) == 0 {
		files = append(files, FormatUpgradeAffectedFile{
			FileType: "sstable", Identifier: "*", Action: "read",
			Detail: "the migration that compacts marked files is included, but no files are currently marked for compaction",
		})
	}
	if stepInRange(steps, FormatRowblkMarkedForCompaction) {
		files = append(files, FormatUpgradeAffectedFile{
			FileType: "sstable", Identifier: "*", Action: "read",
			Detail: "every live sstable is inspected and row-based-block sstables are marked for compaction in the MANIFEST",
		})
	}
	if current < FormatWALSyncChunks && target >= FormatWALSyncChunks {
		files = append(files, FormatUpgradeAffectedFile{
			FileType: "wal", Identifier: "*", Action: "none",
			Detail: "existing WALs are unchanged; records written after the stage commits use WAL sync chunks",
		})
	}
	files = append(files, FormatUpgradeAffectedFile{
		FileType: "blob", Identifier: "*", Action: "none",
		Detail: "existing blob files are not modified; migrations may obsolete blob files through ordinary compaction",
	})
	files = append(files, FormatUpgradeAffectedFile{
		FileType: "options", Identifier: "OPTIONS-*", Action: "read",
		Detail: "the OPTIONS file is not modified by plan execution; it is rewritten on subsequent Open calls as usual",
	})
	return files
}

func stepInRange(steps []FormatUpgradeStep, v FormatMajorVersion) bool {
	for _, s := range steps {
		if s.ToVersion == v {
			return true
		}
	}
	return false
}

func (d *DB) describeFormatUpgradePrerequisitesLocked(
	current, target FormatMajorVersion, fileSet FormatUpgradeFileSet,
) []string {
	prereqs := []string{
		"the database must be opened read-write (Options.ReadOnly must be false)",
		fmt.Sprintf("the database directory identity must be %q", d.dirname),
		fmt.Sprintf("the current format major version must be %d", uint64(current)),
		fmt.Sprintf("the Comparer must remain %q", d.opts.Comparer.Name),
	}
	if m := mergerName(d.opts.Merger); m != "" {
		prereqs = append(prereqs, fmt.Sprintf("the Merger must remain %q", m))
	} else {
		prereqs = append(prereqs, "no Merger must be configured")
	}
	prereqs = append(prereqs, fmt.Sprintf("the KeySchema must remain %q", d.opts.KeySchema))
	prereqs = append(prereqs,
		fmt.Sprintf("the Options fingerprint must remain %s", d.formatUpgradeOptionsFingerprint()),
		fmt.Sprintf("the live file collection must be unchanged: MANIFEST-%06d, %d sstables, %d blob files, %d WALs",
			uint64(fileSet.ManifestFileNum), len(fileSet.Tables), len(fileSet.BlobFiles), len(fileSet.WALFiles)),
		"no other format major version ratchet may be in progress",
		"no flush, compaction or download may be in progress when execution begins",
		fmt.Sprintf("all migrations up to format major version %d must be supported by this binary", uint64(target)),
		"all local files referenced by the MANIFEST must exist and have MANIFEST-recorded sizes",
	)
	return prereqs
}

// FormatUpgradePlanState describes the in-process state of format upgrade
// plan activity.
type FormatUpgradePlanState struct {
	// Active is true while a plan is executing within this process or while an
	// interrupted plan is blocking the database awaiting continuation.
	Active bool
	// Pending is true when an interrupted plan was recovered during Open.
	Pending bool
	// PlanDigest is the digest of the in-progress plan. Empty if no plan is
	// active.
	PlanDigest string
	// FromVersion is the plan's originating format major version.
	FromVersion FormatMajorVersion
	// ToVersion is the plan's target format major version.
	ToVersion FormatMajorVersion
	// NumStages is the total number of stages in the plan.
	NumStages int
	// NextStage is the index of the first stage whose commit point is not yet
	// durable.
	NextStage int
	// CurrentVersion is the database's current format major version.
	CurrentVersion FormatMajorVersion
}

// FormatUpgradePlanState returns the current format upgrade plan state. It
// does not block and is safe to call concurrently.
func (d *DB) FormatUpgradePlanState() FormatUpgradePlanState {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	st := FormatUpgradePlanState{CurrentVersion: d.FormatMajorVersion()}
	if r := d.mu.formatUpgrade.running; r != nil {
		st.Active = true
		st.PlanDigest = r.plan.Digest()
		st.FromVersion = r.plan.FromVersion
		st.ToVersion = r.plan.ToVersion
		st.NumStages = len(r.plan.Steps)
		st.NextStage = r.nextStage
	}
	if p := d.mu.formatUpgrade.pending; p != nil {
		st.Active = true
		st.Pending = true
		st.PlanDigest = p.digest
		st.FromVersion = p.fromVersion
		st.ToVersion = p.toVersion
		st.NumStages = p.numStages
		st.NextStage = p.nextStage
	}
	return st
}

// ExecuteFormatUpgradePlan validates and executes plan.
//
// Immediately before execution, the database identity, open mode, current
// format major version, Options fingerprint and persistent file collection
// are reconfirmed against the plan. Stages are committed individually and
// the call is safe to retry after any failure: resubmitting the same plan
// after a crash or failed stage resumes from the first stage whose commit
// point is not durable. Submitting the plan after it has fully completed is a
// no-op.
//
// The context is consulted between stages; cancellation leaves the recovery
// marker in place for a later resume.
func (d *DB) ExecuteFormatUpgradePlan(ctx context.Context, plan *FormatUpgradePlan) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if !d.opts.EnableFormatUpgradePlan {
		return ErrFormatUpgradeDisabled
	}
	if plan == nil {
		return formatUpgradeError(FormatUpgradeInvalidPlanReason, "nil format upgrade plan")
	}
	if d.opts.ReadOnly {
		return ErrFormatUpgradeReadOnly
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.mu.formatVers.ratcheting {
		return errors.WithHintf(ErrFormatUpgradeInProgress, "a format major version ratchet is already running")
	}
	current := d.FormatMajorVersion()

	// Fast path: the plan has already been fully executed. Re-submission is
	// idempotent (including plans generated for an older version that has
	// since been reached).
	if current == plan.ToVersion && d.mu.formatUpgrade.pending == nil && d.mu.formatUpgrade.running == nil {
		return nil
	}

	resume := d.mu.formatUpgrade.pending
	if resume != nil {
		if resume.digest != plan.Digest() || resume.fromVersion != plan.FromVersion ||
			resume.toVersion != plan.ToVersion || resume.numStages != len(plan.Steps) {
			return &FormatUpgradeError{
				Reason: FormatUpgradePlanMismatchReason,
				Detail: fmt.Sprintf("in-progress plan %s targets %d→%d (%d stages); submitted plan %s targets %d→%d (%d stages)",
					resume.digest, uint64(resume.fromVersion), uint64(resume.toVersion), resume.numStages,
					plan.Digest(), uint64(plan.FromVersion), uint64(plan.ToVersion), len(plan.Steps)),
				PlanID: plan.PlanID,
			}
		}
	}

	// Block the world: new writes, flushes, ingests, checkpoints and manual
	// compactions are rejected while execution runs.
	d.formatUpgradeActive.Store(true)
	if resume == nil {
		// Wait for background activity started before the block to quiesce.
		// Writes are already rejected, so no new flushable can appear.
		if err := d.waitForFormatUpgradeQuiescenceLocked(ctx); err != nil {
			d.formatUpgradeActive.Store(false)
			return err
		}
		if err := d.validateFormatUpgradePlanLocked(plan, current); err != nil {
			d.formatUpgradeActive.Store(false)
			return err
		}
	} else {
		// Resuming: the recovery marker, not the plan snapshot, is the source
		// of truth. Cross-check the marker-derived state.
		if current < plan.FromVersion || current > plan.ToVersion {
			return d.formatUpgradeFatalLocked(plan, &FormatUpgradeError{
				Reason: FormatUpgradeInconsistentRecoveryMarkerReason,
				Detail: fmt.Sprintf("current format version %d is outside the plan range %d→%d",
					uint64(current), uint64(plan.FromVersion), uint64(plan.ToVersion)),
			})
		}
	}

	// Compute the first stage that still needs to run.
	startStage, err := formatUpgradeNextStage(plan, current)
	if err != nil {
		return d.formatUpgradeFatalLocked(plan, err)
	}

	run := &formatUpgradeExecution{plan: plan, nextStage: startStage}
	d.mu.formatUpgrade.running = run
	d.mu.formatVers.ratcheting = true
	defer func() { d.mu.formatVers.ratcheting = false }()

	// Locate a fresh recovery marker handle when starting; a resumed
	// execution already holds the handle located during Open.
	marker := d.mu.formatUpgrade.marker
	if marker == nil {
		m, _, err := atomicfs.LocateMarker(d.opts.FS, d.dirname, formatUpgradePlanMarkerName)
		if err != nil {
			d.formatUpgradeActive.Store(false)
			d.mu.formatUpgrade.running = nil
			return err
		}
		d.mu.formatUpgrade.marker = m
		marker = m
	}

	execErr := d.runFormatUpgradeStagesLocked(ctx, plan, startStage, marker)
	if execErr != nil {
		// If a prepared/committed recovery marker exists on disk, the
		// database remains blocked pending explicit resume or abort. A
		// failure that occurred before any marker was written (e.g.
		// cancellation before stage 0) leaves no on-disk trace, so unblock.
		var markerValue string
		if m := d.mu.formatUpgrade.marker; m != nil {
			var lerr error
			markerValue, lerr = atomicfs.ReadMarker(d.opts.FS, d.dirname, formatUpgradePlanMarkerName)
			if lerr != nil {
				return errors.CombineErrors(execErr, lerr)
			}
		}
		if markerValue != "" {
			next, recErr := formatUpgradeNextStage(plan, d.FormatMajorVersion())
			if recErr != nil {
				next = startStage
			}
			d.mu.formatUpgrade.pending = &formatUpgradeRecovery{
				digest:      plan.Digest(),
				fromVersion: plan.FromVersion,
				toVersion:   plan.ToVersion,
				numStages:   len(plan.Steps),
				nextStage:   next,
			}
			d.opts.Logger.Infof("pebble: format upgrade plan %s failed at stage %d/%d (current version %d): %v",
				plan.PlanID, next, len(plan.Steps), uint64(d.FormatMajorVersion()), execErr)
		} else {
			if d.mu.formatUpgrade.marker != nil {
				_ = d.mu.formatUpgrade.marker.Close()
				d.mu.formatUpgrade.marker = nil
			}
			d.formatUpgradeActive.Store(false)
		}
		d.mu.formatUpgrade.running = nil
		return execErr
	}

	// All stages committed. Record the terminal done state, then remove the
	// recovery marker files.
	if err := moveFormatUpgradeRecoveryMarker(marker, plan, len(plan.Steps), recoveryStateDone); err != nil {
		d.mu.formatUpgrade.pending = &formatUpgradeRecovery{
			digest: plan.Digest(), fromVersion: plan.FromVersion, toVersion: plan.ToVersion,
			numStages: len(plan.Steps), nextStage: len(plan.Steps),
		}
		return err
	}
	d.removeFormatUpgradeMarkerFilesLocked(marker)
	if err := marker.Close(); err != nil {
		return err
	}
	d.mu.formatUpgrade.marker = nil
	d.mu.formatUpgrade.pending = nil
	d.mu.formatUpgrade.running = nil
	d.formatUpgradeActive.Store(false)

	// Resume normal background activity that the pending/active state
	// suppressed (notably flushing the WAL-replay memtable after a resume).
	d.maybeScheduleFlush()
	d.maybeScheduleCompaction()

	d.opts.Logger.Infof("pebble: format upgrade plan %s completed; format major version is %d",
		plan.PlanID, uint64(plan.ToVersion))
	return nil
}

// AbortFormatUpgradePlan abandons a plan that has not crossed an irreversible
// stage. It is only legal while the database is still at the plan's
// originating format major version. It removes the recovery marker and
// unblocks the database.
func (d *DB) AbortFormatUpgradePlan(ctx context.Context, plan *FormatUpgradePlan) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if !d.opts.EnableFormatUpgradePlan {
		return ErrFormatUpgradeDisabled
	}
	if plan == nil {
		return formatUpgradeError(FormatUpgradeInvalidPlanReason, "nil format upgrade plan")
	}
	if d.opts.ReadOnly {
		return ErrFormatUpgradeReadOnly
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	var activeDigest string
	switch {
	case d.mu.formatUpgrade.running != nil:
		activeDigest = d.mu.formatUpgrade.running.plan.Digest()
	case d.mu.formatUpgrade.pending != nil:
		activeDigest = d.mu.formatUpgrade.pending.digest
	default:
		// Nothing in progress: idempotent no-op for an identical, unstarted
		// plan.
		return nil
	}
	if activeDigest != plan.Digest() {
		return &FormatUpgradeError{
			Reason: FormatUpgradePlanMismatchReason,
			Detail: fmt.Sprintf("in-progress plan %s cannot be aborted with plan %s", activeDigest, plan.Digest()),
			PlanID: plan.PlanID,
		}
	}
	if d.FormatMajorVersion() != plan.FromVersion {
		return &FormatUpgradeError{
			Reason: FormatUpgradeCannotAbortReason,
			Detail: fmt.Sprintf("current format version is %d; the plan from %d has committed irreversible stages",
				uint64(d.FormatMajorVersion()), uint64(plan.FromVersion)),
			PlanID: plan.PlanID,
		}
	}
	if d.mu.formatVers.ratcheting {
		// A stage migration is currently on the stack. Let the caller retry
		// after it returns.
		return errors.WithHintf(ErrFormatUpgradeInProgress, "a stage migration is currently running")
	}
	if err := d.waitForFormatUpgradeQuiescenceLocked(ctx); err != nil {
		return err
	}
	if marker := d.mu.formatUpgrade.marker; marker != nil {
		d.removeFormatUpgradeMarkerFilesLocked(marker)
		if err := marker.Close(); err != nil {
			return err
		}
		d.mu.formatUpgrade.marker = nil
	}
	d.mu.formatUpgrade.pending = nil
	d.mu.formatUpgrade.running = nil
	d.formatUpgradeActive.Store(false)
	d.maybeScheduleFlush()
	d.maybeScheduleCompaction()
	return nil
}

// waitForFormatUpgradeQuiescenceLocked waits until no flush, compaction or
// download is running. DB.mu is dropped while waiting.
func (d *DB) waitForFormatUpgradeQuiescenceLocked(ctx context.Context) error {
	for d.mu.compact.flushing || d.mu.compact.compactingCount > 0 || d.mu.compact.downloadingCount > 0 {
		if err := d.closed.Load(); err != nil {
			return err.(error)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		d.mu.compact.cond.Wait()
	}
	return nil
}

// validateFormatUpgradePlanLocked reconfirms every plan prerequisite against
// the live database. It is only invoked when starting a plan (not resuming).
func (d *DB) validateFormatUpgradePlanLocked(
	p *FormatUpgradePlan, current FormatMajorVersion,
) error {
	fail := func(reason FormatUpgradeErrorReason, detail string) error {
		return &FormatUpgradeError{Reason: reason, Detail: detail, PlanID: p.PlanID}
	}
	if d.dirname != p.DBIdentity {
		return fail(FormatUpgradeIdentityMismatchReason,
			fmt.Sprintf("plan generated for %q, executed against %q", p.DBIdentity, d.dirname))
	}
	if current != p.FromVersion {
		return fail(FormatUpgradePlanStaleReason,
			fmt.Sprintf("plan starts at version %d but the database is at version %d",
				uint64(p.FromVersion), uint64(current)))
	}
	if p.ToVersion > internalFormatNewest || len(p.Steps) != int(p.ToVersion-p.FromVersion) {
		return fail(FormatUpgradeUnsupportedTargetReason,
			fmt.Sprintf("plan target %d with %d steps is not supported by this binary",
				uint64(p.ToVersion), len(p.Steps)))
	}
	for _, s := range p.Steps {
		if formatMajorVersionMigrations[s.ToVersion] == nil {
			return fail(FormatUpgradeInvalidPlanReason,
				fmt.Sprintf("no migration registered for version %d", uint64(s.ToVersion)))
		}
		if s.FromVersion+1 != s.ToVersion {
			return fail(FormatUpgradeInvalidPlanReason,
				fmt.Sprintf("step %d→%d is not contiguous", uint64(s.FromVersion), uint64(s.ToVersion)))
		}
	}
	if d.opts.Comparer.Name != p.ComparerName {
		return fail(FormatUpgradeComparerMismatchReason,
			fmt.Sprintf("plan requires %q, options configure %q", p.ComparerName, d.opts.Comparer.Name))
	}
	if got := mergerName(d.opts.Merger); got != p.MergerName {
		return fail(FormatUpgradeMergerMismatchReason,
			fmt.Sprintf("plan requires %q, options configure %q", p.MergerName, got))
	}
	if d.opts.KeySchema != p.KeySchemaName {
		return fail(FormatUpgradeKeySchemaMismatchReason,
			fmt.Sprintf("plan requires %q, options configure %q", p.KeySchemaName, d.opts.KeySchema))
	}
	if got := d.formatUpgradeOptionsFingerprint(); got != p.OptionsFingerprint {
		return fail(FormatUpgradeOptionsFingerprintMismatchReason,
			fmt.Sprintf("plan fingerprint %s does not match current fingerprint %s",
				p.OptionsFingerprint, got))
	}
	if got := d.snapshotFormatUpgradeFileSetLocked(); got.Fingerprint != p.FileSet.Fingerprint {
		return fail(FormatUpgradeFileSetMismatchReason,
			fmt.Sprintf("plan file set fingerprint %s does not match current fingerprint %s",
				p.FileSet.Fingerprint, got.Fingerprint))
	}
	if err := d.checkFormatUpgradeFilesCompleteLocked(); err != nil {
		return fail(FormatUpgradeIncompleteFilesReason, err.Error())
	}
	return nil
}

// checkFormatUpgradeFilesCompleteLocked verifies that every local live
// SSTable backing and blob file exists and has the size recorded in the
// MANIFEST. Remote objects are skipped. DB.mu must be held.
func (d *DB) checkFormatUpgradeFilesCompleteLocked() error {
	current := d.mu.versions.currentVersion()
	seenTables := make(map[base.DiskFileNum]uint64)
	for l := range current.Levels {
		for f := range current.Levels[l].All() {
			seenTables[f.TableBacking.DiskFileNum] = f.TableBacking.Size
		}
	}
	for fn, wantSize := range seenTables {
		if err := checkLocalObjectSize(d.objProvider, base.FileTypeTable, fn, wantSize); err != nil {
			return err
		}
	}
	for bf := range current.BlobFiles.All() {
		if err := checkLocalObjectSize(d.objProvider, base.FileTypeBlob, bf.Physical.FileNum, bf.Physical.Size); err != nil {
			return err
		}
	}
	return nil
}

func checkLocalObjectSize(
	provider objstorage.Provider, fileType base.FileType, fn base.DiskFileNum, wantSize uint64,
) error {
	meta, err := provider.Lookup(fileType, fn)
	if err != nil {
		return errors.Wrapf(err, "%s %s: object is missing from the object store", fileType, fn)
	}
	if meta.IsRemote() {
		return nil
	}
	size, err := provider.Size(meta)
	if err != nil {
		return errors.Wrapf(err, "%s %s: object cannot be statted", fileType, fn)
	}
	if size == 0 || uint64(size) != wantSize {
		return errors.Newf("%s %s: incomplete object: size %d does not match MANIFEST size %d",
			fileType, fn, size, wantSize)
	}
	return nil
}

// runFormatUpgradeStagesLocked executes stages [start, len(steps)) with
// durable prepared/committed markers around each migration.
func (d *DB) runFormatUpgradeStagesLocked(
	ctx context.Context, plan *FormatUpgradePlan, start int, marker *atomicfs.Marker,
) error {
	for i := start; i < len(plan.Steps); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		step := plan.Steps[i]
		if d.FormatMajorVersion() != step.FromVersion && d.FormatMajorVersion() != step.ToVersion {
			return &FormatUpgradeError{
				Reason: FormatUpgradeInconsistentRecoveryMarkerReason,
				Detail: fmt.Sprintf("stage %d expects version %d or %d, database is at %d",
					i, uint64(step.FromVersion), uint64(step.ToVersion), uint64(d.FormatMajorVersion())),
				PlanID: plan.PlanID,
			}
		}
		migration := formatMajorVersionMigrations[step.ToVersion]
		if migration == nil {
			return &FormatUpgradeError{
				Reason: FormatUpgradeInvalidPlanReason,
				Detail: fmt.Sprintf("no migration registered for version %d", uint64(step.ToVersion)),
				PlanID: plan.PlanID,
			}
		}

		// Durably record the intent. If we crash after this move and before
		// the migration commits, recovery will re-run the (idempotent)
		// migration.
		if d.FormatMajorVersion() == step.FromVersion {
			if err := moveFormatUpgradeRecoveryMarker(marker, plan, i, recoveryStatePrepared); err != nil {
				return errors.Wrapf(err, "stage %d: recording prepared state", i)
			}
		}

		// Test instrumentation. Hooks are unexported and used by the package
		// tests only.
		if d.formatUpgradeTestBeforeStage != nil && d.FormatMajorVersion() == step.FromVersion {
			if err := d.formatUpgradeTestBeforeStage(step.ToVersion); err != nil {
				return errors.Wrapf(err, "stage %d: injected failure before migration", i)
			}
		}

		if d.FormatMajorVersion() == step.FromVersion {
			if err := migration(d); err != nil {
				return errors.Wrapf(&FormatUpgradeError{
					Reason: FormatUpgradeInProgressReason,
					Detail: fmt.Sprintf("migration to version %d failed; the stage can be retried",
						uint64(step.ToVersion)),
					PlanID: plan.PlanID,
				}, "stage %d", i)
			}
			if d.FormatMajorVersion() != step.ToVersion {
				d.opts.Logger.Fatalf("pebble: migration to format version %d did not finalize the upgrade",
					uint64(step.ToVersion))
			}
		}

		// The migration may have rewritten or marked files. Verify the
		// resulting live file collection before declaring the stage
		// committed.
		if err := d.checkFormatUpgradeFilesCompleteLocked(); err != nil {
			return errors.Wrapf(&FormatUpgradeError{
				Reason: FormatUpgradeIncompleteFilesReason,
				Detail: err.Error(),
				PlanID: plan.PlanID,
			}, "stage %d", i)
		}

		if d.formatUpgradeTestAfterStage != nil {
			if err := d.formatUpgradeTestAfterStage(step.ToVersion); err != nil {
				return errors.Wrapf(err, "stage %d: injected failure after commit point", i)
			}
		}

		// Durably record the commit. If we crash after the format-version
		// marker moved but before this move, recovery observes the higher
		// format version and treats the stage as committed.
		if err := moveFormatUpgradeRecoveryMarker(marker, plan, i, recoveryStateCommitted); err != nil {
			return errors.Wrapf(err, "stage %d: recording committed state", i)
		}
		if d.mu.formatUpgrade.running != nil {
			d.mu.formatUpgrade.running.nextStage = i + 1
		}
	}
	return nil
}

// formatUpgradeFatalLocked is used for inconsistencies where continuing is
// unsafe: it leaves the database blocked and the recovery marker in place.
func (d *DB) formatUpgradeFatalLocked(plan *FormatUpgradePlan, err error) error {
	d.formatUpgradeActive.Store(true)
	d.mu.formatUpgrade.pending = &formatUpgradeRecovery{
		digest:      plan.Digest(),
		fromVersion: plan.FromVersion,
		toVersion:   plan.ToVersion,
		numStages:   len(plan.Steps),
	}
	return err
}

// formatUpgradeNextStage returns the index of the first stage whose commit
// point is not durable at currentVersion.
func formatUpgradeNextStage(
	plan *FormatUpgradePlan, currentVersion FormatMajorVersion,
) (int, error) {
	if currentVersion < plan.FromVersion {
		return 0, &FormatUpgradeError{
			Reason: FormatUpgradeInconsistentRecoveryMarkerReason,
			Detail: fmt.Sprintf("current version %d is below plan origin %d",
				uint64(currentVersion), uint64(plan.FromVersion)),
			PlanID: plan.PlanID,
		}
	}
	if currentVersion > plan.ToVersion {
		return 0, &FormatUpgradeError{
			Reason: FormatUpgradeInconsistentRecoveryMarkerReason,
			Detail: fmt.Sprintf("current version %d exceeds plan target %d",
				uint64(currentVersion), uint64(plan.ToVersion)),
			PlanID: plan.PlanID,
		}
	}
	return int(currentVersion - plan.FromVersion), nil
}

// formatUpgradeRecovery is the in-memory record of an interrupted plan,
// recovered from the on-disk recovery marker during Open.
type formatUpgradeRecovery struct {
	digest      string
	fromVersion FormatMajorVersion
	toVersion   FormatMajorVersion
	numStages   int
	// nextStage is the index of the first stage whose commit point is not
	// known durable.
	nextStage int
}

// formatUpgradeExecution is the in-memory record of a running plan.
type formatUpgradeExecution struct {
	plan      *FormatUpgradePlan
	nextStage int
}

// recoveryMarkerValue is the parsed content of a recovery marker value.
type recoveryMarkerValue struct {
	digest    string
	from      FormatMajorVersion
	to        FormatMajorVersion
	numStages int
	stage     int
	state     string
}

// encode produces the marker value:
// v1.<digest>.<from>.<to>.<nstages>.<stage>.<state>.<crc>
func encodeFormatUpgradeRecoveryMarker(r recoveryMarkerValue) string {
	prefix := fmt.Sprintf("v1.%s.%d.%d.%d.%d.%s",
		r.digest, uint64(r.from), uint64(r.to), r.numStages, r.stage, r.state)
	crc := crc32.ChecksumIEEE([]byte(prefix))
	return fmt.Sprintf("%s.%08x", prefix, crc)
}

func moveFormatUpgradeRecoveryMarker(
	marker *atomicfs.Marker, plan *FormatUpgradePlan, stage int, state string,
) error {
	v := recoveryMarkerValue{
		digest:    plan.Digest(),
		from:      plan.FromVersion,
		to:        plan.ToVersion,
		numStages: len(plan.Steps),
		stage:     stage,
		state:     state,
	}
	return marker.Move(encodeFormatUpgradeRecoveryMarker(v))
}

func parseFormatUpgradeRecoveryMarker(value string) (recoveryMarkerValue, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 8 || parts[0] != "v1" {
		return recoveryMarkerValue{}, errors.Newf("malformed recovery marker %q", value)
	}
	crcWant := parts[7]
	prefix := strings.Join(parts[:7], ".")
	crc := crc32.ChecksumIEEE([]byte(prefix))
	if fmt.Sprintf("%08x", crc) != crcWant {
		return recoveryMarkerValue{}, errors.Newf("recovery marker checksum mismatch: %q", value)
	}
	from, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return recoveryMarkerValue{}, errors.Newf("malformed recovery marker %q: %v", value, err)
	}
	to, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return recoveryMarkerValue{}, errors.Newf("malformed recovery marker %q: %v", value, err)
	}
	nstages, err := strconv.Atoi(parts[4])
	if err != nil {
		return recoveryMarkerValue{}, errors.Newf("malformed recovery marker %q: %v", value, err)
	}
	stage, err := strconv.Atoi(parts[5])
	if err != nil {
		return recoveryMarkerValue{}, errors.Newf("malformed recovery marker %q: %v", value, err)
	}
	state := parts[6]
	if state != recoveryStatePrepared && state != recoveryStateCommitted && state != recoveryStateDone {
		return recoveryMarkerValue{}, errors.Newf("recovery marker has unknown state %q", state)
	}
	if len(parts[1]) != 32 {
		return recoveryMarkerValue{}, errors.Newf("recovery marker digest has bad length: %q", value)
	}
	if stage < 0 || stage > nstages {
		return recoveryMarkerValue{}, errors.Newf("recovery marker stage %d out of range (0..%d)", stage, nstages)
	}
	return recoveryMarkerValue{
		digest:    parts[1],
		from:      FormatMajorVersion(from),
		to:        FormatMajorVersion(to),
		numStages: nstages,
		stage:     stage,
		state:     state,
	}, nil
}

// recoverFormatUpgradePlanOnOpenLocked inspects the recovery marker located
// during recovery and configures d.mu.formatUpgrade appropriately. It is
// called after the version set is initialized (so the live file collection is
// available) but before WAL replay. DB.mu is held.
//
// On entry d.mu.formatUpgrade.marker holds the marker handle (which is
// non-nil even if no marker value exists). When the handle is consumed (no
// marker value, or terminal marker cleaned up), the handle is closed and
// d.mu.formatUpgrade.marker is set to nil; otherwise the DB retains ownership
// and closes it on Close (or the open-failure cleanup closes it).
func (d *DB) recoverFormatUpgradePlanOnOpenLocked(value string) error {
	marker := d.mu.formatUpgrade.marker
	consumeMarker := func() error {
		if marker == nil {
			return nil
		}
		err := marker.Close()
		d.mu.formatUpgrade.marker = nil
		return err
	}
	if value == "" {
		return consumeMarker()
	}
	rec, err := parseFormatUpgradeRecoveryMarker(value)
	if err != nil {
		return formatUpgradeErrorFromErr(FormatUpgradeCorruptRecoveryMarkerReason, err)
	}
	current := d.FormatMajorVersion()

	if rec.state == recoveryStateDone {
		// A previous process finished the plan but crashed before removing
		// the marker files.
		if current != rec.to {
			return formatUpgradeError(FormatUpgradeInconsistentRecoveryMarkerReason,
				"recovery marker records completion at version %d, but the format-version marker is at %d",
				uint64(rec.to), uint64(current))
		}
		// Best-effort cleanup in a read-write open. Read-only opens leave the
		// files in place; they carry no pending semantics and the handle is
		// retained for Close.
		if !d.opts.ReadOnly {
			d.removeFormatUpgradeMarkerFilesLocked(marker)
			return consumeMarker()
		}
		return nil
	}

	if current < rec.from || current > rec.to {
		return formatUpgradeError(FormatUpgradeInconsistentRecoveryMarkerReason,
			"recovery marker covers %d→%d but the database is at version %d",
			uint64(rec.from), uint64(rec.to), uint64(current))
	}

	// Determine the highest stage consistent with the persisted format
	// version.
	nextStage := int(current - rec.from)
	switch {
	case nextStage == rec.numStages:
		// Format version fully ratcheted; only the terminal marker
		// bookkeeping was lost.
	case rec.state == recoveryStateCommitted && rec.stage+1 > nextStage:
		// A marker claims a stage committed that the format version does not
		// reflect.
		return formatUpgradeError(FormatUpgradeInconsistentRecoveryMarkerReason,
			"recovery marker records stage %d committed but current version is %d (expected at least %d)",
			rec.stage, uint64(current), uint64(rec.from)+uint64(rec.stage+1))
	case rec.state == recoveryStatePrepared && rec.stage < nextStage:
		// The stage's commit point is durable even though the committed
		// marker move was lost.
	case rec.state == recoveryStatePrepared && rec.stage >= nextStage:
		// Stage not yet committed; nextStage from the format version is
		// correct and the (idempotent) migration will be re-run.
	default:
		// Committed marker behind current version, or prepared stage already
		// superseded: normal progression.
	}

	if err := d.checkFormatUpgradeFilesCompleteLocked(); err != nil {
		return wrapFormatUpgradeError(FormatUpgradeIncompleteFilesReason, err,
			"recovery marker indicates an interrupted upgrade")
	}

	if d.opts.ReadOnly || !d.opts.EnableFormatUpgradePlan {
		return &FormatUpgradeError{
			Reason: FormatUpgradeRecoveryRequiredReason,
			Detail: fmt.Sprintf("interrupted plan %s from version %d to %d; current version %d; next stage %d/%d",
				rec.digest, uint64(rec.from), uint64(rec.to), uint64(current), nextStage, rec.numStages),
		}
	}

	d.mu.formatUpgrade.marker = marker
	d.mu.formatUpgrade.pending = &formatUpgradeRecovery{
		digest:      rec.digest,
		fromVersion: rec.from,
		toVersion:   rec.to,
		numStages:   rec.numStages,
		nextStage:   nextStage,
	}
	d.formatUpgradeActive.Store(true)
	d.opts.Logger.Infof("pebble: recovered interrupted format upgrade plan %s (%d→%d), next stage %d/%d",
		rec.digest, uint64(rec.from), uint64(rec.to), nextStage, rec.numStages)
	return nil
}

// removeFormatUpgradeMarkerFilesLocked removes every on-disk file for the
// plan recovery marker. Failures are best-effort: obsolete files are harmless
// and are cleaned up on a subsequent Open or LocateMarker call.
func (d *DB) removeFormatUpgradeMarkerFilesLocked(marker *atomicfs.Marker) {
	fs := d.opts.FS
	ls, err := fs.List(d.dirname)
	if err != nil {
		d.opts.Logger.Infof("pebble: could not list directory while cleaning upgrade marker: %v", err)
		return
	}
	prefix := "marker." + formatUpgradePlanMarkerName + "."
	for _, name := range ls {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if err := fs.Remove(fs.PathJoin(d.dirname, name)); err != nil {
			d.opts.Logger.Infof("pebble: could not remove obsolete upgrade marker %s: %v", name, err)
		}
	}
	if err := marker.SyncDir(); err != nil {
		d.opts.Logger.Infof("pebble: could not sync directory while cleaning upgrade marker: %v", err)
	}
}

// formatUpgradePendingLocked reports whether an interrupted plan is awaiting
// continuation in this open. DB.mu must be held.
func (d *DB) formatUpgradePendingLocked() bool {
	return d.mu.formatUpgrade.pending != nil
}

// formatUpgradeBlocked returns a structured error for an operation blocked by
// an active or pending upgrade.
func (d *DB) formatUpgradeBlocked(op string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.formatUpgradeBlockedLocked(op)
}

// formatUpgradeBlockedLocked is like formatUpgradeBlocked with d.mu held.
func (d *DB) formatUpgradeBlockedLocked(op string) error {
	if p := d.mu.formatUpgrade.pending; p != nil {
		return errors.Wrapf(ErrFormatUpgradeInProgress,
			"%s blocked: format upgrade plan %s is interrupted at stage %d/%d and must be continued or aborted",
			op, p.digest, p.nextStage, p.numStages)
	}
	return errors.Wrapf(ErrFormatUpgradeInProgress, "%s blocked: a format upgrade plan is executing", op)
}
