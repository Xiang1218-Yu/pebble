// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/batchrepr"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/writequota"
)

// quotaRuleAmounts accumulates the four demand dimensions for one compiled
// rule during a single scan.
type quotaRuleAmounts [writequota.NumDimensions]int64

// IngestFileQuota is the quota-relevant summary of one externally produced
// object participating in an ingestion (an SSTable, or an SST plus its blob
// objects).
type IngestFileQuota struct {
	// Size is the logical size (bytes) attributed to the object for quota
	// purposes (for SSTs with blobs this includes attached blob bytes).
	Size int64
	// Bounds is the user-key interval covered by the object's point and range
	// keys.
	Bounds KeyRange
	// EndInclusive records whether Bounds.End is inclusive (true for some
	// synthesized external file bounds).
	EndInclusive bool
}

// scanBatch performs a single linear pass over the batch representation and
// produces the atomic demand vector across the configured rules:
//
//   - point operations (Set/Delete/SingleDelete/DeleteSized/Merge/
//     SetWithDelete) are attributed to the single highest-priority rule
//     containing the key;
//   - range deletions and range keys are attributed in full (record bytes,
//     one operation, memtable estimate) to every overlapping rule;
//   - LogData records contribute record bytes only (no operation, no
//     memtable estimate);
//   - IngestSST/Excise records are skipped: ingestions are metered separately
//     at the ingest admission point using object metadata and never reach
//     this scanner;
//   - ConcurrentBatches is set to 1 for every touched rule.
//
// Pending bytes are measured as the record's exact encoded repr length;
// unflushed bytes use the same memTableEntrySize accounting as
// refreshMemTableSize. Operations that do not match any rule are unmetered.
func (qc *quotaConfig) scanBatch(b *Batch) ([]writequota.RuleDemand, error) {
	if qc == nil || len(b.data) < batchrepr.HeaderLen {
		return nil, nil
	}
	acc := make(map[*compiledQuotaRule]*quotaRuleAmounts)
	add := func(r *compiledQuotaRule, bytes, ops, unflushed int64) {
		if r == nil {
			return
		}
		a := acc[r]
		if a == nil {
			a = &quotaRuleAmounts{}
			acc[r] = a
		}
		a[writequota.PendingBytes] += bytes
		a[writequota.PendingOps] += ops
		a[writequota.UnflushedBytes] += unflushed
	}
	addRangeRules := func(rules []*compiledQuotaRule, bytes, ops, unflushed int64) {
		for _, r := range rules {
			add(r, bytes, ops, unflushed)
		}
	}

	for r := b.Reader(); ; {
		before := len(r)
		kind, key, value, ok, err := r.Next()
		if !ok || err != nil {
			if err != nil {
				return nil, err
			}
			break
		}
		if kind > InternalKeyKindMax {
			return nil, ErrInvalidBatch
		}
		recBytes := int64(before - len(r))
		switch batchKinds[kind].category {
		case batchKindPoint:
			entry := int64(memTableEntrySize(len(key), len(value)))
			add(qc.pointRule(key), recBytes, 1, entry)
		case batchKindRangeDel:
			entry := int64(memTableEntrySize(len(key), len(value)))
			span := base.UserKeyBoundsEndExclusive(key, value)
			rules := qc.rangeRules(nil, span)
			addRangeRules(rules, recBytes, 1, entry)
		case batchKindRangeKey:
			entry := int64(memTableEntrySize(len(key), len(value)))
			end, _, err := rangekey.DecodeEndKey(kind, value)
			if err != nil {
				return nil, err
			}
			span := base.UserKeyBoundsEndExclusive(key, end)
			rules := qc.rangeRules(nil, span)
			addRangeRules(rules, recBytes, 1, entry)
		case batchKindLogData:
			add(qc.pointRule(key), recBytes, 0, 0)
		case batchKindIngestOrExcise:
			// Metered via the ingest admission path; never here.
			continue
		default: // batchKindInvalid
			return nil, ErrInvalidBatch
		}
	}
	return qc.flushAccum(acc), nil
}

// buildIngestDemands produces the demand vector for an SSTable ingestion.
// Each file's size and one operation are attributed to every rule overlapping
// the file's bounds; ConcurrentBatches is 1 for every touched rule. Unflushed
// bytes are zero here (direct-to-LSM ingest occupies no memtable); the
// flushable-ingest path adds the unflushed dimension separately.
func (qc *quotaConfig) buildIngestDemands(files []IngestFileQuota, exciseBounds []KeyRange) []writequota.RuleDemand {
	if qc == nil {
		return nil
	}
	acc := make(map[*compiledQuotaRule]*quotaRuleAmounts)
	add := func(r *compiledQuotaRule, bytes, ops int64) {
		if r == nil {
			return
		}
		a := acc[r]
		if a == nil {
			a = &quotaRuleAmounts{}
			acc[r] = a
		}
		a[writequota.PendingBytes] += bytes
		a[writequota.PendingOps] += ops
	}
	buf := make([]*compiledQuotaRule, 0, 2)
	for i := range files {
		f := &files[i]
		span := base.UserKeyBoundsEndExclusiveIf(f.Bounds.Start, f.Bounds.End, !f.EndInclusive)
		buf = qc.rangeRules(buf[:0], span)
		for _, r := range buf {
			add(r, f.Size, 1)
		}
	}
	for i := range exciseBounds {
		b := &exciseBounds[i]
		span := base.UserKeyBoundsEndExclusive(b.Start, b.End)
		for _, r := range qc.rangeRules(buf[:0], span) {
			add(r, 0, 1)
		}
	}
	return qc.flushAccum(acc)
}

// buildIngestUnflushed adds the UnflushedBytes dimension to an existing ingest
// demand vector (used for flushable ingestions that remain in the flushable
// queue until flushed). It returns a new vector; file sizes are attributed to
// every overlapping rule.
func (qc *quotaConfig) buildIngestUnflushed(files []IngestFileQuota) []writequota.RuleDemand {
	if qc == nil {
		return nil
	}
	acc := make(map[*compiledQuotaRule]*quotaRuleAmounts)
	buf := make([]*compiledQuotaRule, 0, 2)
	for i := range files {
		f := &files[i]
		span := base.UserKeyBoundsEndExclusiveIf(f.Bounds.Start, f.Bounds.End, !f.EndInclusive)
		for _, r := range qc.rangeRules(buf[:0], span) {
			a := acc[r]
			if a == nil {
				a = &quotaRuleAmounts{}
				acc[r] = a
			}
			a[writequota.UnflushedBytes] += f.Size
		}
	}
	return qc.flushAccum(acc)
}

// mergeUnflushedDemands folds the UnflushedBytes component of uf into the
// commit-scoped demand vector demands (which both index the same active rule
// set), returning the combined vector.
func mergeUnflushedDemands(demands, uf []writequota.RuleDemand) []writequota.RuleDemand {
	if len(uf) == 0 {
		return demands
	}
	byRule := make(map[*writequota.Rule]int, len(demands))
	for i := range demands {
		byRule[demands[i].Rule] = i
	}
	for i := range uf {
		v := uf[i].Amount[writequota.UnflushedBytes]
		if v == 0 {
			continue
		}
		if idx, ok := byRule[uf[i].Rule]; ok {
			demands[idx].Amount[writequota.UnflushedBytes] += v
		} else {
			demands = append(demands, writequota.RuleDemand{
				Rule:   uf[i].Rule,
				Amount: [writequota.NumDimensions]int64{writequota.UnflushedBytes: v},
			})
		}
	}
	return demands
}

// loadResultQuotaFiles builds the quota file summary for an ingest load
// result. Local SSTables additionally contribute the physical size of their
// attached blob files.
func loadResultQuotaFiles(lr *ingestLoadResult) []IngestFileQuota {
	count := lr.sstCount()
	if count == 0 {
		return nil
	}
	files := make([]IngestFileQuota, 0, count)
	add := func(meta *manifest.TableMetadata, blobPhysical int64, endInclusive bool) {
		ukb := meta.UserKeyBounds()
		files = append(files, IngestFileQuota{
			Size:         int64(meta.Size) + blobPhysical,
			EndInclusive: endInclusive,
			Bounds:       KeyRange{Start: ukb.Start, End: ukb.End.Key},
		})
	}
	for i := range lr.local {
		var blobPhysical int64
		for j := range lr.local[i].blobFiles {
			blobPhysical += int64(lr.local[i].blobFiles[j].Physical.Size)
		}
		add(lr.local[i].TableMetadata, blobPhysical,
			lr.local[i].TableMetadata.UserKeyBounds().End.Kind == base.Inclusive)
	}
	for i := range lr.shared {
		ukb := lr.shared[i].TableMetadata.UserKeyBounds()
		add(lr.shared[i].TableMetadata, 0, ukb.End.Kind == base.Inclusive)
	}
	for i := range lr.external {
		ukb := lr.external[i].TableMetadata.UserKeyBounds()
		add(lr.external[i].TableMetadata, 0, ukb.End.Kind == base.Inclusive)
	}
	return files
}

// flushAccum converts the per-rule accumulator into an admission demand
// vector, setting ConcurrentBatches=1 for every touched rule. The output order
// follows the configuration's rule order for determinism.
func (qc *quotaConfig) flushAccum(acc map[*compiledQuotaRule]*quotaRuleAmounts) []writequota.RuleDemand {
	if len(acc) == 0 {
		return nil
	}
	demands := make([]writequota.RuleDemand, 0, len(acc))
	for i := range qc.rules {
		r := &qc.rules[i]
		a := acc[r]
		if a == nil {
			continue
		}
		a[writequota.ConcurrentBatches] = 1
		demands = append(demands, writequota.RuleDemand{Rule: r.rule, Amount: *a})
	}
	return demands
}

// unflushedBreakdown returns the per-rule UnflushedBytes component of an
// admitted demand vector, used to register the amounts against the concrete
// flushables that carry the write until they flush.
func unflushedBreakdown(demands []writequota.RuleDemand) map[*writequota.Rule]int64 {
	out := make(map[*writequota.Rule]int64)
	for i := range demands {
		if v := demands[i].Amount[writequota.UnflushedBytes]; v > 0 {
			out[demands[i].Rule] += v
		}
	}
	return out
}
