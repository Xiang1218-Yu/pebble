// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/batchrepr"
	"github.com/cockroachdb/pebble/internal/writequota"
)

func recLen(key, value []byte) int64 {
	n := int64(1 + uvarintLen(len(key)) + len(key))
	if value != nil {
		n += int64(uvarintLen(len(value)) + len(value))
	}
	return n
}

func uvarintLen(n int) int {
	l := 1
	for n >= 128 {
		n >>= 7
		l++
	}
	return l
}

func demandByName(t *testing.T, ds []writequota.RuleDemand, name string) writequota.RuleDemand {
	t.Helper()
	for i := range ds {
		if ds[i].Rule.Name() == name {
			return ds[i]
		}
	}
	t.Fatalf("no demand for rule %q (have %d demands)", name, len(ds))
	return writequota.RuleDemand{}
}

func scanTestConfig(t *testing.T) *writeQuotaManager {
	return newTestWriteQuotaManager(t,
		quotaRule("a", "a", "m", 0, WriteQuotaReject),
		quotaRule("b", "m", "z", 0, WriteQuotaWait),
	)
}

func TestScanBatchPoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	qc := scanTestConfig(t).current()
	b := &Batch{}
	defer b.Close()
	if err := b.Set([]byte("a"), []byte("v1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge([]byte("n"), []byte("v2"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("m0"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("zzz"), []byte("unmatched"), nil); err != nil {
		t.Fatal(err)
	}
	ds, err := qc.scanBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 {
		t.Fatalf("expected 2 touched rules, got %d", len(ds))
	}
	da := demandByName(t, ds, "a")
	if da.Amount[writequota.PendingOps] != 1 {
		t.Fatalf("a ops = %d, want 1", da.Amount[writequota.PendingOps])
	}
	wantBytesA := recLen([]byte("a"), []byte("v1"))
	if da.Amount[writequota.PendingBytes] != wantBytesA {
		t.Fatalf("a bytes = %d, want %d", da.Amount[writequota.PendingBytes], wantBytesA)
	}
	wantEntryA := int64(memTableEntrySize(len("a"), len("v1")))
	if da.Amount[writequota.UnflushedBytes] != wantEntryA {
		t.Fatalf("a unflushed = %d, want %d", da.Amount[writequota.UnflushedBytes], wantEntryA)
	}
	if da.Amount[writequota.ConcurrentBatches] != 1 {
		t.Fatalf("a concurrent = %d, want 1", da.Amount[writequota.ConcurrentBatches])
	}
	db := demandByName(t, ds, "b")
	if db.Amount[writequota.PendingOps] != 2 {
		t.Fatalf("b ops = %d, want 2", db.Amount[writequota.PendingOps])
	}
	wantBytesB := recLen([]byte("n"), []byte("v2")) + recLen([]byte("m0"), nil)
	if db.Amount[writequota.PendingBytes] != wantBytesB {
		t.Fatalf("b bytes = %d, want %d", db.Amount[writequota.PendingBytes], wantBytesB)
	}
}

func TestScanBatchRangeOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	qc := scanTestConfig(t).current()

	b1 := &Batch{}
	defer b1.Close()
	if err := b1.DeleteRange([]byte("a"), []byte("z"), nil); err != nil {
		t.Fatal(err)
	}
	ds, err := qc.scanBatch(b1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 {
		t.Fatalf("rangedel should touch both rules, got %d", len(ds))
	}
	wantBytes := recLen([]byte("a"), []byte("z"))
	wantEntry := int64(memTableEntrySize(len("a"), len("z")))
	for _, name := range []string{"a", "b"} {
		d := demandByName(t, ds, name)
		if d.Amount[writequota.PendingBytes] != wantBytes ||
			d.Amount[writequota.PendingOps] != 1 ||
			d.Amount[writequota.UnflushedBytes] != wantEntry ||
			d.Amount[writequota.ConcurrentBatches] != 1 {
			t.Fatalf("rangedel rule %s amounts = %v, want full record on every dimension", name, d.Amount)
		}
	}

	b2 := &Batch{}
	defer b2.Close()
	if err := b2.RangeKeySet([]byte("a"), []byte("z"), []byte("suf"), []byte("rkval"), nil); err != nil {
		t.Fatal(err)
	}
	ds2, err := qc.scanBatch(b2)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds2) != 2 {
		t.Fatalf("rangekey should touch both rules, got %d", len(ds2))
	}
	// Range key record key=start, value encodes end+suffix/value.
	wantRKBytes := int64(len(b2.Repr()) - batchrepr.HeaderLen)
	for _, name := range []string{"a", "b"} {
		d := demandByName(t, ds2, name)
		if d.Amount[writequota.PendingBytes] != wantRKBytes || d.Amount[writequota.PendingOps] != 1 {
			t.Fatalf("rangekey rule %s bytes=%d ops=%d, want %d/1",
				name, d.Amount[writequota.PendingBytes], d.Amount[writequota.PendingOps], wantRKBytes)
		}
	}
}

func TestScanBatchLogDataAndUnmatched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	qc := scanTestConfig(t).current()

	b := &Batch{}
	defer b.Close()
	if err := b.LogData([]byte("abc"), nil); err != nil {
		t.Fatal(err)
	}
	ds, err := qc.scanBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 {
		t.Fatalf("logdata should match rule a, got %d", len(ds))
	}
	d := demandByName(t, ds, "a")
	if d.Amount[writequota.PendingBytes] != recLen([]byte("abc"), nil) {
		t.Fatalf("logdata bytes = %d", d.Amount[writequota.PendingBytes])
	}
	if d.Amount[writequota.PendingOps] != 0 || d.Amount[writequota.UnflushedBytes] != 0 {
		t.Fatalf("logdata must not count ops/unflushed: %v", d.Amount)
	}

	b2 := &Batch{}
	defer b2.Close()
	if err := b2.Set([]byte("0"), []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b2.Set([]byte("zzz"), []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	ds2, err := qc.scanBatch(b2)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds2) != 0 {
		t.Fatalf("unmatched keys must produce no demand, got %d", len(ds2))
	}
}

func TestScanBatchNestedApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	qc := scanTestConfig(t).current()

	parent := &Batch{}
	defer parent.Close()
	c1 := &Batch{}
	defer c1.Close()
	c2 := &Batch{}
	defer c2.Close()
	if err := c1.Set([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := c2.Set([]byte("n"), []byte("2"), nil); err != nil {
		t.Fatal(err)
	}
	if err := parent.Apply(c1, nil); err != nil {
		t.Fatal(err)
	}
	if err := parent.Apply(c2, nil); err != nil {
		t.Fatal(err)
	}
	ds, err := qc.scanBatch(parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 {
		t.Fatalf("flattened nested batch should touch 2 rules, got %d", len(ds))
	}
	d := demandByName(t, ds, "a")
	if d.Amount[writequota.PendingOps] != 1 {
		t.Fatalf("a ops = %d, want 1", d.Amount[writequota.PendingOps])
	}
	d = demandByName(t, ds, "b")
	if d.Amount[writequota.PendingOps] != 1 {
		t.Fatalf("b ops = %d, want 1", d.Amount[writequota.PendingOps])
	}
	// The child batches remain independently scannable.
	ds1, err := qc.scanBatch(c1)
	if err != nil || len(ds1) != 1 {
		t.Fatalf("child c1 scan: ds=%v err=%v", ds1, err)
	}
}

func TestBuildIngestDemands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	qc := scanTestConfig(t).current()

	files := []IngestFileQuota{
		{Size: 100, Bounds: KeyRange{Start: []byte("a"), End: []byte("b")}},
		{Size: 200, Bounds: KeyRange{Start: []byte("l"), End: []byte("n")}},   // spans boundary m
		{Size: 50, Bounds: KeyRange{Start: []byte("zz"), End: []byte("zzz")}}, // unmatched
	}
	ds := qc.buildIngestDemands(files, nil)
	if len(ds) != 2 {
		t.Fatalf("expected 2 touched rules, got %d", len(ds))
	}
	da := demandByName(t, ds, "a")
	if da.Amount[writequota.PendingBytes] != 300 || da.Amount[writequota.PendingOps] != 2 {
		t.Fatalf("a ingest amounts = %v, want 300 bytes/2 ops (file1+file2)", da.Amount)
	}
	db := demandByName(t, ds, "b")
	if db.Amount[writequota.PendingBytes] != 200 || db.Amount[writequota.PendingOps] != 1 {
		t.Fatalf("b ingest amounts = %v, want 200 bytes/1 op", db.Amount)
	}
	if da.Amount[writequota.UnflushedBytes] != 0 {
		t.Fatalf("direct ingest unflushed must be 0, got %d", da.Amount[writequota.UnflushedBytes])
	}
	if da.Amount[writequota.ConcurrentBatches] != 1 {
		t.Fatalf("ingest concurrent = %d, want 1", da.Amount[writequota.ConcurrentBatches])
	}

	// Unflushed component (flushable ingest) attributes file sizes.
	uf := qc.buildIngestUnflushed(files)
	if demandByName(t, uf, "a").Amount[writequota.UnflushedBytes] != 300 {
		t.Fatalf("a unflushed = %v", uf)
	}

	// Excise counts one operation on its span.
	dsExcise := qc.buildIngestDemands(nil, []KeyRange{{Start: []byte("m"), End: []byte("n")}})
	de := demandByName(t, dsExcise, "b")
	if de.Amount[writequota.PendingOps] != 1 || de.Amount[writequota.PendingBytes] != 0 {
		t.Fatalf("excise amounts = %v, want 1 op/0 bytes", de.Amount)
	}
}
