// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/internal/writequota"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func writeIngestSST(t *testing.T, fs vfs.FS, path string, kvs ...string) uint64 {
	t.Helper()
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		TableFormat: sstable.TableFormatPebblev2,
	})
	for i := 0; i < len(kvs); i += 2 {
		if err := w.Set([]byte(kvs[i]), []byte(kvs[i+1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := fs.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(info.Size())
}

func quotaIngestDB(t *testing.T, rules ...WriteQuotaRule) (*DB, vfs.FS) {
	t.Helper()
	fs := vfs.NewMem()
	opts := &Options{FS: fs, MemTableSize: 4 << 20}
	opts.EnsureDefaults()
	opts.WriteQuotaConfig = MakeStaticWriteQuotaConfig(opts.Comparer.Compare, rules...)
	d, err := Open("", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if d.closed.Load() == nil {
			_ = d.Close()
		}
	})
	return d, fs
}

func TestWriteQuotaIngestReject(t *testing.T) {
	quotaTestLeak(t)
	r := quotaRule("ing", "a", "m", 0, WriteQuotaReject)
	r.PendingBytes = 1 // no real sstable fits
	d, fs := quotaIngestDB(t, r)

	path := "reject.sst"
	size := writeIngestSST(t, fs, path, "a", "v")
	if size <= 1 {
		t.Fatalf("test sst too small: %d", size)
	}
	err := d.Ingest(context.Background(), []string{path})
	if !errors.Is(err, ErrWriteQuotaExceeded) {
		t.Fatalf("expected ErrWriteQuotaExceeded, got %v", err)
	}
	// Admission happens before linking or sequence-number allocation; the
	// source file remains and a subsequent out-of-range ingestion succeeds.
	path2 := "ok.sst"
	writeIngestSST(t, fs, path2, "z", "v")
	if err := d.Ingest(context.Background(), []string{path2}); err != nil {
		t.Fatalf("out-of-range ingest failed: %v", err)
	}
	if _, closer, err := d.Get([]byte("z")); err != nil {
		t.Fatalf("ingested value not visible: %v", err)
	} else {
		closer.Close()
	}
	if _, closer, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected ingest left data, err=%v", err)
	} else if closer != nil {
		closer.Close()
	}
	// No quota usage remains.
	if mt := quotaMetric(d, "ing"); mt.Used[writequota.PendingBytes] != 0 ||
		mt.Used[writequota.UnflushedBytes] != 0 || mt.Waiting != 0 {
		t.Fatalf("rejected ingest leaked quota: %+v", mt)
	}
}

func TestWriteQuotaIngestDirectDrains(t *testing.T) {
	quotaTestLeak(t)
	r := quotaRule("ing", "a", "m", 0, WriteQuotaWait)
	r.PendingBytes = 1 << 30
	r.PendingOps = 10
	r.UnflushedBytes = 1 << 30
	r.ConcurrentBatches = 10
	d, fs := quotaIngestDB(t, r)

	path := "direct.sst"
	writeIngestSST(t, fs, path, "a", "v", "b", "w")
	if err := d.Ingest(context.Background(), []string{path}); err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
	// Direct-to-LSM ingest releases every dimension on return.
	mt := quotaMetric(d, "ing")
	for dim := writequota.Dimension(0); dim < writequota.NumDimensions; dim++ {
		if mt.Used[dim] != 0 {
			t.Fatalf("direct ingest left used[%s]=%d", dim, mt.Used[dim])
		}
	}
	if mt.Admits != 1 {
		t.Fatalf("admits=%d, want 1", mt.Admits)
	}
	if _, closer, err := d.Get([]byte("b")); err != nil {
		t.Fatalf("ingested value not visible: %v", err)
	} else {
		closer.Close()
	}
}

func TestWriteQuotaIngestFlushableHoldsUnflushed(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	r := quotaRule("ing", "a", "m", 0, WriteQuotaWait)
	// Capacity accommodates one memtable write plus the ingested object.
	r.UnflushedBytes = entry + 4096
	d, fs := quotaIngestDB(t, r)

	// Put an overlapping key in the memtable so the ingest is forced onto the
	// flushable queue.
	if err := d.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	path := "flushable.sst"
	writeIngestSST(t, fs, path, "a", "ing")
	if err := d.Ingest(context.Background(), []string{path}); err != nil {
		t.Fatalf("flushable ingest failed: %v", err)
	}
	mt := quotaMetric(d, "ing")
	if mt.Used[writequota.UnflushedBytes] == 0 {
		t.Fatalf("flushable ingest should hold unflushed reservation: %+v", mt)
	}
	// Flushing drains the ingested flushable and the memtable.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := quotaMetric(d, "ing").Used[writequota.UnflushedBytes]; got != 0 {
		t.Fatalf("unflushed after flush = %d, want 0", got)
	}
	if _, closer, err := d.Get([]byte("a")); err != nil {
		t.Fatalf("ingested value not visible after flush: %v", err)
	} else {
		closer.Close()
	}
}

func TestWriteQuotaIngestCancel(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	r := quotaRule("ing", "a", "m", 0, WriteQuotaWait)
	// The in-memtable write fills the unflushed dimension; any ingestion is
	// blocked at admission until a flush.
	r.UnflushedBytes = entry
	d, fs := quotaIngestDB(t, r)
	if err := d.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	path := "cancel.sst"
	writeIngestSST(t, fs, path, "a", "ing")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.Ingest(ctx, []string{path})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// Nothing leaked; after flushing, the same ingestion succeeds.
	if mt := quotaMetric(d, "ing"); mt.Waiting != 0 || mt.Reserved[writequota.UnflushedBytes] != 0 {
		t.Fatalf("canceled ingest leaked: %+v", mt)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Logf("post-flush metrics: %+v", quotaMetric(d, "ing"))
	if err := d.Ingest(context.Background(), []string{path}); err != nil {
		t.Fatalf("ingest after cancel+flush failed: %v", err)
	}
}
