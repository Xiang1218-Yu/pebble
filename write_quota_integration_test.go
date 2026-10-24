// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/internal/writequota"
	"github.com/cockroachdb/pebble/vfs"
)

func openQuotaDB(t *testing.T, rules ...WriteQuotaRule) *DB {
	t.Helper()
	opts := &Options{FS: vfs.NewMem(), MemTableSize: 4 << 20}
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
	return d
}

// quotaTestLeak registers the leaktest check as a Cleanup (running after the
// DB-close cleanup) rather than a defer (which would run before Cleanup).
func quotaTestLeak(t *testing.T) {
	t.Helper()
	t.Cleanup(leaktest.AfterTest(t))
}

func quotaMetric(d *DB, name string) writequota.RuleMetrics {
	for _, rm := range d.writeQuota.metrics().Rules {
		if rm.Name == name {
			return rm
		}
	}
	return writequota.RuleMetrics{}
}

func waitQuotaWaiting(d *DB, name string, n int64) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if quotaMetric(d, name).Waiting == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWriteQuotaRejectIntegration(t *testing.T) {
	quotaTestLeak(t)
	d := openQuotaDB(t, func() WriteQuotaRule {
		r := quotaRule("small", "a", "m", 0, WriteQuotaReject)
		// One pending byte: no real record ever fits.
		r.PendingBytes = 1
		return r
	}())

	// Writes into the rule's range are rejected before any resource is used.
	err := d.Set([]byte("a"), []byte("v"), nil)
	if !errors.Is(err, ErrWriteQuotaExceeded) {
		t.Fatalf("expected ErrWriteQuotaExceeded, got %v", err)
	}
	var ee *WriteQuotaExceededError
	if !errors.As(err, &ee) || ee.RuleName != "small" || ee.Dimension != writequota.PendingBytes {
		t.Fatalf("unexpected exceeded error: %v", err)
	}
	// All write APIs reject consistently.
	if err := d.Merge([]byte("b"), []byte("v"), nil); !errors.Is(err, ErrWriteQuotaExceeded) {
		t.Fatalf("Merge: %v", err)
	}
	b := d.NewBatch()
	defer b.Close()
	if err := b.DeleteRange([]byte("a"), []byte("c"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(nil); !errors.Is(err, ErrWriteQuotaExceeded) {
		t.Fatalf("batch commit: %v", err)
	}
	// After rejection the batch can be reset and reused (it never committed).
	b.Reset()
	if err := b.Set([]byte("z"), []byte("outside"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(nil); err != nil {
		t.Fatalf("out-of-range write must succeed: %v", err)
	}

	// Writes outside the rule's range are unaffected and remain visible;
	// rejected writes left no sequence-number trace (the values around them
	// are contiguous and readable).
	if err := d.Set([]byte("z2"), []byte("v2"), nil); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"z", "z2"} {
		if _, closer, err := d.Get([]byte(k)); err != nil || closer == nil {
			t.Fatalf("key %s not visible: %v", k, err)
		} else {
			closer.Close()
		}
	}
	if _, closer, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected key must be absent, err=%v", err)
	} else if closer != nil {
		closer.Close()
	}
	// No hard usage should be outstanding from rejected writes.
	if mt := quotaMetric(d, "small"); mt.Used[writequota.PendingBytes] != 0 || mt.Waiting != 0 {
		t.Fatalf("rejected writes leaked quota: %+v", mt)
	}
}

func TestWriteQuotaWaitAndFlushIntegration(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	d := openQuotaDB(t, func() WriteQuotaRule {
		r := quotaRule("u", "a", "m", 0, WriteQuotaWait)
		r.UnflushedBytes = entry
		return r
	}())

	if err := d.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	// The second write of the same estimated size cannot be admitted until the
	// first is flushed.
	done := make(chan error, 1)
	go func() { done <- d.Set(key, val, nil) }()
	waitQuotaWaiting(d, "u", 1)

	// Flush releases the unflushed reservation; the blocked write proceeds.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked write should complete after flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked write did not complete after flush")
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if mt := quotaMetric(d, "u"); mt.Used[writequota.UnflushedBytes] != 0 || mt.Waiting != 0 {
		t.Fatalf("quota not drained after flush: %+v", mt)
	}
}

func TestWriteQuotaPriorityOrderIntegration(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	d := openQuotaDB(t, func() WriteQuotaRule {
		r := quotaRule("p", "a", "m", 0, WriteQuotaPriority)
		r.UnflushedBytes = entry
		return r
	}())
	if err := d.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}

	lowDone := make(chan struct{}, 1)
	highDone := make(chan struct{}, 1)
	go func() {
		_ = d.ApplyWithContext(context.Background(),
			mustBatch(d, key, val), &WriteOptions{Priority: 0})
		lowDone <- struct{}{}
	}()
	go func() {
		_ = d.ApplyWithContext(context.Background(),
			mustBatch(d, key, val), &WriteOptions{Priority: 10})
		highDone <- struct{}{}
	}()
	waitQuotaWaiting(d, "p", 2)

	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-highDone:
	case <-time.After(5 * time.Second):
		t.Fatal("high priority writer should be granted first")
	}
	select {
	case <-lowDone:
		t.Fatal("low priority writer must not be granted before the next flush")
	case <-time.After(30 * time.Millisecond):
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lowDone:
	case <-time.After(5 * time.Second):
		t.Fatal("low priority writer should complete after second flush")
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
}

func mustBatch(d *DB, key, val []byte) *Batch {
	b := d.NewBatch()
	if err := b.Set(key, val, nil); err != nil {
		panic(err)
	}
	return b
}

func TestWriteQuotaContextCancel(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	d := openQuotaDB(t, func() WriteQuotaRule {
		r := quotaRule("c", "a", "m", 0, WriteQuotaWait)
		r.UnflushedBytes = entry
		return r
	}())
	if err := d.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		b := d.NewBatch()
		defer b.Close()
		_ = b.Set(key, val, nil)
		errCh <- b.CommitWithContext(ctx, nil)
	}()
	waitQuotaWaiting(d, "c", 1)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not unblock writer")
	}
	if mt := quotaMetric(d, "c"); mt.Waiting != 0 || mt.Reserved[writequota.UnflushedBytes] != 0 {
		t.Fatalf("canceled writer leaked quota: %+v", mt)
	}
	// Flush frees capacity; subsequent writes work normally.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(key, val, nil); err != nil {
		t.Fatalf("write after cancel+flush failed: %v", err)
	}
}

func TestWriteQuotaCloseWhileWaiting(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	opts := &Options{FS: vfs.NewMem(), MemTableSize: 4 << 20}
	opts.EnsureDefaults()
	opts.WriteQuotaConfig = MakeStaticWriteQuotaConfig(opts.Comparer.Compare, func() WriteQuotaRule {
		r := quotaRule("x", "a", "m", 0, WriteQuotaWait)
		r.UnflushedBytes = entry
		return r
	}())
	d, err := Open("", opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- d.Set(key, val, nil) }()
	waitQuotaWaiting(d, "x", 1)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not unblock quota waiter")
	}
}

func TestWriteQuotaNoSyncWaitReleasesAtSyncWait(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	d := openQuotaDB(t, func() WriteQuotaRule {
		r := quotaRule("s", "a", "m", 0, WriteQuotaWait)
		r.UnflushedBytes = entry
		r.PendingBytes = 1 << 20
		r.ConcurrentBatches = 1
		return r
	}())

	b := d.NewBatch()
	defer b.Close()
	if err := b.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	if err := d.ApplyNoSyncWait(b, Sync); err != nil {
		t.Fatal(err)
	}
	// Before SyncWait the concurrent-batch reservation is still held, so a
	// second write cannot be admitted.
	blocked := make(chan struct{}, 1)
	go func() {
		_ = d.Set(key, val, nil)
		blocked <- struct{}{}
	}()
	waitQuotaWaiting(d, "s", 1)
	if err := b.SyncWait(); err != nil {
		t.Fatal(err)
	}
	// Unflushed is still charged (flush pending); but concurrent reservation
	// is released. The blocked writer now waits on unflushed (flushed away).
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not complete after SyncWait+flush release")
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteQuotaWaitDurationStat(t *testing.T) {
	quotaTestLeak(t)
	key, val := []byte("a"), []byte("v1")
	entry := int64(memTableEntrySize(len(key), len(val)))
	d := openQuotaDB(t, func() WriteQuotaRule {
		r := quotaRule("w", "a", "m", 0, WriteQuotaWait)
		r.UnflushedBytes = entry
		return r
	}())
	if err := d.Set(key, val, nil); err != nil {
		t.Fatal(err)
	}
	statCh := make(chan BatchCommitStats, 1)
	go func() {
		b := d.NewBatch()
		defer b.Close()
		_ = b.Set(key, val, nil)
		_ = b.Commit(nil)
		statCh <- b.CommitStats()
	}()
	waitQuotaWaiting(d, "w", 1)
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case st := <-statCh:
		if st.QuotaWaitDuration <= 0 {
			t.Fatalf("expected positive QuotaWaitDuration, got %v", st.QuotaWaitDuration)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not complete")
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
}
