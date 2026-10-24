// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"

	"github.com/cockroachdb/pebble/internal/writequota"
)

// flushableQuota tracks write-quota grants whose UnflushedBytes reservation is
// released when the containing flushable is flushed out of the flushable
// queue. It is embedded (anonymously, for method promotion) in the concrete
// flushable types (memTable, flushableBatch, ingestedFlushable). A zero value
// is ready to use and the field is inert (no allocations) until a grant is
// added.
type flushableQuota struct {
	mu     sync.Mutex
	grants []*writequota.Grant
}

// addGrant attaches a grant whose UnflushedBytes dimension must remain
// reserved until this flushable flushes. It is safe for concurrent commit
// apply goroutines to add grants to the same memTable.
func (q *flushableQuota) addGrant(g *writequota.Grant) {
	if q == nil || g == nil {
		return
	}
	q.mu.Lock()
	q.grants = append(q.grants, g)
	q.mu.Unlock()
}

// releaseWriteQuotaUnflushed releases the unflushed reservation of every
// attached grant. It is called exactly once, after the flushable has been
// successfully flushed out of the queue. Per-grant ReleaseUnflushed is
// idempotent.
func (q *flushableQuota) releaseWriteQuotaUnflushed() {
	if q == nil {
		return
	}
	q.mu.Lock()
	grants := q.grants
	q.grants = nil
	q.mu.Unlock()
	for _, g := range grants {
		g.ReleaseUnflushed()
	}
}

// Compile-time guarantees that the concrete flushables expose the release
// hook used after a successful flush.
var (
	_ interface{ releaseWriteQuotaUnflushed() } = (*memTable)(nil)
	_ interface{ releaseWriteQuotaUnflushed() } = (*flushableBatch)(nil)
	_ interface{ releaseWriteQuotaUnflushed() } = (*ingestedFlushable)(nil)
)

// releaseFlushablesWriteQuota releases unflushed quota reservations for every
// flushed entry that carries any. Flushables without quota state are
// unaffected.
func releaseFlushablesWriteQuota(flushed flushableList) {
	for i := range flushed {
		if qf, ok := flushed[i].flushable.(interface{ releaseWriteQuotaUnflushed() }); ok {
			qf.releaseWriteQuotaUnflushed()
		}
	}
}
