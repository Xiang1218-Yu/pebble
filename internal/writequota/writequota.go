// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package writequota implements the admission-control core for Pebble's
// experimental per-user-key-range write quota feature.
//
// The package is intentionally independent of the pebble root package. Callers
// compile their configuration into RuleSpecs, perform key-range matching
// themselves, and submit atomic demand vectors (at most one entry per touched
// rule) to Manager.Admit.
//
// Concurrency model #
//
// A single mutex protects all rule accounting and the wait queue; no I/O is
// performed while the mutex is held.
//
// Two kinds of capacity commitments are tracked per rule and dimension:
//
//   - hard usage (ruleState.used): charged only for requests that have been
//     admitted and have not yet released the dimension;
//   - soft reservations (ruleState.reserved): charged for requests that are
//     waiting in the queue. A queued request's demand can never be consumed by
//     a later request, which makes the construction starvation-free.
//
// Waiters never hold hard usage, so a cross-range request never holds capacity
// of one range while blocked on another: there is no hold-and-wait cycle and
// therefore no cross-range deadlock. A demand vector is admitted atomically
// for every touched rule and dimension, or not at all.
//
// At any instant the per-rule, per-dimension invariant is:
//
//	used + reserved <= limit   (for finite limits only).
//
// A queued waiter demanding amount a on dimension d is grantable exactly when
//
//	used[d] + reserved[d] <= limit[d]:
//
// its own soft reservation cancels out, and granting converts its
// reservation into hard usage without changing the sum. Waiters that do not
// demand a blocked dimension may still be granted (using surplus capacity of
// other ranges), while earlier waiters remain protected by their
// reservations.
package writequota // import "github.com/cockroachdb/pebble/internal/writequota"

import (
	"container/heap"
	"context"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

// Dimension identifies one of the four independently limited resources.
type Dimension uint8

const (
	// PendingBytes limits the logical record bytes of submissions that have
	// been admitted but have not yet completed committing (WAL publish and,
	// for synchronous writes, sync; SyncWait for ApplyNoSyncWait).
	PendingBytes Dimension = iota
	// PendingOps limits the number of mutating operations (Batch.Count
	// accounting) of in-flight submissions.
	PendingOps
	// UnflushedBytes limits the estimated memory admitted writes may occupy
	// in memtables and other flushables before those flushables are flushed
	// out of the flushable queue.
	UnflushedBytes
	// ConcurrentBatches limits the number of concurrently in-flight
	// submissions (one per Batch commit or SSTable ingestion call).
	ConcurrentBatches

	numDimensions Dimension = 4
)

// NumDimensions is the number of independently limited dimensions.
const NumDimensions = numDimensions

// String returns a short stable name for the dimension.
func (d Dimension) String() string {
	switch d {
	case PendingBytes:
		return "pending-bytes"
	case PendingOps:
		return "pending-ops"
	case UnflushedBytes:
		return "unflushed-bytes"
	case ConcurrentBatches:
		return "concurrent-batches"
	default:
		return "unknown"
	}
}

// Policy decides what happens when a demand cannot be satisfied immediately.
type Policy uint8

const (
	// PolicyReject (the zero value) rejects the request immediately when any
	// touched rule cannot satisfy it.
	PolicyReject Policy = iota
	// PolicyWait queues the request FIFO until the demand can be satisfied.
	PolicyWait
	// PolicyPriority queues the request ahead of lower-priority requests
	// (FIFO within the same priority).
	PolicyPriority
	// PolicyRejectRange behaves like PolicyReject for requests touching a
	// single rule; requests touching two or more rules are rejected whenever
	// the demand cannot be satisfied immediately for every touched rule
	// (cross-range requests never partially wait).
	PolicyRejectRange
)

// String returns a short stable name for the policy.
func (p Policy) String() string {
	switch p {
	case PolicyReject:
		return "reject"
	case PolicyWait:
		return "wait"
	case PolicyPriority:
		return "priority"
	case PolicyRejectRange:
		return "reject-range"
	default:
		return "unknown"
	}
}

// RuleSpec is the immutable specification of one rule within a configuration
// snapshot. The caller guarantees the spec has been validated.
type RuleSpec struct {
	// Name is the stable, unique-within-configuration name used in metrics and
	// error attribution.
	Name string
	// Policy is applied when the rule's finite limits cannot immediately
	// satisfy a request.
	Policy Policy
	// Limits holds the finite limits for the four dimensions; a zero limit
	// means the dimension is not limited.
	Limits [numDimensions]int64
}

// Rule is a handle to a rule state object. Rule pointers remain valid after a
// reconfiguration: outstanding requests and grants keep referencing the
// snapshot they were admitted against, even after the rule is removed from
// the active configuration (a "retired" rule).
type Rule struct {
	spec       RuleSpec
	generation uint64
	active     bool

	used     [numDimensions]int64
	reserved [numDimensions]int64
	// waiting is the number of queued waiters currently referencing the rule.
	waiting int64

	// Cumulative metrics.
	admits   int64
	rejected [numDimensions]int64
	waited   int64
	waitDur  time.Duration
}

// Name returns the rule's configured name.
func (r *Rule) Name() string { return r.spec.Name }

// Policy returns the rule's configured policy.
func (r *Rule) Policy() Policy { return r.spec.Policy }

// Generation returns the configuration generation in which the rule was
// introduced.
func (r *Rule) Generation() uint64 { return r.generation }

// Retired reports whether the rule has been removed from the active
// configuration by a later reconfiguration.
func (r *Rule) Retired() bool { return !r.active }

// Snapshot is an immutable view of the active rules, index-aligned with the
// RuleSpec slice passed to Configure.
type Snapshot struct {
	// Generation is incremented on every successful Configure.
	Generation uint64
	// Rules are the active rule handles, in spec order.
	Rules []*Rule
}

// RuleDemand is one entry of an atomic demand vector: an amount requested from
// a single rule. The slice passed to the Manager must contain at most one
// entry per rule.
type RuleDemand struct {
	// Rule is the rule handle resolved from the current Snapshot.
	Rule *Rule
	// Amount is the requested amount for each dimension; zero entries are
	// ignored.
	Amount [numDimensions]int64
}

// Request is an admission request.
type Request struct {
	// Demands is the atomic demand vector across every touched rule.
	Demands []RuleDemand
	// Priority orders queued requests when any touched rule uses
	// PolicyPriority; it is ignored for requests queued purely FIFO.
	Priority int32
}

// ErrExceeded is the sentinel wrapped by ExceededError when an admission
// request is rejected because a quota is exhausted.
var ErrExceeded = errors.New("pebble: write quota exceeded")

// ErrClosed is returned by Admit when the Manager has been closed (which the
// pebble root package translates to pebble.ErrClosed).
var ErrClosed = errors.New("writequota: manager closed")

// ExceededError attributes a rejected admission to the first blocking rule
// and dimension. errors.Is(err, ErrExceeded) always holds.
type ExceededError struct {
	// RuleName is the name of the blocking rule.
	RuleName string
	// Dimension is the first dimension that could not be satisfied.
	Dimension Dimension
	// Policy is the effective policy that caused the rejection.
	Policy Policy
}

// Error implements error.
func (e *ExceededError) Error() string {
	return "pebble: write quota exceeded for rule " + e.RuleName +
		" (" + e.Dimension.String() + ", policy " + e.Policy.String() + ")"
}

// Is implements the errors.Is interface.
func (e *ExceededError) Is(target error) bool { return target == ErrExceeded }

// Grant represents a successful, atomic reservation across every touched
// rule. It is released in up to two independent phases, each exactly once:
//
//   - ReleaseCommitted releases the commit-scoped dimensions (PendingBytes,
//     PendingOps, ConcurrentBatches) when the submission completes;
//   - ReleaseUnflushed releases UnflushedBytes once the containing flushable
//     has been flushed (or immediately via Seed-style grants that never
//     flush).
//
// A zero-value Grant pointer (nil) is valid and all of its methods are no-ops.
type Grant struct {
	m            *Manager
	demands      []RuleDemand
	waitDuration time.Duration

	committedOnce sync.Once
	flushedOnce   sync.Once
}

// WaitDuration returns the time the request spent queued before being granted
// (zero when it was admitted immediately).
func (g *Grant) WaitDuration() time.Duration {
	if g == nil {
		return 0
	}
	return g.waitDuration
}

// Demands returns the demand vector the grant was created with. The returned
// slice must not be modified.
func (g *Grant) Demands() []RuleDemand {
	if g == nil {
		return nil
	}
	return g.demands
}

// ReleaseCommitted releases the commit-scoped dimensions. It is safe to call
// more than once; subsequent calls are no-ops.
func (g *Grant) ReleaseCommitted() {
	if g == nil {
		return
	}
	g.committedOnce.Do(func() {
		g.m.release(g.demands, PendingBytes, PendingOps, ConcurrentBatches)
	})
}

// ReleaseUnflushed releases the UnflushedBytes dimension. It is safe to call
// more than once; subsequent calls are no-ops.
func (g *Grant) ReleaseUnflushed() {
	if g == nil {
		return
	}
	g.flushedOnce.Do(func() {
		g.m.release(g.demands, UnflushedBytes)
	})
}

// waiter is a queued admission request.
type waiter struct {
	demands []RuleDemand
	// priorityMode is true when any touched rule uses PolicyPriority; the
	// request then participates in priority ordering, otherwise it is strictly
	// FIFO regardless of Request.Priority.
	priorityMode bool
	priority     int32
	seq          uint64
	enqueuedAt   time.Time

	ch      chan struct{}
	granted bool
	closed  bool
}

// waiterHeap orders waiters by (priority desc, seq asc); FIFO waiters use the
// minimum order priority so strict-FIFO requests are never reordered by
// caller-supplied priority values.
type waiterHeap []*waiter

func (h waiterHeap) Len() int { return len(h) }
func (h waiterHeap) Less(i, j int) bool {
	pi, pj := h[i].priority, h[j].priority
	if !h[i].priorityMode {
		pi = math.MinInt32
	}
	if !h[j].priorityMode {
		pj = math.MinInt32
	}
	if pi != pj {
		return pi > pj
	}
	return h[i].seq < h[j].seq
}
func (h waiterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *waiterHeap) Push(x any)   { *h = append(*h, x.(*waiter)) }
func (h *waiterHeap) Pop() any {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return w
}

// Metrics is a point-in-time snapshot of Manager accounting.
type Metrics struct {
	// Generation is the active configuration generation.
	Generation uint64
	// Rules contains active rules followed by not-yet-drained retired rules.
	Rules []RuleMetrics
}

// RuleMetrics is the per-rule accounting snapshot.
type RuleMetrics struct {
	Name       string
	Generation uint64
	Retired    bool
	// Used is the current hard usage per dimension.
	Used [numDimensions]int64
	// Reserved is the current soft reservation of queued waiters per
	// dimension.
	Reserved [numDimensions]int64
	// Waiting is the number of queued waiters referencing the rule.
	Waiting int64
	// Admits is the cumulative number of admitted requests touching the rule.
	Admits int64
	// Rejected is the cumulative number of rejections attributed to each
	// dimension of the rule.
	Rejected [numDimensions]int64
	// Waited is the cumulative number of requests that had to queue.
	Waited int64
	// WaitDuration is the cumulative time queued requests spent waiting
	// (including wait time of requests later canceled).
	WaitDuration time.Duration
}

// Manager is the write-quota admission manager. The zero value is not usable;
// create one with New.
type Manager struct {
	mu struct {
		sync.Mutex
		generation uint64
		rules      []*Rule
		retired    []*Rule
		queue      waiterHeap
		nextSeq    uint64
		closed     bool
	}
	nowFn func() time.Time
}

// New creates a Manager with the given initial rules. Configure may be called
// later to reconfigure. Passing nil starts the manager with no active rules
// (every request is admitted with empty demand).
func New(nowFn func() time.Time) *Manager {
	if nowFn == nil {
		nowFn = time.Now
	}
	m := &Manager{nowFn: nowFn}
	m.mu.queue = waiterHeap{}
	return m
}

// Configure atomically replaces the active rule set and returns the new
// Snapshot. Outstanding waiters and grants remain bound to the rules they were
// admitted against; rules removed from the new spec are retired and retained
// until their usage and reservations drain to zero.
func (m *Manager) Configure(specs []RuleSpec) Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.generation++
	rules := make([]*Rule, len(specs))
	for i := range specs {
		rules[i] = &Rule{spec: specs[i], generation: m.mu.generation, active: true}
	}
	old := m.mu.rules
	m.mu.rules = rules
	for _, r := range old {
		r.active = false
		if r.outstanding() {
			m.mu.retired = append(m.mu.retired, r)
		}
	}
	m.pruneRetiredLocked()
	return Snapshot{Generation: m.mu.generation, Rules: rules}
}

// Current returns the active snapshot.
func (m *Manager) Current() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Snapshot{Generation: m.mu.generation, Rules: append([]*Rule(nil), m.mu.rules...)}
}

// Closed reports whether Close has been called.
func (m *Manager) Closed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.closed
}

// Admit attempts to reserve the request's demand atomically across every
// touched rule. On success the returned Grant must be released in both phases
// exactly once. If the demand cannot be satisfied immediately, behavior
// follows the effective policy of the touched rules:
//
//   - a blocking rule with PolicyReject, or PolicyRejectRange for a request
//     touching at least two rules, results in *ExceededError wrapping
//     ErrExceeded;
//   - otherwise the request queues (FIFO or priority order) and blocks until
//     granted, ctx is canceled (returning ctx.Err()), or the Manager is closed
//     (returning ErrClosed).
//
// A request with an empty demand vector is admitted without accounting.
func (m *Manager) Admit(ctx context.Context, req Request) (*Grant, error) {
	start := m.nowFn()
	demands := append([]RuleDemand(nil), req.Demands...)

	m.mu.Lock()
	if m.mu.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if len(demands) == 0 {
		m.mu.Unlock()
		return &Grant{m: m, demands: nil}, nil
	}

	if br, bd, ok := m.blocking(demands); ok {
		m.applyUsageLocked(demands, +1)
		for _, d := range demands {
			d.Rule.admits++
		}
		m.mu.Unlock()
		return &Grant{m: m, demands: demands}, nil
	} else {
		// The demand does not fit. Decide whether to reject or queue.
		touched := len(demands)
		rangeReject := false
		priorityMode := false
		// A cross-range request touching a PolicyRejectRange rule never
		// partially waits: reject whenever any touched rule is currently
		// unsatisfiable. Independently, any unsatisfied dimension governed by
		// a PolicyReject (or single-rule PolicyRejectRange) rule rejects.
		rejectByBlockingRule := false
		for _, d := range demands {
			p := d.Rule.spec.Policy
			switch p {
			case PolicyRejectRange:
				if touched >= 2 {
					rangeReject = true
				}
			case PolicyPriority:
				priorityMode = true
			}
			if p == PolicyReject || (p == PolicyRejectRange && touched < 2) {
				for dim := Dimension(0); dim < numDimensions; dim++ {
					if d.Amount[dim] > 0 && !m.fitsLocked(d.Rule, dim, d.Amount[dim]) {
						rejectByBlockingRule = true
						break
					}
				}
			}
		}
		// Reject when the blocking rule itself rejects (PolicyReject, or
		// PolicyRejectRange which is equivalent for a single touched rule), or
		// when a cross-range request touches a PolicyRejectRange rule.
		rejectNow := rejectByBlockingRule || rangeReject
		if rejectNow {
			for _, d := range demands {
				for dim := Dimension(0); dim < numDimensions; dim++ {
					if d.Amount[dim] > 0 && !m.fitsLocked(d.Rule, dim, d.Amount[dim]) {
						d.Rule.rejected[dim]++
					}
				}
			}
			policy := br.spec.Policy
			if rangeReject {
				policy = PolicyRejectRange
			}
			m.mu.Unlock()
			return nil, &ExceededError{RuleName: br.spec.Name, Dimension: bd, Policy: policy}
		}

		// Queue: take a soft reservation and wait.
		m.mu.nextSeq++
		w := &waiter{
			demands:      demands,
			priorityMode: priorityMode,
			priority:     req.Priority,
			seq:          m.mu.nextSeq,
			enqueuedAt:   start,
			ch:           make(chan struct{}),
		}
		for _, d := range demands {
			for dim := Dimension(0); dim < numDimensions; dim++ {
				d.Rule.reserved[dim] += d.Amount[dim]
			}
			d.Rule.waiting++
			d.Rule.waited++
		}
		heap.Push(&m.mu.queue, w)
		m.mu.Unlock()

		select {
		case <-w.ch:
			if w.closed {
				return nil, ErrClosed
			}
			return &Grant{m: m, demands: demands, waitDuration: m.nowFn().Sub(start)}, nil
		case <-ctx.Done():
			m.mu.Lock()
			if w.granted {
				// The request was granted concurrently; the cancellation lost
				// the race. Keep the grant.
				m.mu.Unlock()
				return &Grant{m: m, demands: demands, waitDuration: m.nowFn().Sub(start)}, nil
			}
			m.removeWaiterLocked(w)
			m.releaseReservedLocked(demands)
			waitTime := m.nowFn().Sub(start)
			for _, d := range demands {
				d.Rule.waitDur += waitTime
			}
			m.grantPendingLocked()
			m.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

// Seed attributes pre-existing usage without an admission decision or wait.
// It is used after database open to reconcile the UnflushedBytes dimension
// against the recovered flushable queue. The returned Grant releases the
// seeded amounts.
func (m *Manager) Seed(demands []RuleDemand) *Grant {
	demands = append([]RuleDemand(nil), demands...)
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(demands) > 0 {
		m.applyUsageLocked(demands, +1)
		for _, d := range demands {
			d.Rule.admits++
		}
	}
	return &Grant{m: m, demands: demands}
}

// Close wakes every queued waiter with ErrClosed. Usage held by outstanding
// grants remains releasable after Close.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.closed {
		return
	}
	m.mu.closed = true
	for m.mu.queue.Len() > 0 {
		w := heap.Pop(&m.mu.queue).(*waiter)
		w.closed = true
		close(w.ch)
	}
}

// Metrics returns a consistent snapshot of rule accounting, including
// not-yet-drained retired rules.
func (m *Manager) Metrics() Metrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := Metrics{Generation: m.mu.generation}
	snap := make([]RuleMetrics, 0, len(m.mu.rules)+len(m.mu.retired))
	for _, r := range m.mu.rules {
		snap = append(snap, r.metricsLocked())
	}
	for _, r := range m.mu.retired {
		snap = append(snap, r.metricsLocked())
	}
	out.Rules = snap
	return out
}

// release subtracts the given dimensions from hard usage and wakes queued
// waiters that become grantable.
func (m *Manager) release(demands []RuleDemand, dims ...Dimension) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, d := range demands {
		for _, dim := range dims {
			amt := d.Amount[dim]
			if amt == 0 {
				continue
			}
			if d.Rule.used[dim] < amt {
				// Internal accounting invariant; never go negative. In
				// non-invariant builds clamp to zero defensively.
				d.Rule.used[dim] = 0
			} else {
				d.Rule.used[dim] -= amt
			}
		}
	}
	m.pruneRetiredLocked()
	m.grantPendingLocked()
}

// applyUsageLocked adds (sign +1) or subtracts (sign -1) the demand amounts
// from hard usage.
func (m *Manager) applyUsageLocked(demands []RuleDemand, sign int64) {
	for _, d := range demands {
		for dim := Dimension(0); dim < numDimensions; dim++ {
			amt := sign * d.Amount[dim]
			d.Rule.used[dim] += amt
		}
	}
}

// blocking returns the first rule and dimension that prevent immediate
// satisfaction of the demand given current hard usage and all queued soft
// reservations.
func (m *Manager) blocking(demands []RuleDemand) (*Rule, Dimension, bool) {
	for _, d := range demands {
		for dim := Dimension(0); dim < numDimensions; dim++ {
			if !m.fitsLocked(d.Rule, dim, d.Amount[dim]) {
				return d.Rule, dim, false
			}
		}
	}
	return nil, 0, true
}

// fitsLocked reports whether adding amt to the rule/dimension stays within
// the finite limit given current usage and reservations (including requests
// queued ahead).
func (m *Manager) fitsLocked(r *Rule, dim Dimension, amt int64) bool {
	limit := r.spec.Limits[dim]
	if limit <= 0 {
		return true
	}
	return r.used[dim]+r.reserved[dim]+amt <= limit
}

// grantPendingLocked grants queued waiters in scheduling order (priority then
// FIFO). Waiters are popped in heap order; a waiter is granted when its demand
// fits alongside current hard usage and the soft reservations of earlier
// waiters that remain blocked in this sweep. Earlier blocked waiters therefore
// keep their demand protected from later grants (freedom from starvation);
// later waiters may still consume surplus on ranges the earlier waiters do not
// (fully) reserve. Waiters that cannot be granted yet are re-queued.
func (m *Manager) grantPendingLocked() {
	n := m.mu.queue.Len()
	if n == 0 {
		return
	}
	ordered := make([]*waiter, 0, n)
	for m.mu.queue.Len() > 0 {
		ordered = append(ordered, heap.Pop(&m.mu.queue).(*waiter))
	}
	blocked := make(map[*Rule][numDimensions]int64, n)
	requeued := make([]*waiter, 0, n)
	for _, w := range ordered {
		grant := true
		for _, d := range w.demands {
			bl := blocked[d.Rule]
			for dim := Dimension(0); dim < numDimensions; dim++ {
				limit := d.Rule.spec.Limits[dim]
				amt := d.Amount[dim]
				if amt > 0 && limit > 0 && d.Rule.used[dim]+amt+bl[dim] > limit {
					grant = false
				}
			}
		}
		if grant {
			m.releaseReservedLocked(w.demands)
			m.applyUsageLocked(w.demands, +1)
			for _, d := range w.demands {
				d.Rule.waiting--
				d.Rule.admits++
				d.Rule.waitDur += m.nowFn().Sub(w.enqueuedAt)
			}
			w.granted = true
			close(w.ch)
			continue
		}
		for _, d := range w.demands {
			cur := blocked[d.Rule]
			for dim := Dimension(0); dim < numDimensions; dim++ {
				cur[dim] += d.Amount[dim]
			}
			blocked[d.Rule] = cur
		}
		requeued = append(requeued, w)
	}
	for _, w := range requeued {
		heap.Push(&m.mu.queue, w)
	}
}

// removeWaiterLocked unlinks the waiter and decrements per-rule waiting
// counters. Its soft reservations are released separately by the caller.
func (m *Manager) removeWaiterLocked(target *waiter) {
	for i := 0; i < m.mu.queue.Len(); i++ {
		if m.mu.queue[i] == target {
			heap.Remove(&m.mu.queue, i)
			break
		}
	}
	for _, d := range target.demands {
		d.Rule.waiting--
	}
}

// releaseReservedLocked removes the given soft reservations.
func (m *Manager) releaseReservedLocked(demands []RuleDemand) {
	for _, d := range demands {
		for dim := Dimension(0); dim < numDimensions; dim++ {
			d.Rule.reserved[dim] -= d.Amount[dim]
		}
	}
}

// pruneRetiredLocked drops retired rules whose usage and reservations have
// fully drained.
func (m *Manager) pruneRetiredLocked() {
	retained := m.mu.retired[:0]
	for _, r := range m.mu.retired {
		if r.outstanding() {
			retained = append(retained, r)
		}
	}
	m.mu.retired = retained
}

// outstanding reports whether any hard usage, soft reservation, or queued
// waiter still references the rule.
func (r *Rule) outstanding() bool {
	for dim := Dimension(0); dim < numDimensions; dim++ {
		if r.used[dim] > 0 || r.reserved[dim] > 0 {
			return true
		}
	}
	return r.waiting > 0
}

// metricsLocked snapshots the rule's counters; the caller must hold m.mu.
func (r *Rule) metricsLocked() RuleMetrics {
	return RuleMetrics{
		Name:         r.spec.Name,
		Generation:   r.generation,
		Retired:      !r.active,
		Used:         r.used,
		Reserved:     r.reserved,
		Waiting:      r.waiting,
		Admits:       r.admits,
		Rejected:     r.rejected,
		Waited:       r.waited,
		WaitDuration: r.waitDur,
	}
}
