// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package writequota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func bytesSpec(name string, limit int64, policy Policy) RuleSpec {
	s := RuleSpec{Name: name, Policy: policy}
	s.Limits[PendingBytes] = limit
	return s
}

func bytesReq(r *Rule, n int64, prio int32) Request {
	d := RuleDemand{Rule: r}
	d.Amount[PendingBytes] = n
	return Request{Demands: []RuleDemand{d}, Priority: prio}
}

func crossReq(a, b *Rule) Request {
	da := RuleDemand{Rule: a}
	da.Amount[PendingBytes] = 10
	db := RuleDemand{Rule: b}
	db.Amount[PendingBytes] = 10
	return Request{Demands: []RuleDemand{da, db}}
}

func partialReq(r *Rule, n int64) []RuleDemand {
	d := RuleDemand{Rule: r}
	d.Amount[PendingBytes] = n
	return []RuleDemand{d}
}

func mustAdmit(t *testing.T, m *Manager, req Request) *Grant {
	t.Helper()
	g, err := m.Admit(context.Background(), req)
	if err != nil {
		t.Fatalf("expected grant, got %v", err)
	}
	return g
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not satisfied within %v", timeout)
}

func waitChan(t *testing.T, ch chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", msg)
	}
}

func assertNoSignal(t *testing.T, ch chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("unexpected signal: %s", msg)
	case <-time.After(15 * time.Millisecond):
	}
}

func ruleMetrics(m *Manager, name string) RuleMetrics {
	for _, rm := range m.Metrics().Rules {
		if rm.Name == name {
			return rm
		}
	}
	return RuleMetrics{}
}

func TestAdmitReleaseAndMetrics(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r1", 100, PolicyReject)})
	r := snap.Rules[0]

	g := mustAdmit(t, m, bytesReq(r, 40, 0))
	if got := ruleMetrics(m, "r1").Used[PendingBytes]; got != 40 {
		t.Fatalf("used = %d, want 40", got)
	}
	if g.WaitDuration() != 0 {
		t.Fatalf("expected no wait, got %v", g.WaitDuration())
	}
	if got := ruleMetrics(m, "r1").Used[ConcurrentBatches]; got != 0 {
		t.Fatalf("unexpected concurrent usage: %d", got)
	}

	g.ReleaseUnflushed()
	g.ReleaseCommitted()
	g.ReleaseCommitted() // idempotent
	g.ReleaseUnflushed()
	if got := ruleMetrics(m, "r1").Used[PendingBytes]; got != 0 {
		t.Fatalf("used after release = %d, want 0", got)
	}
	if got := ruleMetrics(m, "r1").Admits; got != 1 {
		t.Fatalf("admits = %d, want 1", got)
	}

	g2 := mustAdmit(t, m, Request{})
	g2.ReleaseCommitted()
	var nilGrant *Grant
	nilGrant.ReleaseCommitted()
	nilGrant.ReleaseUnflushed()
}

func TestSeed(t *testing.T) {
	m := New(nil)
	spec := RuleSpec{Name: "r1", Policy: PolicyReject}
	spec.Limits[UnflushedBytes] = 100
	snap := m.Configure([]RuleSpec{spec})
	r := snap.Rules[0]

	seedDemand := RuleDemand{Rule: r}
	seedDemand.Amount[UnflushedBytes] = 30
	g := m.Seed([]RuleDemand{seedDemand})
	if got := ruleMetrics(m, "r1").Used[UnflushedBytes]; got != 30 {
		t.Fatalf("seeded unflushed = %d, want 30", got)
	}
	req := Request{Demands: []RuleDemand{func() RuleDemand {
		d := RuleDemand{Rule: r}
		d.Amount[UnflushedBytes] = 80
		return d
	}()}}
	if _, err := m.Admit(context.Background(), req); !errors.Is(err, ErrExceeded) {
		t.Fatalf("expected ErrExceeded, got %v", err)
	}
	g.ReleaseUnflushed()
	if got := ruleMetrics(m, "r1").Used[UnflushedBytes]; got != 0 {
		t.Fatalf("unflushed after release = %d, want 0", got)
	}
}

func TestPolicyReject(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r1", 10, PolicyReject)})
	r := snap.Rules[0]

	g := mustAdmit(t, m, bytesReq(r, 10, 0))
	_, err := m.Admit(context.Background(), bytesReq(r, 1, 0))
	var ee *ExceededError
	if !errors.As(err, &ee) || !errors.Is(err, ErrExceeded) {
		t.Fatalf("expected ExceededError wrapping ErrExceeded, got %v", err)
	}
	if ee.RuleName != "r1" || ee.Dimension != PendingBytes {
		t.Fatalf("unexpected error attribution: %+v", ee)
	}
	g.ReleaseCommitted()
	mustAdmit(t, m, bytesReq(r, 10, 0)).ReleaseCommitted()

	mt := ruleMetrics(m, "r1")
	if mt.Rejected[PendingBytes] != 1 {
		t.Fatalf("rejected = %d, want 1", mt.Rejected[PendingBytes])
	}
	if mt.Admits != 2 {
		t.Fatalf("admits = %d, want 2", mt.Admits)
	}
}

// grantOrder records the order in which writer goroutines are granted.
type grantOrder struct {
	mu  sync.Mutex
	got []int
}

func (o *grantOrder) record(i int) {
	o.mu.Lock()
	o.got = append(o.got, i)
	o.mu.Unlock()
}

func (o *grantOrder) snapshot() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int(nil), o.got...)
}

// runQueuedWriters starts n goroutines that admit 10-byte requests with the
// given per-goroutine priorities. Goroutines enqueue in index order (gated by
// the test), so that index order matches FIFO arrival order. When granted,
// the goroutine records its index in order and then blocks until
// release[index] is closed, after which it releases the grant.
func runQueuedWriters(t *testing.T, m *Manager, r *Rule, n int, prios []int32) (*grantOrder, []chan struct{}) {
	t.Helper()
	order := &grantOrder{}
	release := make([]chan struct{}, n)
	start := make([]chan struct{}, n)
	for i := 0; i < n; i++ {
		release[i] = make(chan struct{})
		start[i] = make(chan struct{})
		i := i
		go func() {
			<-start[i]
			g, err := m.Admit(context.Background(), bytesReq(r, 10, prios[i]))
			if err != nil {
				return
			}
			order.record(i)
			<-release[i]
			g.ReleaseCommitted()
		}()
	}
	for i := 0; i < n; i++ {
		close(start[i])
		waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "r1").Waiting == int64(i+1) })
	}
	return order, release
}

func waitGrantedPrefix(t *testing.T, order *grantOrder, want []int, msg string) {
	t.Helper()
	waitForCondition(t, time.Second, func() bool {
		got := order.snapshot()
		if len(got) < len(want) {
			return false
		}
		for i := range want {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	})
	if got := order.snapshot(); len(got) != len(want) {
		t.Fatalf("%s: grant order = %v, want prefix %v", msg, got, want)
	}
}

// TestPolicyWaitFIFO verifies strict FIFO ordering; caller priority values
// must be ignored by PolicyWait rules.
func TestPolicyWaitFIFO(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r1", 10, PolicyWait)})
	r := snap.Rules[0]

	holder := mustAdmit(t, m, bytesReq(r, 10, 0))
	order, release := runQueuedWriters(t, m, r, 3, []int32{0, 10, 5})
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "r1").Waiting == 3 })
	mt := ruleMetrics(m, "r1")
	if mt.Used[PendingBytes] != 10 || mt.Reserved[PendingBytes] != 30 {
		t.Fatalf("unexpected accounting while queued: used=%d reserved=%d", mt.Used, mt.Reserved)
	}

	holder.ReleaseCommitted()
	waitGrantedPrefix(t, order, []int{0}, "FIFO first grant")

	// Free waiter 2 first while waiter 1 is next in line: 2 must not be
	// granted before 1.
	close(release[0])
	waitGrantedPrefix(t, order, []int{0, 1}, "FIFO second grant")
	close(release[2])
	if got := order.snapshot(); len(got) != 2 {
		t.Fatalf("waiter 2 jumped ahead of FIFO: %v", got)
	}
	close(release[1])
	waitGrantedPrefix(t, order, []int{0, 1, 2}, "FIFO third grant")
	waitForCondition(t, time.Second, func() bool {
		mt := ruleMetrics(m, "r1")
		return mt.Waiting == 0 && mt.Used[PendingBytes] == 0
	})
}

// TestPolicyPriority verifies priority ordering, FIFO within same priority.
func TestPolicyPriority(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r1", 10, PolicyPriority)})
	r := snap.Rules[0]

	holder := mustAdmit(t, m, bytesReq(r, 10, 0))
	// Enqueue order 0:prio0, 1:prio10, 2:prio10, 3:prio5. Expected grant
	// order: 1, 2, 3, 0.
	order, release := runQueuedWriters(t, m, r, 4, []int32{0, 10, 10, 5})
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "r1").Waiting == 4 })

	holder.ReleaseCommitted()
	for k, want := range []int{1, 2, 3, 0} {
		wantLen := k + 1
		// Wait until exactly wantLen grants have been recorded and the tail
		// is the expected index.
		waitForCondition(t, time.Second, func() bool {
			got := order.snapshot()
			return len(got) == wantLen && got[wantLen-1] == want
		})
		close(release[want])
	}
	waitGrantedPrefix(t, order, []int{1, 2, 3, 0}, "priority final order")
	waitForCondition(t, time.Second, func() bool {
		mt := ruleMetrics(m, "r1")
		return mt.Waiting == 0 && mt.Used[PendingBytes] == 0
	})
}

// TestPolicyRejectRange verifies cross-range atomic rejection semantics.
func TestPolicyRejectRange(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{
		bytesSpec("a", 10, PolicyRejectRange),
		bytesSpec("b", 10, PolicyRejectRange),
	})
	a, b := snap.Rules[0], snap.Rules[1]

	mustAdmit(t, m, crossReq(a, b)).ReleaseCommitted()

	holderA := mustAdmit(t, m, bytesReq(a, 10, 0))
	if _, err := m.Admit(context.Background(), crossReq(a, b)); !errors.Is(err, ErrExceeded) {
		t.Fatalf("expected ErrExceeded, got %v", err)
	}
	if mt := ruleMetrics(m, "b"); mt.Used[PendingBytes] != 0 || mt.Waiting != 0 {
		t.Fatalf("rule b partially affected: %+v", mt)
	}
	mustAdmit(t, m, bytesReq(b, 10, 0)).ReleaseCommitted()
	if _, err := m.Admit(context.Background(), bytesReq(a, 1, 0)); !errors.Is(err, ErrExceeded) {
		t.Fatalf("expected ErrExceeded for single rule, got %v", err)
	}
	holderA.ReleaseCommitted()
}

// TestCrossRangeWaitIsAtomic verifies a queued cross-range request holds no
// hard usage anywhere and is granted atomically on both rules.
func TestCrossRangeWaitIsAtomic(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{
		bytesSpec("a", 10, PolicyWait),
		bytesSpec("b", 10, PolicyWait),
	})
	a, b := snap.Rules[0], snap.Rules[1]

	holderA := mustAdmit(t, m, bytesReq(a, 10, 0))
	result := make(chan *Grant, 1)
	go func() {
		g, err := m.Admit(context.Background(), crossReq(a, b))
		if err != nil {
			result <- nil
			return
		}
		result <- g
	}()
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "a").Waiting == 1 })
	if mt := ruleMetrics(m, "a"); mt.Used[PendingBytes] != 10 || mt.Reserved[PendingBytes] != 10 {
		t.Fatalf("rule a accounting: %+v", mt)
	}
	if mt := ruleMetrics(m, "b"); mt.Used[PendingBytes] != 0 || mt.Reserved[PendingBytes] != 10 {
		t.Fatalf("rule b accounting: %+v", mt)
	}
	holderA.ReleaseCommitted()
	var g *Grant
	waitForCondition(t, time.Second, func() (ok bool) {
		select {
		case g = <-result:
			return g != nil
		default:
			return false
		}
	})
	if mt := ruleMetrics(m, "a"); mt.Used[PendingBytes] != 10 {
		t.Fatalf("rule a used = %d, want 10", mt.Used)
	}
	if mt := ruleMetrics(m, "b"); mt.Used[PendingBytes] != 10 {
		t.Fatalf("rule b used = %d, want 10", mt.Used)
	}
	g.ReleaseCommitted()
}

// TestSoftReservationFairness verifies surplus capacity on non-head ranges
// stays usable while the head waiter's reserved share is protected.
func TestSoftReservationFairness(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{
		bytesSpec("a", 10, PolicyWait),
		bytesSpec("b", 10, PolicyWait),
	})
	a, b := snap.Rules[0], snap.Rules[1]

	holderA := mustAdmit(t, m, bytesReq(a, 10, 0))
	headDone := make(chan struct{})
	go func() {
		req := Request{Demands: append(partialReq(a, 10), partialReq(b, 5)...)}
		g, err := m.Admit(context.Background(), req)
		if err != nil {
			return
		}
		close(headDone)
		g.ReleaseCommitted()
	}()
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "a").Waiting == 1 })

	// Surplus 5 bytes on b remain usable while head waits.
	for i := 0; i < 3; i++ {
		mustAdmit(t, m, bytesReq(b, 5, 0)).ReleaseCommitted()
	}
	// A 6-byte request would consume head's reserved share and must queue.
	bigGranted := make(chan struct{})
	go func() {
		g, err := m.Admit(context.Background(), bytesReq(b, 6, 0))
		if err != nil {
			return
		}
		close(bigGranted)
		g.ReleaseCommitted()
	}()
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "b").Waiting == 1 })
	assertNoSignal(t, bigGranted, "b-6 request must not be granted while head holds 5")

	// Free a: head is granted immediately. Once the head then releases its
	// bytes (headDone is closed right before release), the b-6 request fits
	// and is granted as well.
	holderA.ReleaseCommitted()
	waitChan(t, headDone, time.Second, "head grant")
	waitChan(t, bigGranted, time.Second, "b-6 grant after head releases")
	waitForCondition(t, time.Second, func() bool {
		mt := ruleMetrics(m, "b")
		return mt.Waiting == 0 && mt.Used[PendingBytes] == 0
	})
}

// TestCancelWhileWaiting verifies cancellation releases reservations and
// wakes successors without leaking usage.
func TestCancelWhileWaiting(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r1", 10, PolicyWait)})
	r := snap.Rules[0]
	holder := mustAdmit(t, m, bytesReq(r, 10, 0))

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := m.Admit(ctx, bytesReq(r, 10, 0)); errCh <- err }()
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "r1").Waiting == 1 })
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	mt := ruleMetrics(m, "r1")
	if mt.Reserved[PendingBytes] != 0 || mt.Waiting != 0 {
		t.Fatalf("reservation leaked: reserved=%d waiting=%d", mt.Reserved, mt.Waiting)
	}
	// Successor admitted after release.
	holder.ReleaseCommitted()
	mustAdmit(t, m, bytesReq(r, 10, 0)).ReleaseCommitted()
}

// TestCancelGrantRace stresses the cancel/grant race: exactly one outcome,
// no accounting leak.
func TestCancelGrantRace(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r1", 1, PolicyWait)})
	r := snap.Rules[0]
	const iterations = 500
	var wg sync.WaitGroup
	for i := 0; i < iterations; i++ {
		holder := mustAdmit(t, m, bytesReq(r, 1, 0))
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		grantCh := make(chan *Grant, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, err := m.Admit(ctx, bytesReq(r, 1, 0))
			result <- err
			grantCh <- g
		}()
		waitForCondition(t, 5*time.Second, func() bool { return ruleMetrics(m, "r1").Waiting == 1 })
		if i%2 == 0 {
			cancel()
			holder.ReleaseCommitted()
		} else {
			holder.ReleaseCommitted()
			cancel()
		}
		err := <-result
		g := <-grantCh
		if err == nil {
			g.ReleaseCommitted()
		} else if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected err %v", err)
		}
		cancel()
	}
	wg.Wait()
	mt := ruleMetrics(m, "r1")
	if mt.Used[PendingBytes] != 0 || mt.Reserved[PendingBytes] != 0 || mt.Waiting != 0 {
		t.Fatalf("leaked accounting: %+v", mt)
	}
}

// TestCloseWakesWaiters verifies Close wakes waiters with ErrClosed and
// outstanding grants remain releasable.
func TestCloseWakesWaiters(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r1", 10, PolicyWait)})
	r := snap.Rules[0]
	holder := mustAdmit(t, m, bytesReq(r, 10, 0))

	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() { _, err := m.Admit(context.Background(), bytesReq(r, 10, 0)); errs <- err }()
	}
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "r1").Waiting == 4 })
	m.Close()
	for i := 0; i < 4; i++ {
		if err := <-errs; !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed, got %v", err)
		}
	}
	if _, err := m.Admit(context.Background(), bytesReq(r, 1, 0)); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after close, got %v", err)
	}
	holder.ReleaseCommitted()
}

// TestReconfigureBinding verifies retired-rule binding and draining.
func TestReconfigureBinding(t *testing.T) {
	m := New(nil)
	s1 := m.Configure([]RuleSpec{bytesSpec("r", 10, PolicyWait)})
	old := s1.Rules[0]

	holder := mustAdmit(t, m, bytesReq(old, 10, 0))
	go func() {
		g, err := m.Admit(context.Background(), bytesReq(old, 10, 0))
		if err != nil {
			return
		}
		g.ReleaseCommitted()
	}()
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "r").Waiting == 1 })

	s2 := m.Configure([]RuleSpec{bytesSpec("other", 10, PolicyReject)})
	if s2.Generation != 2 {
		t.Fatalf("generation = %d, want 2", s2.Generation)
	}
	var retired, active int
	for _, rm := range m.Metrics().Rules {
		if rm.Retired {
			retired++
		} else {
			active++
		}
	}
	if retired != 1 || active != 1 {
		t.Fatalf("retired=%d active=%d, want 1/1", retired, active)
	}

	mustAdmit(t, m, bytesReq(s2.Rules[0], 10, 0)).ReleaseCommitted()

	holder.ReleaseCommitted()
	waitForCondition(t, time.Second, func() bool {
		for _, rm := range m.Metrics().Rules {
			if rm.Retired {
				return false
			}
		}
		return true
	})
}

// TestReconfigurePreservesWaiters verifies a reconfigure neither grants nor
// rejects queued waiters.
func TestReconfigurePreservesWaiters(t *testing.T) {
	m := New(nil)
	s1 := m.Configure([]RuleSpec{bytesSpec("r", 10, PolicyWait)})
	holder := mustAdmit(t, m, bytesReq(s1.Rules[0], 10, 0))
	errCh := make(chan error, 1)
	go func() {
		_, err := m.Admit(context.Background(), bytesReq(s1.Rules[0], 10, 0))
		errCh <- err
	}()
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "r").Waiting == 1 })
	m.Configure([]RuleSpec{bytesSpec("r", 5, PolicyWait)})
	select {
	case err := <-errCh:
		t.Fatalf("waiter resolved during reconfigure with %v", err)
	case <-time.After(15 * time.Millisecond):
	}
	holder.ReleaseCommitted()
	if err := <-errCh; err != nil {
		t.Fatalf("waiter should have been granted against old rule, got %v", err)
	}
}

// TestMixedPolicyCombination verifies effective-policy combination across
// rules with different policies.
func TestMixedPolicyCombination(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{
		bytesSpec("wait", 10, PolicyWait),
		bytesSpec("rej", 10, PolicyReject),
	})
	waitR, rejR := snap.Rules[0], snap.Rules[1]

	// Scenario 1: only the Wait rule is unsatisfiable; the Reject rule is
	// free. The cross-range request queues and completes once wait frees.
	holderWait := mustAdmit(t, m, bytesReq(waitR, 10, 0))
	errCh := make(chan error, 1)
	go func() {
		g, err := m.Admit(context.Background(), crossReq(waitR, rejR))
		if err == nil {
			g.ReleaseCommitted()
		}
		errCh <- err
	}()
	waitForCondition(t, time.Second, func() bool { return ruleMetrics(m, "wait").Waiting == 1 })
	holderWait.ReleaseCommitted()
	if err := <-errCh; err != nil {
		t.Fatalf("expected queued request to complete, got %v", err)
	}

	// Scenario 2: both rules are unsatisfiable at admission time. A blocking
	// dimension governed by the Reject rule makes the request reject instead
	// of queue.
	holderWait2 := mustAdmit(t, m, bytesReq(waitR, 10, 0))
	holderRej := mustAdmit(t, m, bytesReq(rejR, 10, 0))
	if _, err := m.Admit(context.Background(), crossReq(waitR, rejR)); !errors.Is(err, ErrExceeded) {
		t.Fatalf("expected ErrExceeded from blocking reject rule, got %v", err)
	}
	holderWait2.ReleaseCommitted()
	holderRej.ReleaseCommitted()
}

// TestDoubleReleaseNoNegative verifies defensive clamping and idempotency.
func TestDoubleReleaseNoNegative(t *testing.T) {
	m := New(nil)
	snap := m.Configure([]RuleSpec{bytesSpec("r", 10, PolicyReject)})
	g := mustAdmit(t, m, bytesReq(snap.Rules[0], 5, 0))
	g.ReleaseCommitted()
	g.ReleaseCommitted()
	if got := ruleMetrics(m, "r").Used[PendingBytes]; got != 0 {
		t.Fatalf("used = %d, want 0", got)
	}
}
