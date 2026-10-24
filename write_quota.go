// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/writequota"
)

// WriteQuotaPolicy decides how a write is affected when a rule's finite
// limits cannot immediately satisfy it.
type WriteQuotaPolicy = writequota.Policy

const (
	// WriteQuotaReject rejects writes immediately when the rule's limits
	// cannot be satisfied.
	WriteQuotaReject = writequota.PolicyReject
	// WriteQuotaWait queues writes FIFO until the rule's limits can be
	// satisfied.
	WriteQuotaWait = writequota.PolicyWait
	// WriteQuotaPriority queues writes by request priority (see
	// WriteOptions.Priority), FIFO within the same priority.
	WriteQuotaPriority = writequota.PolicyPriority
	// WriteQuotaRejectRange behaves like WriteQuotaReject for writes touching
	// a single rule; writes touching two or more rules are rejected whenever
	// they cannot be immediately satisfied by every touched rule (cross-range
	// writes never partially wait).
	WriteQuotaRejectRange = writequota.PolicyRejectRange
)

// WriteQuotaRule is one user-key range write quota rule.
//
// The rule governs user keys in the half-open interval [Bounds.Start,
// Bounds.End). A zero-length (nil or empty) Start or End denotes the
// beginning, respectively the end, of the keyspace. Rules may overlap; in
// that case a point key is attributed to the highest-Priority rule, and
// overlapping rules must have distinct priorities (validated at Open and on
// reconfiguration).
type WriteQuotaRule struct {
	// Name is the stable name of the rule, unique within a configuration. It
	// is used in metrics and in quota-exceeded errors.
	Name string
	// Bounds is the half-open user-key interval the rule applies to.
	Bounds KeyRange
	// Priority resolves overlapping rules: a point key is attributed to the
	// overlapping rule with the highest Priority. Overlapping rules must have
	// distinct priorities. Higher values indicate higher priority.
	Priority int32
	// Policy determines behavior when the rule's finite limits cannot
	// immediately satisfy a request. The zero value is WriteQuotaReject.
	Policy WriteQuotaPolicy

	// PendingBytes limits the logical record bytes of in-flight submissions
	// attributed to the rule. Zero disables the dimension.
	PendingBytes int64
	// PendingOps limits the number of mutating operations in in-flight
	// submissions attributed to the rule. Zero disables the dimension.
	PendingOps int64
	// UnflushedBytes limits the estimated memory that admitted writes may
	// occupy in unflushed flushables attributed to the rule. Zero disables
	// the dimension.
	UnflushedBytes int64
	// ConcurrentBatches limits the number of concurrently in-flight
	// submissions attributed to the rule. Zero disables the dimension.
	ConcurrentBatches int64
}

// WriteQuotaConfig is an immutable set of write quota rules. A value must not
// be mutated after it has been returned from a WriteQuotaConfigFunc (the
// static constructor defensively copies the input slice).
type WriteQuotaConfig struct {
	// Rules are the configured rules.
	Rules []WriteQuotaRule
}

// WriteQuotaConfigFunc returns the active write quota configuration. The
// feature is enabled whenever Options.WriteQuotaConfig is non-nil; returning
// a pointer to a new *WriteQuotaConfig atomically reconfigures the database,
// while returning the same pointer leaves the active configuration
// unchanged. In-flight writes remain bound to the configuration they were
// admitted under.
//
// The function must be cheap (it is invoked on every write) and must not
// block or perform I/O.
type WriteQuotaConfigFunc func() *WriteQuotaConfig

// MakeStaticWriteQuotaConfig returns a WriteQuotaConfigFunc that always
// returns the same immutable configuration for the given rules. The rules are
// validated using cmp; invalid rules panic (mirroring
// MakeStaticSpanPolicyFunc).
func MakeStaticWriteQuotaConfig(cmp Compare, rules ...WriteQuotaRule) WriteQuotaConfigFunc {
	cp := append([]WriteQuotaRule(nil), rules...)
	cfg := &WriteQuotaConfig{Rules: cp}
	if err := cfg.validate(cmp); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "invalid write quota config"))
	}
	return func() *WriteQuotaConfig { return cfg }
}

// validate checks rule names, bounds, limit values, policy values and the
// overlap/priority invariant. cmp is the user key comparer.
func (c *WriteQuotaConfig) validate(cmp base.Compare) error {
	if c == nil {
		return errors.AssertionFailedf("write quota config is nil")
	}
	var buf strings.Builder
	addErr := func(format string, args ...interface{}) {
		fmt.Fprintf(&buf, format, args...)
		buf.WriteByte('\n')
	}
	names := make(map[string]struct{}, len(c.Rules))
	for i := range c.Rules {
		r := &c.Rules[i]
		if r.Name == "" {
			addErr("write quota rule at index %d has an empty Name", i)
		} else if _, ok := names[r.Name]; ok {
			addErr("write quota rule %q has a duplicate Name", r.Name)
		} else {
			names[r.Name] = struct{}{}
		}
		switch r.Policy {
		case WriteQuotaReject, WriteQuotaWait, WriteQuotaPriority, WriteQuotaRejectRange:
		default:
			addErr("write quota rule %q has invalid policy %d", r.Name, r.Policy)
		}
		if r.PendingBytes < 0 || r.PendingOps < 0 || r.UnflushedBytes < 0 || r.ConcurrentBatches < 0 {
			addErr("write quota rule %q has a negative limit", r.Name)
		}
		if len(r.Bounds.Start) > 0 && len(r.Bounds.End) > 0 && cmp(r.Bounds.Start, r.Bounds.End) >= 0 {
			addErr("write quota rule %q has invalid bounds: Start must be sort before End", r.Name)
		}
	}
	// Overlap check: overlapping rules must have distinct priorities.
	for i := 0; i < len(c.Rules); i++ {
		for j := i + 1; j < len(c.Rules); j++ {
			a, b := &c.Rules[i], &c.Rules[j]
			if boundsOverlap(cmp, a.Bounds, b.Bounds) && a.Priority == b.Priority {
				addErr("write quota rules %q and %q overlap and have the same priority %d; "+
					"overlapping rules require distinct priorities", a.Name, b.Name, a.Priority)
			}
		}
	}
	if buf.Len() > 0 {
		return errors.New(strings.TrimSuffix(buf.String(), "\n"))
	}
	return nil
}

// boundsOverlap reports whether the half-open user-key intervals overlap,
// treating zero-length Start/End boundaries as -inf/+inf.
func boundsOverlap(cmp base.Compare, a, b KeyRange) bool {
	// a.Start < b.End && b.Start < a.End, with empty bounds as infinities.
	if len(a.Start) > 0 && len(b.End) > 0 && cmp(a.Start, b.End) >= 0 {
		return false
	}
	if len(b.Start) > 0 && len(a.End) > 0 && cmp(b.Start, a.End) >= 0 {
		return false
	}
	return true
}

// boundsContains reports whether key is within [b.Start, b.End), with
// zero-length boundaries as -inf/+inf.
func boundsContains(cmp base.Compare, b KeyRange, key []byte) bool {
	if len(b.Start) > 0 && cmp(b.Start, key) > 0 {
		return false
	}
	if len(b.End) > 0 && cmp(key, b.End) >= 0 {
		return false
	}
	return true
}

// compiledQuotaRule is a compiled rule: public definition plus the admission-core
// spec and derived state.
type compiledQuotaRule struct {
	def  WriteQuotaRule
	spec writequota.RuleSpec
	// rule is the admission-core handle for the active generation. It is nil
	// for the zero-value and set by compile.
	rule *writequota.Rule
}

// quotaConfig is an immutable, compiled configuration snapshot.
type quotaConfig struct {
	def   *WriteQuotaConfig
	cmp   base.Compare
	rules []compiledQuotaRule
	// orderedByStart holds rule indices sorted by Start (empty first).
	orderedByStart []int
	// overlaps records whether any pair of rules overlaps; when false the
	// matcher uses binary search instead of a linear scan.
	overlaps bool
	snapshot writequota.Snapshot
}

// compile validates and compiles cfg against the admission manager's next
// snapshot. The caller must hold no manager state.
func compileQuotaConfig(
	wm *writequota.Manager, cmp base.Compare, cfg *WriteQuotaConfig,
) (*quotaConfig, error) {
	if cfg == nil {
		return nil, nil
	}
	if err := cfg.validate(cmp); err != nil {
		return nil, err
	}
	specs := make([]writequota.RuleSpec, len(cfg.Rules))
	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		specs[i] = writequota.RuleSpec{
			Name:   r.Name,
			Policy: r.Policy,
			Limits: [writequota.NumDimensions]int64{
				writequota.Dimension(0): r.PendingBytes,
				writequota.Dimension(1): r.PendingOps,
				writequota.Dimension(2): r.UnflushedBytes,
				writequota.Dimension(3): r.ConcurrentBatches,
			},
		}
	}
	snapshot := wm.Configure(specs)
	qc := &quotaConfig{
		def:      cfg,
		cmp:      cmp,
		rules:    make([]compiledQuotaRule, len(cfg.Rules)),
		snapshot: snapshot,
	}
	for i := range cfg.Rules {
		qc.rules[i] = compiledQuotaRule{def: cfg.Rules[i], spec: specs[i], rule: snapshot.Rules[i]}
	}
	qc.orderedByStart = make([]int, len(cfg.Rules))
	for i := range qc.orderedByStart {
		qc.orderedByStart[i] = i
	}
	slices.SortFunc(qc.orderedByStart, func(a, b int) int {
		// Empty start sorts first.
		as, bs := qc.rules[a].def.Bounds.Start, qc.rules[b].def.Bounds.Start
		if len(as) == 0 || len(bs) == 0 {
			if len(as) == 0 && len(bs) == 0 {
				return 0
			}
			if len(as) == 0 {
				return -1
			}
			return 1
		}
		return cmp(as, bs)
	})
	for i := 0; i < len(cfg.Rules) && !qc.overlaps; i++ {
		for j := i + 1; j < len(cfg.Rules); j++ {
			if boundsOverlap(cmp, cfg.Rules[i].Bounds, cfg.Rules[j].Bounds) {
				qc.overlaps = true
				break
			}
		}
	}
	return qc, nil
}

// pointRule returns the rule that owns the point key (highest priority among
// containing overlapping rules), or nil if no rule contains it.
func (qc *quotaConfig) pointRule(key []byte) *compiledQuotaRule {
	if qc == nil {
		return nil
	}
	if qc.overlaps {
		return qc.pointRuleLinear(key)
	}
	return qc.pointRuleBinary(key)
}

func (qc *quotaConfig) pointRuleLinear(key []byte) *compiledQuotaRule {
	var best *compiledQuotaRule
	for i := range qc.rules {
		r := &qc.rules[i]
		if !boundsContains(qc.cmp, r.def.Bounds, key) {
			continue
		}
		if best == nil || r.def.Priority > best.def.Priority {
			best = r
		}
	}
	return best
}

// pointRuleBinary resolves the containing rule for the common non-overlapping
// layout via binary search on sorted starts.
func (qc *quotaConfig) pointRuleBinary(key []byte) *compiledQuotaRule {
	// Find the first index whose Start is greater than or equal to key; an
	// equal start is itself a candidate (starts are inclusive).
	order := qc.orderedByStart
	idx, eq := slices.BinarySearchFunc(order, key, func(ruleIdx int, k []byte) int {
		s := qc.rules[ruleIdx].def.Bounds.Start
		if len(s) == 0 {
			return -1
		}
		return qc.cmp(s, k)
	})
	first := idx - 1
	if eq {
		first = idx
	}
	for i := first; i >= 0; i-- {
		r := &qc.rules[order[i]]
		if boundsContains(qc.cmp, r.def.Bounds, key) {
			return r
		}
	}
	return nil
}

// rangeRules appends every rule overlapping span to dst and returns it. Rule
// bounds are half-open with zero-length boundaries treated as -inf/+inf.
func (qc *quotaConfig) rangeRules(dst []*compiledQuotaRule, span base.UserKeyBounds) []*compiledQuotaRule {
	if qc == nil {
		return dst
	}
	for i := range qc.rules {
		b := qc.rules[i].def.Bounds
		if ruleOverlapsSpan(qc.cmp, b, span) {
			dst = append(dst, &qc.rules[i])
		}
	}
	return dst
}

// ruleOverlapsSpan reports whether the half-open rule interval (with zero-length
// boundaries as infinities) overlaps a user-key span whose end may be
// inclusive.
func ruleOverlapsSpan(cmp base.Compare, rule KeyRange, span base.UserKeyBounds) bool {
	// rule.Start must be at or below the span's upper boundary. A zero-length
	// rule.Start is -inf and always satisfies this.
	if len(rule.Start) > 0 && !span.End.IsUpperBoundFor(cmp, rule.Start) {
		return false
	}
	// span.Start must sort strictly before the rule's exclusive End. A
	// zero-length rule.End is +inf and always satisfies this.
	if len(rule.End) > 0 && cmp(span.Start, rule.End) >= 0 {
		return false
	}
	return true
}

// writeQuotaManager owns the admission manager and the active compiled
// configuration. A nil *writeQuotaManager represents the disabled feature
// and all its methods are no-ops/fast-path accordingly.
type writeQuotaManager struct {
	wm     *writequota.Manager
	cmp    base.Compare
	logger Logger
	get    WriteQuotaConfigFunc

	cur atomic.Pointer[quotaConfig]
}

// newWriteQuotaManager builds the manager from the initial configuration. It
// returns nil when fn is nil (feature disabled).
func newWriteQuotaManager(cmp base.Compare, logger Logger, fn WriteQuotaConfigFunc) (*writeQuotaManager, error) {
	if fn == nil {
		return nil, nil
	}
	m := &writeQuotaManager{
		wm:     writequota.New(nil),
		cmp:    cmp,
		logger: logger,
		get:    fn,
	}
	cfg := fn()
	if cfg == nil {
		// Non-nil func returning nil: treat as an explicit empty
		// configuration (no active rules, feature enabled).
		cfg = &WriteQuotaConfig{}
	}
	qc, err := compileQuotaConfig(m.wm, cmp, cfg)
	if err != nil {
		return nil, err
	}
	m.cur.Store(qc)
	return m, nil
}

// current returns the active compiled configuration, atomically
// reconfiguring when the closure returns a new config pointer. An invalid new
// configuration is rejected: it is logged and the previous configuration
// remains active.
func (m *writeQuotaManager) current() *quotaConfig {
	if m == nil {
		return nil
	}
	cur := m.cur.Load()
	cfg := m.get()
	if cfg == nil {
		return cur
	}
	if cur != nil && cfg == cur.def {
		return cur
	}
	next, err := compileQuotaConfig(m.wm, m.cmp, cfg)
	if err != nil {
		m.logger.Errorf("pebble: ignoring invalid write quota configuration: %v", err)
		return cur
	}
	m.cur.Store(next)
	return next
}

// close shuts down the admission manager, waking queued waiters.
func (m *writeQuotaManager) close() {
	if m == nil {
		return
	}
	m.wm.Close()
}

// metrics returns the admission-core metrics, or zero when disabled.
func (m *writeQuotaManager) metrics() writequota.Metrics {
	if m == nil {
		return writequota.Metrics{}
	}
	return m.wm.Metrics()
}

// ErrWriteQuotaExceeded is returned (wrapped by WriteQuotaExceededError) when
// a write is rejected because a write-quota rule's finite limits could not be
// satisfied. Use errors.Is to test for it.
var ErrWriteQuotaExceeded = writequota.ErrExceeded

// WriteQuotaExceededError attributes a rejected write to the blocking write
// quota rule and dimension.
type WriteQuotaExceededError = writequota.ExceededError

// mapWriteQuotaAdmitErr translates admission-core errors into pebble-level
// errors. Context errors and ErrWriteQuotaExceeded are returned unchanged.
func mapWriteQuotaAdmitErr(err error) error {
	if errors.Is(err, writequota.ErrClosed) {
		return ErrClosed
	}
	return err
}
