// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

func quotaRule(name, start, end string, prio int32, policy WriteQuotaPolicy) WriteQuotaRule {
	r := WriteQuotaRule{Name: name, Priority: prio, Policy: policy}
	r.Bounds.Start = []byte(start)
	r.Bounds.End = []byte(end)
	return r
}

func TestWriteQuotaConfigValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cmp := DefaultComparer.Compare
	valid := []WriteQuotaRule{
		quotaRule("a", "a", "m", 0, WriteQuotaReject),
		quotaRule("b", "m", "z", 0, WriteQuotaWait),
	}
	if err := (&WriteQuotaConfig{Rules: valid}).validate(cmp); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
	// A full-keyspace rule overlaps both; distinct priority makes it valid.
	withWhole := append([]WriteQuotaRule(nil), valid...)
	withWhole = append(withWhole, quotaRule("whole", "", "", 1, WriteQuotaRejectRange))
	if err := (&WriteQuotaConfig{Rules: withWhole}).validate(cmp); err != nil {
		t.Fatalf("expected distinct-priority full-keyspace rule valid, got %v", err)
	}
	// Overlapping rules with distinct priorities are valid.
	overlap := append([]WriteQuotaRule(nil), valid...)
	overlap = append(overlap, quotaRule("a-hi", "a", "c", 1, WriteQuotaPriority))
	if err := (&WriteQuotaConfig{Rules: overlap}).validate(cmp); err != nil {
		t.Fatalf("expected distinct-priority overlap valid, got %v", err)
	}

	for _, tc := range []struct {
		name  string
		rules []WriteQuotaRule
	}{
		{"empty name", []WriteQuotaRule{quotaRule("", "a", "b", 0, WriteQuotaReject)}},
		{"duplicate name", []WriteQuotaRule{
			quotaRule("a", "a", "c", 0, WriteQuotaReject),
			quotaRule("a", "c", "e", 0, WriteQuotaReject),
		}},
		{"inverted bounds", []WriteQuotaRule{quotaRule("x", "z", "a", 0, WriteQuotaReject)}},
		{"negative limit", []WriteQuotaRule{func() WriteQuotaRule {
			r := quotaRule("x", "a", "z", 0, WriteQuotaReject)
			r.PendingBytes = -1
			return r
		}()}},
		{"invalid policy", []WriteQuotaRule{quotaRule("x", "a", "z", 0, WriteQuotaPolicy(42))}},
		{"overlap same priority", []WriteQuotaRule{
			quotaRule("a", "a", "m", 0, WriteQuotaReject),
			quotaRule("b", "l", "z", 0, WriteQuotaWait),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := (&WriteQuotaConfig{Rules: tc.rules}).validate(cmp); err == nil {
				t.Fatalf("expected validation error for %q", tc.name)
			}
		})
	}
}

func TestMakeStaticWriteQuotaConfigImmutable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rules := []WriteQuotaRule{quotaRule("a", "a", "z", 0, WriteQuotaReject)}
	fn := MakeStaticWriteQuotaConfig(DefaultComparer.Compare, rules...)
	cfg1 := fn()
	cfg2 := fn()
	if cfg1 != cfg2 {
		t.Fatalf("static config must return the same pointer")
	}
	// Mutating the caller's slice after construction must not affect the
	// frozen configuration.
	rules[0].Name = "mutated"
	if got := fn().Rules[0].Name; got != "a" {
		t.Fatalf("static config mutated by caller: %q", got)
	}
}

func TestMakeStaticWriteQuotaConfigPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on invalid rules")
		}
	}()
	MakeStaticWriteQuotaConfig(DefaultComparer.Compare, quotaRule("", "z", "a", 0, WriteQuotaReject))
}

func TestQuotaMatcherPointAndRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	wm := newTestWriteQuotaManager(t,
		quotaRule("low", "a", "m", 0, WriteQuotaReject),
		quotaRule("hi", "m", "z", 0, WriteQuotaWait),
	)
	qc := wm.current()
	if r := qc.pointRule([]byte("a")); r == nil || r.def.Name != "low" {
		t.Fatalf("a should map to low, got %v", r)
	}
	if r := qc.pointRule([]byte("m")); r == nil || r.def.Name != "hi" {
		t.Fatalf("m should map to hi (exclusive end), got %v", r)
	}
	if r := qc.pointRule([]byte("z")); r != nil {
		t.Fatalf("z is outside every rule, got %v", r.def.Name)
	}
	if r := qc.pointRule([]byte("0")); r != nil {
		t.Fatalf("0 is outside every rule, got %v", r.def.Name)
	}
	var got []string
	for _, r := range qc.rangeRules(nil, base.UserKeyBoundsEndExclusive([]byte("b"), []byte("n"))) {
		got = append(got, r.def.Name)
	}
	if len(got) != 2 {
		t.Fatalf("range [b,n) should overlap both rules, got %v", got)
	}
	got = got[:0]
	for _, r := range qc.rangeRules(nil, base.UserKeyBoundsEndExclusive([]byte("n"), []byte("z"))) {
		got = append(got, r.def.Name)
	}
	if len(got) != 1 || got[0] != "hi" {
		t.Fatalf("range [n,z) should overlap only hi, got %v", got)
	}
}

func TestQuotaMatcherOverlappingPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	wm := newTestWriteQuotaManager(t,
		quotaRule("base", "a", "z", 0, WriteQuotaReject),
		quotaRule("hot", "h", "l", 10, WriteQuotaWait),
	)
	qc := wm.current()
	if r := qc.pointRule([]byte("i")); r == nil || r.def.Name != "hot" {
		t.Fatalf("i should map to hot (higher priority), got %v", r)
	}
	if r := qc.pointRule([]byte("b")); r == nil || r.def.Name != "base" {
		t.Fatalf("b should map to base, got %v", r)
	}
}

func TestWriteQuotaOptionsStringExcludesClosure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	o := &Options{}
	o.EnsureDefaults()
	o.WriteQuotaConfig = MakeStaticWriteQuotaConfig(DefaultComparer.Compare,
		quotaRule("r", "a", "z", 0, WriteQuotaReject))
	if s := o.String(); strings.Contains(strings.ToLower(s), "write_quota") ||
		strings.Contains(strings.ToLower(s), "writequota") {
		t.Fatalf("closure option must not be serialized in OPTIONS text:\n%s", s)
	}
}

func TestWriteQuotaOptionsValidationOnOpen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opts := func() *Options { o := &Options{FS: vfs.NewMem()}; o.EnsureDefaults(); return o }()
	opts.WriteQuotaConfig = MakeStaticWriteQuotaConfig(DefaultComparer.Compare,
		quotaRule("r", "a", "z", 0, WriteQuotaReject))
	d, err := Open("", opts)
	if err != nil {
		t.Fatalf("open with valid quota config: %v", err)
	}
	if d.writeQuota == nil {
		t.Fatalf("writeQuota manager not constructed")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Invalid config must fail Open validation.
	bad := func() *Options { o := &Options{FS: vfs.NewMem()}; o.EnsureDefaults(); return o }()
	bad.WriteQuotaConfig = func() *WriteQuotaConfig {
		return &WriteQuotaConfig{Rules: []WriteQuotaRule{quotaRule("", "z", "a", 0, WriteQuotaReject)}}
	}
	if _, err := Open("", bad); err == nil {
		t.Fatalf("expected Open to reject invalid write quota config")
	}
}

func TestWriteQuotaDisabledByDefault(t *testing.T) {
	defer leaktest.AfterTest(t)()
	opts := func() *Options { o := &Options{FS: vfs.NewMem()}; o.EnsureDefaults(); return o }()
	d, err := Open("", opts)
	if err != nil {
		t.Fatal(err)
	}
	if d.writeQuota != nil {
		t.Fatalf("write quota must be disabled by default")
	}
	if err := d.Set([]byte("k"), []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

// newTestWriteQuotaManager constructs a manager with the given rules.
func newTestWriteQuotaManager(t *testing.T, rules ...WriteQuotaRule) *writeQuotaManager {
	t.Helper()
	m, err := newWriteQuotaManager(DefaultComparer.Compare, DefaultLogger,
		MakeStaticWriteQuotaConfig(DefaultComparer.Compare, rules...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.close() })
	return m
}
