// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package schema

import (
	"testing"

	"github.com/danos/yang/data/datanode"
	"github.com/danos/yang/data/encoding"
)

// R14: reconcile sends a component a document built from a union tree, which
// materialises every YANG default. A component that has never been configured
// answers its own read-back with an empty document (vrrp: "{}"). Comparing a
// payload carrying defaults against "{}" never matches, so the component stays
// in the journal's dirty set forever and "the journal ends clean" can never be
// true.
//
// The fix is on the comparison, not on what is sent: components read the tree
// they are given, and several reject a document that lacks their root
// containers. The earlier attempt to stop sending defaults made vrrp fail with
// "Error: 'vyatta-interfaces-v1:interfaces'" and kdump with "Error: No kernel
// crash dump configuration." -- one stuck component became two failures.
//
// sameConfigAllowDefaulted(received, full, stripped, got) is handed three views
// of the same configuration: what was sent, the same document without schema
// defaults, and what the component reported. These tests pin its edges using
// JSON only, because the interesting cases are all about which subtrees count
// as "defaulted" and which do not.

func TestAllowDefaultedEmptyReadBackMatchesDefaultsOnly(t *testing.T) {
	full := `{"a:interfaces":{"dp0s3":{"vrrp":{"start-delay":0}}}}`
	stripped := `{}`
	got := `{}`
	same, err := pruneAndCompare(full, stripped, got)
	if err != nil {
		t.Fatal(err)
	}
	if !same {
		t.Fatal("a never-configured component that reports {} must compare equal " +
			"to a payload whose entire content is schema defaults")
	}
}

func TestAllowDefaultedEchoedDefaultsMatch(t *testing.T) {
	full := `{"a:interfaces":{"dp0s3":{"vrrp":{"start-delay":0}}}}`
	stripped := `{}`
	got := `{"a:interfaces":{"dp0s3":{"vrrp":{"start-delay":0}}}}`
	same, err := pruneAndCompare(full, stripped, got)
	if err != nil {
		t.Fatal(err)
	}
	if !same {
		t.Fatal("a component echoing the defaulted payload must compare equal")
	}
}

func TestAllowDefaultedConfiguredValueStillCompared(t *testing.T) {
	// start-delay is a default in the sent document, but the component reports
	// a different value: that is real configuration and must not be masked.
	full := `{"a:interfaces":{"dp0s3":{"vrrp":{"start-delay":0}}}}`
	stripped := `{}`
	got := `{"a:interfaces":{"dp0s3":{"vrrp":{"start-delay":30}}}}`
	same, err := pruneAndCompare(full, stripped, got)
	if err != nil {
		t.Fatal(err)
	}
	if same {
		t.Fatal("a defaulted leaf holding a different value must still fail the comparison")
	}
}

func TestAllowDefaultedNonDefaultContentMismatch(t *testing.T) {
	// "enabled" is not defaulted on the sent side, so it must be present in the
	// read-back. An empty read-back here is a genuine mismatch.
	full := `{"a:interfaces":{"dp0s3":{"vrrp":{"enabled":true}}}}`
	stripped := `{"a:interfaces":{"dp0s3":{"vrrp":{"enabled":true}}}}`
	got := `{}`
	same, err := pruneAndCompare(full, stripped, got)
	if err != nil {
		t.Fatal(err)
	}
	if same {
		t.Fatal("configuration that is not a default must be reported back")
	}
}

func TestAllowDefaultedNoDefaultsIsPlainEquality(t *testing.T) {
	full := `{"a:system":{"host-name":"r1"}}`
	stripped := full
	if same, err := pruneAndCompare(full, stripped, full); err != nil || !same {
		t.Fatalf("identical documents must match (same=%v err=%v)", same, err)
	}
	if same, err := pruneAndCompare(full, stripped, `{"a:system":{"host-name":"r2"}}`); err != nil || same {
		t.Fatalf("different host-name must not match (same=%v err=%v)", same, err)
	}
}

func TestAllowDefaultedExtraReadBackContentMismatches(t *testing.T) {
	// The component reports something that was never sent: that must fail even
	// when everything else is defaulted.
	full := `{"a:interfaces":{"dp0s3":{"vrrp":{"start-delay":0}}}}`
	stripped := `{}`
	got := `{"a:interfaces":{"dp0s3":{"vrrp":{"start-delay":0,"enabled":true}}}}`
	same, err := pruneAndCompare(full, stripped, got)
	if err != nil {
		t.Fatal(err)
	}
	if same {
		t.Fatal("content that was never sent must not be ignored")
	}
}

// ---- defaults inside list entries -----------------------------------------
//
// pruneDefaulted used to walk JSON objects only and returned at the first
// array, so a default leaf inside a list entry (acm "rule" has "action"
// default "deny") was never removed from the read-back. A component that
// echoed exactly what it was sent then compared unequal to the defaults-free
// document and stayed unverified (r4 on hardware: 74/81, acmd and provisiond).
//
// The first five tests use JSON only (pruneAndCompare compares arrays
// positionally); the sixth needs a model set because the order of a
// system-ordered list is the schema's business.

const (
	listFull  = `{"a:rules":{"rule":[{"id":1,"path":"/a","action":"deny"}]}}`
	listStrip = `{"a:rules":{"rule":[{"id":1,"path":"/a"}]}}`
)

func mustCompare(t *testing.T, full, stripped, got string) bool {
	t.Helper()
	same, err := pruneAndCompare(full, stripped, got)
	if err != nil {
		t.Fatal(err)
	}
	return same
}

func TestAllowDefaultedListEntryEchoedDefaultMatches(t *testing.T) {
	if !mustCompare(t, listFull, listStrip, listFull) {
		t.Fatal("a component echoing a list entry with its defaulted leaf must compare equal")
	}
}

func TestAllowDefaultedListEntryOmittedDefaultMatches(t *testing.T) {
	if !mustCompare(t, listFull, listStrip, listStrip) {
		t.Fatal("a component reporting the list entry without its defaulted leaf must compare equal")
	}
}

func TestAllowDefaultedListEntryOverriddenDefaultMismatches(t *testing.T) {
	got := `{"a:rules":{"rule":[{"id":1,"path":"/a","action":"permit"}]}}`
	if mustCompare(t, listFull, listStrip, got) {
		t.Fatal("a defaulted leaf the component reports with another value is real " +
			"configuration and must not be masked")
	}
}

func TestAllowDefaultedListEntryNonDefaultChangedMismatches(t *testing.T) {
	got := `{"a:rules":{"rule":[{"id":1,"path":"/b","action":"deny"}]}}`
	if mustCompare(t, listFull, listStrip, got) {
		t.Fatal("a changed non-default leaf in a list entry must still fail the comparison")
	}
}

func TestAllowDefaultedListEntryRemovedMismatches(t *testing.T) {
	full := `{"a:rules":{"rule":[{"id":1,"path":"/a","action":"deny"},{"id":2,"path":"/b","action":"permit"}]}}`
	strip := `{"a:rules":{"rule":[{"id":1,"path":"/a"},{"id":2,"path":"/b","action":"permit"}]}}`
	got := `{"a:rules":{"rule":[{"id":2,"path":"/b","action":"permit"}]}}`
	if mustCompare(t, full, strip, got) {
		t.Fatal("a list entry the component no longer has must still fail the comparison")
	}
	// and the extra-entry direction
	if mustCompare(t, listFull, listStrip,
		`{"a:rules":{"rule":[{"id":1,"path":"/a","action":"deny"},{"id":9,"path":"/z"}]}}`) {
		t.Fatal("an entry that was never sent must still fail the comparison")
	}
}

// Through ComponentReconcile with a real model set: the full document sorts
// [id1 deny, id2 permit] but the stripped one sorts [id2 permit, id1] because a
// system-ordered list is ordered by each entry's own spelling. Pairing by
// position would compare the wrong entries; pairing by content and normalising
// again afterwards must report a match.
func TestAllowDefaultedListEntryOrderDiffersBetweenFullAndStripped(t *testing.T) {
	const alpha = `container alphaCont {
		leaf l { type string; }
		list rule { key id; leaf id { type uint32; } leaf path { type string; }
			leaf action { type string; default "deny"; } }
	}`
	const pfx = `{"vyatta-test-alpha-v1:alphaCont":{"l":"v","rule":`
	full := pfx + `[{"id":1,"path":"/a","action":"deny"},{"id":2,"path":"/b","action":"permit"}]}}`
	strip := pfx + `[{"id":1,"path":"/a"},{"id":2,"path":"/b","action":"permit"}]}}`

	run := func(t *testing.T, got string) Verification {
		h := newCompHarnessWithAlpha(t, alpha)
		tgt, err := encoding.UnmarshalJSON(h.ms, []byte(full))
		if err != nil {
			t.Fatal(err)
		}
		cmp, err := encoding.UnmarshalJSON(h.ms, []byte(strip))
		if err != nil {
			t.Fatal(err)
		}
		h.ops.getter[compAlpha] = got
		rep, errs := h.cm.ComponentReconcile(h.ms,
			func() datanode.DataNode { return tgt }, func() datanode.DataNode { return cmp },
			[]string{compAlpha}, h.pol, h.rec, nil)
		if len(errs) != 0 {
			t.Fatal(errs)
		}
		return h.result(rep, compAlpha).Verified
	}
	if v := run(t, full); v != VerifyMatched {
		t.Fatalf("echo of the full document: verified=%v, want matched", v)
	}
	if v := run(t, strip); v != VerifyMatched {
		t.Fatalf("report without the default: verified=%v, want matched", v)
	}
	changed := pfx + `[{"id":1,"path":"/a","action":"permit"},{"id":2,"path":"/b","action":"permit"}]}}`
	if v := run(t, changed); v != VerifyMismatch {
		t.Fatalf("a defaulted leaf changed to a real value: verified=%v, want mismatch", v)
	}
}
