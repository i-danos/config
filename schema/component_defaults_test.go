// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package schema

import "testing"

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
