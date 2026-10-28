// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package union

import (
	"testing"
)


// R14: FilterSetTree (schema/namespace_map.go) walks whatever is handed to it
// as a datanode.DataNode and sends the result to a component on reconcile. A
// plain union.Node always materialises YANG defaults -- that is what lets
// "show configuration all" display them -- so a component that was never
// configured still received a payload containing its defaulted leaves. The
// component itself, having nothing to report, answers its own read-back with
// an empty document, the two are compared and never match, and the component
// is marked unverified in the commit journal forever (verify/R14-vrrp-...md).
//
// NewDataNodeWithoutDefaults is the fix: a datanode.DataNode view of a
// union.Node that omits anything whose value is a default rather than
// something configured. These tests pin down its two load-bearing properties.

func TestNewDataNodeWithoutDefaultsOmitsDefaultLeaves(t *testing.T) {
	// getInitialTree(t) is the baseSchema tree with NOTHING configured: every
	// leaf in it holds only its YANG default. A real union.Node still reports
	// them as children -- that is the bug this test pins down as fixed on the
	// wrapper, and confirms still reproduces on the bare node (so the test
	// would fail loudly if the fixture ever stopped being all-default).
	root := getInitialTree(t)

	if got := len(root.YangDataChildren()); got == 0 {
		t.Fatalf("fixture sanity check failed: a bare union.Node over an " +
			"all-default tree reported 0 children; the fixture no longer " +
			"reproduces the bug this test is pinned to")
	}

	wrapped := NewDataNodeWithoutDefaults(root)
	if got := len(wrapped.YangDataChildren()); got != 0 {
		t.Fatalf("NewDataNodeWithoutDefaults(root).YangDataChildren() = %d children, want 0 "+
			"(every child here is a YANG default)", got)
	}
	if got := len(wrapped.YangDataChildrenNoSorting()); got != 0 {
		t.Fatalf("NewDataNodeWithoutDefaults(root).YangDataChildrenNoSorting() = %d children, want 0",
			got)
	}
}

func TestNewDataNodeWithoutDefaultsKeepsConfiguredLeaves(t *testing.T) {
	// Give "nondefault" a real value. It must still appear -- the wrapper is
	// not allowed to hide anything that was actually configured, only what
	// exists solely because of a "default" statement.
	root := getInitialTree(t)
	if err := root.Set(nil, []string{"nondefault", "explicit value"}); err != nil {
		t.Fatalf("setting nondefault failed: %v", err)
	}

	wrapped := NewDataNodeWithoutDefaults(root)
	var sawNondefault bool
	for _, ch := range wrapped.YangDataChildren() {
		if ch.YangDataName() == "nondefault" {
			sawNondefault = true
		}
		if ch.YangDataName() == "default" || ch.YangDataName() == "default2" {
			t.Fatalf("still-default leaf %q leaked through the wrapper even though "+
				"an unrelated leaf was configured", ch.YangDataName())
		}
	}
	if !sawNondefault {
		t.Fatal("the explicitly configured leaf 'nondefault' did not appear at all")
	}
}

func TestNewDataNodeWithoutDefaultsRecursesIntoDefaultedContainers(t *testing.T) {
	// "nonpresence" is a non-presence container that structurally exists (it
	// has no presence statement) purely to hold defaulted leaves. Pruning
	// must happen at every depth, not just the top level -- R14's actual
	// payload hid the default three levels down, inside a list entry inside a
	// non-presence container.
	root := getInitialTree(t)
	wrapped := NewDataNodeWithoutDefaults(root)

	for _, ch := range wrapped.YangDataChildren() {
		if ch.YangDataName() == "nonpresence" {
			t.Fatalf("'nonpresence' survived the wrapper with no configured descendant; "+
				"it has %d filtered children, want 0 (it is entirely defaults)",
				len(ch.YangDataChildren()))
		}
	}
}
