// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// A02 phase 3, stage 3-F (F4): sameConfig must not depend on the order in
// which lists, leaf-lists or containers are spelled or enumerated.

package schema

import (
	"os"
	"testing"

	"github.com/danos/yang/testutils"
)

const sameConfigSnippet = `container c {
	leaf-list ordered { type string; ordered-by user; }
	list oitems {
		ordered-by user;
		key name;
		leaf name { type string; }
		leaf v { type string; }
		leaf-list otags { type string; ordered-by user; }
	}
	choice ch {
		case one {
			leaf-list inchoice { type string; ordered-by user; }
		}
		case two {
			leaf-list inchoice2 { type string; }
		}
	}
	list items {
		key name;
		leaf name { type string; }
		leaf v { type string; }
		leaf-list tags { type string; }
	}
	leaf-list top { type string; }
	leaf a { type string; }
	leaf b { type string; }
}`

func newSameConfigModelSet(t *testing.T) ModelSet {
	t.Helper()
	dir := createYangDir(t, "samecfg", []*testutils.TestSchema{
		testutils.NewTestSchema("vyatta-test-samecfg-v1", "samecfg").AddSchemaSnippet(sameConfigSnippet),
	})
	t.Cleanup(func() { os.RemoveAll(dir) })
	ms, _ := getModelSet(t, dir)
	return ms
}

func TestSameConfigIsOrderIndependent(t *testing.T) {
	ms := newSameConfigModelSet(t)
	a := `{"c":{"a":"1","b":"2","top":["x","y","z"],"items":[` +
		`{"name":"n1","v":"1","tags":["p","q","r"]},` +
		`{"name":"n2","v":"2","tags":["s","t"]},` +
		`{"name":"n3","v":"3"}]}}`
	b := `{"c":{"b":"2","a":"1","items":[` +
		`{"name":"n3","v":"3"},` +
		`{"tags":["t","s"],"v":"2","name":"n2"},` +
		`{"v":"1","name":"n1","tags":["r","p","q"]}],"top":["z","x","y"]}}`
	for i := 0; i < 200; i++ {
		same, err := sameConfig(ms, a, b)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if !same {
			t.Fatalf("run %d: documents that differ only in ordering compare unequal", i)
		}
	}
}

func TestSameConfigStillDetectsRealDifferences(t *testing.T) {
	ms := newSameConfigModelSet(t)
	base := `{"c":{"top":["x","y"],"items":[{"name":"n1","v":"1","tags":["p","q"]},{"name":"n2","v":"2"}]}}`
	for name, other := range map[string]string{
		"leaf value":      `{"c":{"top":["x","y"],"items":[{"name":"n1","v":"9","tags":["p","q"]},{"name":"n2","v":"2"}]}}`,
		"missing entry":   `{"c":{"top":["x","y"],"items":[{"name":"n1","v":"1","tags":["p","q"]}]}}`,
		"extra leaf-list": `{"c":{"top":["x","y","w"],"items":[{"name":"n1","v":"1","tags":["p","q"]},{"name":"n2","v":"2"}]}}`,
		"inner leaf-list": `{"c":{"top":["x","y"],"items":[{"name":"n1","v":"1","tags":["p"]},{"name":"n2","v":"2"}]}}`,
		"extra leaf":      `{"c":{"a":"1","top":["x","y"],"items":[{"name":"n1","v":"1","tags":["p","q"]},{"name":"n2","v":"2"}]}}`,
	} {
		for i := 0; i < 20; i++ {
			same, err := sameConfig(ms, base, other)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if same {
				t.Fatalf("%s: a real difference compares equal", name)
			}
		}
	}
}

// Equal entries are not collapsed: a leaf-list that repeats a value is not the
// same configuration as one that holds it once.
func TestSameConfigDoesNotCollapseDuplicateEntries(t *testing.T) {
	ms := newSameConfigModelSet(t)
	once := `{"c":{"top":["x","y"]}}`
	twice := `{"c":{"top":["x","x","y"]}}`
	for i := 0; i < 20; i++ {
		same, err := sameConfig(ms, once, twice)
		if err != nil {
			t.Fatal(err)
		}
		if same {
			t.Fatalf("run %d: a duplicated entry compares equal to a single one", i)
		}
	}
}

// ordered-by user is part of the configuration: the same entries in another
// order are a different configuration and must not read back as "matched".
func TestSameConfigIsOrderSensitiveForUserOrderedLists(t *testing.T) {
	ms := newSameConfigModelSet(t)
	for name, pair := range map[string][2]string{
		"leaf-list": {`{"c":{"ordered":["x","y","z"]}}`, `{"c":{"ordered":["y","x","z"]}}`},
		"list": {`{"c":{"oitems":[{"name":"a","v":"1"},{"name":"b","v":"2"}]}}`,
			`{"c":{"oitems":[{"name":"b","v":"2"},{"name":"a","v":"1"}]}}`},
		"leaf-list inside a user-ordered list entry": {
			`{"c":{"oitems":[{"name":"a","otags":["p","q"]}]}}`,
			`{"c":{"oitems":[{"name":"a","otags":["q","p"]}]}}`},
		"leaf-list inside a choice case": {`{"c":{"inchoice":["x","y"]}}`, `{"c":{"inchoice":["y","x"]}}`},
	} {
		for i := 0; i < 50; i++ {
			same, err := sameConfig(ms, pair[0], pair[1])
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if same {
				t.Fatalf("%s: a different order of a user-ordered list compares equal", name)
			}
		}
	}
}

func TestSameConfigUserOrderedListsStillEqualWhenOrderIsTheSame(t *testing.T) {
	ms := newSameConfigModelSet(t)
	a := `{"c":{"ordered":["x","y","z"],"inchoice":["p","q"],"oitems":[{"name":"b","v":"2","otags":["t","s"]},{"name":"a","v":"1"}]}}`
	b := `{"c":{"oitems":[{"otags":["t","s"],"v":"2","name":"b"},{"v":"1","name":"a"}],"inchoice":["p","q"],"ordered":["x","y","z"]}}`
	for i := 0; i < 200; i++ {
		same, err := sameConfig(ms, a, b)
		if err != nil {
			t.Fatal(err)
		}
		if !same {
			t.Fatalf("run %d: identical user-ordered content compares unequal", i)
		}
	}
}

// The system-ordered siblings of a user-ordered list keep comparing by content.
func TestSameConfigSystemOrderedStaysOrderIndependentNextToUserOrdered(t *testing.T) {
	ms := newSameConfigModelSet(t)
	a := `{"c":{"top":["x","y"],"inchoice2":["m","n"],"ordered":["1","2"]}}`
	b := `{"c":{"ordered":["1","2"],"inchoice2":["n","m"],"top":["y","x"]}}`
	for i := 0; i < 200; i++ {
		same, err := sameConfig(ms, a, b)
		if err != nil {
			t.Fatal(err)
		}
		if !same {
			t.Fatalf("run %d: system-ordered lists differ only in order but compare unequal", i)
		}
	}
}
