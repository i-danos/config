// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// A02 phase 3, stage 3-G: sameConfig against the real acm YANG shipped in the
// image. DANOS_YANG_DIR, when set, names a directory holding the image's YANG
// files and takes precedence. When unset the tests fall back to the vendored
// copy in testdata/realyang (see its README for provenance), so a plain
// "go test ./..." -- including debian/rules dh_auto_test -- runs them.
// A missing fallback directory is a hard failure, never a skip.
//
// To run against a different set of YANG files:
//   DANOS_YANG_DIR=<dir with the image's yang files> go test ./schema/ -run RealAcmYang

package schema

import (
	"os"
	"path/filepath"
	"testing"
)

const (
	acmNS = "vyatta-system-acm-v1:"
	acmIn = `{"vyatta-system-v1:system":{"` + acmNS + `acm":`
)

// realYangDir returns DANOS_YANG_DIR if set, else the vendored testdata copy.
func realYangDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("DANOS_YANG_DIR"); dir != "" {
		return dir
	}
	dir := filepath.Join("testdata", "realyang")
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("DANOS_YANG_DIR not set and fallback %s is unusable: %v",
			dir, err)
	}
	return dir
}

func TestSameConfigRealAcmYang(t *testing.T) {
	dir := realYangDir(t)
	ms, err := getModelSet(t, dir)
	if ms == nil {
		t.Fatalf("cannot compile %s: %v", dir, err)
	}
	for name, body := range map[string]string{
		"rule without action":     `{"` + acmNS + `ruleset":{"rule":[{"tagnode":1,"path":"/a","operation":"read","group":["g"]}]}}`,
		"rule without group":      `{"` + acmNS + `ruleset":{"rule":[{"tagnode":1,"path":"/a","action":"deny","operation":"read"}]}}`,
		"rpc rule without action": `{"` + acmNS + `rpc-ruleset":{"rule":[{"rule-number":1,"rpc-name":"x","group":["g"]}]}}`,
		"notif rule w/o action":   `{"` + acmNS + `notification-ruleset":{"rule":[{"rule-number":3,"notification-name":"n","group":["g"]}]}}`,
		"empty presence ruleset":  `{"` + acmNS + `ruleset":{}}`,
		"full rule":               `{"` + acmNS + `ruleset":{"rule":[{"tagnode":1,"path":"/a","action":"deny","operation":"read","group":["g"]}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			doc := acmIn + body + `}}`
			same, err := sameConfig(ms, doc, doc)
			if err != nil || !same {
				t.Fatalf("sameConfig(doc, doc) = %v, %v", same, err)
			}
		})
	}
}

// The shape the 3-G getter used to emit for a rule without action: the empty
// string is not a valid value, so it cannot be normalised at all.
func TestSameConfigRealAcmYangRejectsEmptyAction(t *testing.T) {
	dir := realYangDir(t)
	ms, _ := getModelSet(t, dir)
	doc := acmIn + `{"` + acmNS + `ruleset":{"rule":[{"tagnode":1,"path":"/a","action":"","operation":"read"}]}}}}`
	if _, err := sameConfig(ms, doc, doc); err == nil {
		t.Fatal("an empty action must not normalise")
	}
}

// "operation" is mandatory without a default in the real YANG (unlike
// "action", which has default "deny"), so a rule without it is rejected by
// normalisation rather than silently accepted. That makes the omitempty on
// AcmV1Rule.Operation safe: configd enforces the member at commit time, so
// the getter never has to read back such a rule, and if it ever did the
// result would be a visible verify error, not a quiet mismatch.
func TestSameConfigRealAcmYangRejectsRuleWithoutOperation(t *testing.T) {
	dir := realYangDir(t)
	ms, _ := getModelSet(t, dir)
	doc := acmIn + `{"` + acmNS + `ruleset":{"rule":[{"tagnode":1,"path":"/a","action":"deny"}]}}}}`
	if _, err := sameConfig(ms, doc, doc); err == nil {
		t.Fatal("a rule without operation must be rejected")
	}
}
