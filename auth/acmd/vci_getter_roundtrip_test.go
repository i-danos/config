// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// A02 phase 3, stage 3-F (F6): what the getter returns must equal what configd
// sent, otherwise the reconcile read-back reports a permanent mismatch.

package acmd

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/danos/encoding/rfc7951"
)

func roundTrip(t *testing.T, sent string) (sentV, gotV interface{}) {
	t.Helper()
	var c AcmdConfig
	if err := rfc7951.Unmarshal([]byte(sent), &c); err != nil {
		t.Fatalf("unmarshal sent payload: %v", err)
	}
	got, err := rfc7951.Marshal(&c)
	if err != nil {
		t.Fatalf("marshal getter output: %v", err)
	}
	if err := json.Unmarshal([]byte(sent), &sentV); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got, &gotV); err != nil {
		t.Fatalf("getter output is not JSON: %v: %s", err, got)
	}
	return sentV, gotV
}

const (
	acmKey = "vyatta-system-v1:system"
	cfgNS  = "vyatta-system-acm-configd-v1:"
)

// wrap builds a payload the way configd delivers it: the changed leaves and
// containers below system/acm only.
func wrap(body string) string {
	return `{"` + acmKey + `":{"vyatta-system-acm-v1:acm":{` + body + `}}}`
}

// Each payload must come back from the getter exactly as it was sent: same
// keys, same values, nothing added and nothing dropped.
func TestGetterEqualsSentPayload(t *testing.T) {
	for name, sent := range map[string]string{
		"empty configuration":      `{}`,
		"only enable":              wrap(`"` + cfgNS + `enable":[null]`),
		"enable and log-requests":  wrap(`"` + cfgNS + `enable":[null],"` + cfgNS + `log-requests":[null]`),
		"some defaults":            wrap(`"` + cfgNS + `read-default":"allow","` + cfgNS + `delete-default":"deny"`),
		"all defaults":             wrap(`"` + cfgNS + `create-default":"deny","` + cfgNS + `read-default":"allow","` + cfgNS + `update-default":"deny","` + cfgNS + `delete-default":"deny","` + cfgNS + `rpc-default":"allow","` + cfgNS + `notification-default":"allow"`),
		"empty presence ruleset":   wrap(`"` + cfgNS + `enable":[null],"` + cfgNS + `ruleset":{}`),
		"empty rpc-ruleset":        wrap(`"` + cfgNS + `rpc-ruleset":{}`),
		"empty notif ruleset":      wrap(`"` + cfgNS + `notification-ruleset":{}`),
		"all three empty rulesets": wrap(`"` + cfgNS + `ruleset":{},"` + cfgNS + `rpc-ruleset":{},"` + cfgNS + `notification-ruleset":{}`),
		"populated ruleset": wrap(`"` + cfgNS + `ruleset":{"rule":[` +
			`{"tagnode":1,"path":"/a","action":"deny","operation":"read","group":["g1","g2"]},` +
			`{"tagnode":2,"path":"/b","action":"allow","operation":"*","log":[null]}]}`),
		"populated rpc and notification rulesets": wrap(`"` + cfgNS + `rpc-ruleset":{"rule":[` +
			`{"rule-number":1,"rpc-name":"x","module-name":"m","action":"deny","group":["g"]}]},"` +
			cfgNS + `notification-ruleset":{"rule":[{"rule-number":3,"notification-name":"n","module-name":"*","action":"allow"}]}`),
		"rule without action": wrap(`"` + cfgNS + `ruleset":{"rule":[` +
			`{"tagnode":1,"path":"/a","operation":"read","group":["g1"]}]}`),
		"rule without group": wrap(`"` + cfgNS + `ruleset":{"rule":[` +
			`{"tagnode":1,"path":"/a","action":"deny","operation":"read"}]}`),
		"rule without operation": wrap(`"` + cfgNS + `ruleset":{"rule":[` +
			`{"tagnode":1,"path":"/a","action":"deny"}]}`),
		"rpc and notification rules without action or group": wrap(`"` + cfgNS + `rpc-ruleset":{"rule":[` +
			`{"rule-number":1,"rpc-name":"x"}]},"` +
			cfgNS + `notification-ruleset":{"rule":[{"rule-number":3,"notification-name":"n"}]}`),
	} {
		t.Run(name, func(t *testing.T) {
			s, g := roundTrip(t, sent)
			if !reflect.DeepEqual(s, g) {
				t.Fatalf("getter output differs from the sent payload:\nsent: %v\n got: %v", s, g)
			}
		})
	}
}
