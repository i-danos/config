// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// A02 phase 3, stage 3-F (F5): the validation (Check) call to a component is
// bounded. A component whose handler is stopped must not hold the commit (and
// the commit lock) for as long as it stays stopped.

package schema

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// stuckCheckOps blocks CheckConfigForModel of one model until released.
type stuckCheckOps struct {
	*compOps
	model   string
	release chan struct{}
	once    sync.Once
}

func (o *stuckCheckOps) CheckConfigForModel(model string, obj interface{}) error {
	if model == o.model {
		<-o.release
	}
	return o.compOps.CheckConfigForModel(model, obj)
}

func (o *stuckCheckOps) unblock() { o.once.Do(func() { close(o.release) }) }

func TestComponentValidationIsBoundedWhenACheckNeverReplies(t *testing.T) {
	old := checkCallDeadline
	checkCallDeadline = 150 * time.Millisecond
	t.Cleanup(func() { checkCallDeadline = old })

	h := newCompHarness(t)
	stuck := &stuckCheckOps{compOps: h.ops, model: "net.vyatta.test.beta",
		release: make(chan struct{})}
	t.Cleanup(stuck.unblock)
	h.cm.OperationsManager = stuck

	type result struct{ errs []error }
	done := make(chan result, 1)
	go func() { done <- result{h.cm.ComponentValidation(h.ms, h.newDN, nil)} }()

	select {
	case r := <-done:
		if len(r.errs) != 1 {
			t.Fatalf("want exactly one error (beta timed out), got %d: %v", len(r.errs), r.errs)
		}
		msg := r.errs[0].Error()
		if !strings.Contains(msg, "beta") || !strings.Contains(msg, "did not answer") {
			t.Fatalf("error must name the component and say it did not answer: %q", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ComponentValidation is still waiting 3s after the Check deadline: the Check call is unbounded")
	}
}

func TestComponentValidationCheckWithinDeadlineIsUnchanged(t *testing.T) {
	old := checkCallDeadline
	checkCallDeadline = 5 * time.Second
	t.Cleanup(func() { checkCallDeadline = old })

	h := newCompHarness(t)
	if errs := h.cm.ComponentValidation(h.ms, h.newDN, nil); len(errs) != 0 {
		t.Fatalf("unexpected validation errors: %v", errs)
	}
}

// A02 phase 3, R2 item 1 (written decision): an abandoned Check is read-only
// and must not enter the pending table, otherwise it would block later Set
// dispatches to the same component.
func TestAbandonedCheckIsNotRegisteredAsPending(t *testing.T) {
	old := checkCallDeadline
	checkCallDeadline = 100 * time.Millisecond
	t.Cleanup(func() { checkCallDeadline = old })

	h := newCompHarness(t)
	stuck := &stuckCheckOps{compOps: h.ops, model: "net.vyatta.test.beta",
		release: make(chan struct{})}
	t.Cleanup(stuck.unblock)
	h.cm.OperationsManager = stuck

	if errs := h.cm.ComponentValidation(h.ms, h.newDN, nil); len(errs) != 1 {
		t.Fatalf("want one timeout error, got %v", errs)
	}
	if pc := h.cm.pending.get("net.vyatta.test.beta"); pc != nil {
		t.Fatal("an abandoned Check was registered in the pending table; it would block later Sets")
	}
}
