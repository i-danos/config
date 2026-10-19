// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// A02: component apply failure semantics.
//
// Tests named ...Today characterize the LEGACY wrappers (ComponentSetRunning,
// ComponentSetRunningWithLog), which are kept unchanged for existing callers:
// failure is invisible or reduced to a free-text output line and later
// components are still applied. Tests named ...Checked cover
// ComponentSetRunningChecked, the fail-closed entry point: every failure class
// is reported with the component identity and the loop stops at the first one.
// They use a scripted in-memory OperationsManager; no bus, no service.

package schema

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/danos/godbus/dbus"
	"github.com/danos/mgmterror"
	"github.com/danos/utils/exec"
	"github.com/danos/vci/conf"
	"github.com/danos/yang/data/encoding"
	"github.com/danos/yang/testutils"
)

const (
	compAlpha = "net.vyatta.test.alpha"
	compBeta  = "net.vyatta.test.beta"
	compGamma = "net.vyatta.test.gamma"
)

// scriptedOps wraps the in-memory test ops manager and lets a test script the
// result of SetConfigForModel per component.
type scriptedOps struct {
	*testOpsMgr
	dialErr error
	results map[string]error // nil / missing entry = success
	calls   []string         // components SetConfigForModel was called for, in order
}

func (s *scriptedOps) Dial() error { return s.dialErr }

func (s *scriptedOps) SetConfigForModel(model string, obj interface{}) error {
	s.calls = append(s.calls, model)
	if err := s.results[model]; err != nil {
		return err // component rejected / unreachable: nothing committed
	}
	return s.testOpsMgr.SetConfigForModel(model, obj)
}

type setRunningHarness struct {
	t     *testing.T
	ops   *scriptedOps
	cm    *compMgr
	ms    *modelSet
	input []byte
}

func newSetRunningHarness(t *testing.T) *setRunningHarness {
	snippet := func(cont string) string {
		return "container " + cont + " { leaf l { type string; } }"
	}
	schemas := []*testutils.TestSchema{
		testutils.NewTestSchema("vyatta-test-alpha-v1", "alpha").AddSchemaSnippet(snippet("alphaCont")),
		testutils.NewTestSchema("vyatta-test-beta-v1", "beta").AddSchemaSnippet(snippet("betaCont")),
		testutils.NewTestSchema("vyatta-test-gamma-v1", "gamma").AddSchemaSnippet(snippet("gammaCont")),
	}
	dir := createYangDir(t, "a02", schemas)
	t.Cleanup(func() { os.RemoveAll(dir) })

	comps := []string{
		conf.CreateTestDotComponentFile("alpha").SetBefore("beta").AddBaseModel().String(),
		conf.CreateTestDotComponentFile("beta").SetBefore("gamma").AddBaseModel().String(),
		conf.CreateTestDotComponentFile("gamma").AddBaseModel().String(),
	}
	ms, _ := getModelSet(t, dir)
	mappings, _ := CreateComponentNSMappings(ms, testModelSet, getComponentConfigs(t, comps...))

	params := &testCompParams{
		t:               t,
		validatedConfig: map[string]string{},
		committedConfig: map[string]string{},
		currentState:    map[string]string{},
	}
	ops := &scriptedOps{testOpsMgr: newTestOpsMgr(params), results: map[string]error{}}
	return &setRunningHarness{
		t: t, ops: ops, ms: ms,
		cm:    NewCompMgr(ops, newTestSvcMgr(params), ms, mappings),
		input: []byte(`{"alphaCont":{"l":"a"},"betaCont":{"l":"b"},"gammaCont":{"l":"c"}}`),
	}
}

// run applies the config to every component and returns what the caller of
// ComponentSetRunning gets back: only a slice of text outputs.
func (h *setRunningHarness) run() []*exec.Output {
	dn, err := encoding.UnmarshalJSON(h.ms, h.input)
	if err != nil {
		h.t.Fatalf("encode: %s", err)
	}
	return h.cm.ComponentSetRunning(h.ms, dn, nil)
}

func (h *setRunningHarness) committed(model string) bool {
	_, ok := h.ops.tcmParams.committedConfig[model]
	return ok
}

func (h *setRunningHarness) wantCalls(want ...string) {
	h.t.Helper()
	if !reflect.DeepEqual(h.ops.calls, want) {
		h.t.Fatalf("components called: got %v, want %v", h.ops.calls, want)
	}
}

func (h *setRunningHarness) wantCommitted(want map[string]bool) {
	h.t.Helper()
	for model, exp := range want {
		if got := h.committed(model); got != exp {
			h.t.Errorf("%s committed: got %v, want %v", model, got, exp)
		}
	}
}

func TestSetRunningAllComponentsSucceed(t *testing.T) {
	h := newSetRunningHarness(t)
	outs := h.run()
	h.wantCalls(compAlpha, compBeta, compGamma)
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: true, compGamma: true})
	if len(outs) != 0 {
		t.Fatalf("success must report nothing, got %v", outs)
	}
}

// Ordinary error: today it is only printed to stdout. The caller receives an
// EMPTY result -- indistinguishable from success -- and later components are
// still applied.
func TestSetRunningOrdinaryComponentErrorIsInvisibleToCallerToday(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compBeta] = errors.New("beta: cannot apply config")
	outs := h.run()
	h.wantCalls(compAlpha, compBeta, compGamma) // does not stop at the failure
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: false, compGamma: true})
	if len(outs) != 0 {
		t.Fatalf("TODAY: an ordinary error yields no output at all; got %v", outs)
	}
}

// A DBus error (value type) is the one case that surfaces, but only as an
// Output line with an empty path and free text: no status, no component name,
// no severity. The caller cannot tell it from an informational message.
func TestSetRunningDBusErrorBecomesBareTextOutputToday(t *testing.T) {
	h := newSetRunningHarness(t)
	dbusErr := dbus.Error{Name: "org.freedesktop.DBus.Error.ServiceUnknown",
		Body: []interface{}{"The name is not activatable"}}
	h.ops.results[compBeta] = dbusErr
	outs := h.run()
	h.wantCalls(compAlpha, compBeta, compGamma)
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: false, compGamma: true})
	if len(outs) != 1 {
		t.Fatalf("TODAY: one text output expected, got %d: %v", len(outs), outs)
	}
	if !reflect.DeepEqual(outs[0].Path, []string{""}) {
		t.Errorf("TODAY: output carries no path/component, got %v", outs[0].Path)
	}
	if outs[0].Output == "" {
		t.Errorf("expected the DBus error text in the output")
	}
}

// The same DBus error as a POINTER is not matched by the value-type assertion
// and disappears like an ordinary error.
func TestSetRunningPointerDBusErrorIsInvisibleToday(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compBeta] = dbus.NewError("org.freedesktop.DBus.Error.NoReply", nil)
	outs := h.run()
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: false, compGamma: true})
	if len(outs) != 0 {
		t.Fatalf("TODAY: a *dbus.Error is dropped; got %v", outs)
	}
}

// Timeouts: neither a context deadline nor a DBus NoReply (value) is treated
// as anything but the cases above. A timeout may mean the component DID apply
// the config; nothing records that either way.
func TestSetRunningTimeoutIsNotDistinguishedToday(t *testing.T) {
	for name, err := range map[string]error{
		"context deadline": context.DeadlineExceeded,
		"os deadline":      os.ErrDeadlineExceeded,
	} {
		h := newSetRunningHarness(t)
		h.ops.results[compBeta] = err
		outs := h.run()
		h.wantCalls(compAlpha, compBeta, compGamma)
		if len(outs) != 0 {
			t.Errorf("%s: TODAY a timeout error is dropped; got %v", name, outs)
		}
	}
	h := newSetRunningHarness(t)
	h.ops.results[compBeta] = dbus.Error{Name: "org.freedesktop.DBus.Error.NoReply",
		Body: []interface{}{"Message did not receive a reply (timeout)"}}
	outs := h.run()
	if len(outs) != 1 || outs[0].Output == "" {
		t.Errorf("TODAY: a DBus NoReply surfaces only as bare text; got %v", outs)
	}
}

// Partial success: the first component applied, the second failed, the third
// applied. The result carries nothing that says the set is now inconsistent.
func TestSetRunningPartialSuccessLeavesMixedStateWithoutSignalToday(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compAlpha] = nil
	h.ops.results[compBeta] = errors.New("beta down")
	outs := h.run()
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: false, compGamma: true})
	if len(outs) != 0 {
		t.Fatalf("TODAY: mixed state is not reported; got %v", outs)
	}
}

// First component failing does not stop the rest, and the last failing does not
// roll back the earlier ones.
func TestSetRunningNoRollbackAndNoShortCircuitToday(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compAlpha] = errors.New("alpha down")
	h.run()
	h.wantCalls(compAlpha, compBeta, compGamma)
	h.wantCommitted(map[string]bool{compAlpha: false, compBeta: true, compGamma: true})

	h = newSetRunningHarness(t)
	h.ops.results[compGamma] = errors.New("gamma down")
	h.run()
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: true, compGamma: false})
}

// Dial failure is the only path that stops early: nothing is applied, and it
// is reported as one text output.
func TestSetRunningDialFailureStopsEverythingAndIsReportedToday(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.dialErr = errors.New("bus unavailable")
	outs := h.run()
	h.wantCalls() // nothing was attempted
	if len(outs) != 1 || outs[0].Output != "bus unavailable" {
		t.Fatalf("TODAY: dial failure is a single text output; got %v", outs)
	}
}

// ---- fail-closed entry point ------------------------------------------------

func (h *setRunningHarness) runChecked() ([]*exec.Output, []error) {
	dn, err := encoding.UnmarshalJSON(h.ms, h.input)
	if err != nil {
		h.t.Fatalf("encode: %s", err)
	}
	return h.cm.ComponentSetRunningChecked(h.ms, dn, nil, nil)
}

type timeoutNetErr struct{}

func (timeoutNetErr) Error() string   { return "i/o timeout" }
func (timeoutNetErr) Timeout() bool   { return true }
func (timeoutNetErr) Temporary() bool { return true }

func dbusErr(name, text string) dbus.Error {
	return dbus.Error{Name: name, Body: []interface{}{text}}
}

func TestSetRunningCheckedSuccessReturnsNoErrors(t *testing.T) {
	h := newSetRunningHarness(t)
	outs, errs := h.runChecked()
	if len(errs) != 0 || len(outs) != 0 {
		t.Fatalf("success: outs=%v errs=%v", outs, errs)
	}
	h.wantCalls(compAlpha, compBeta, compGamma)
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: true, compGamma: true})
}

// Every failure class: one error naming the failing component, later
// components not attempted, and the unknown-outcome wording only for timeouts.
func TestSetRunningCheckedReportsEveryFailureClass(t *testing.T) {
	const definite = "not known to have applied it"
	const unknown = "OUTCOME UNKNOWN"
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"ordinary", errors.New("beta: cannot apply"), definite},
		{"dbus value", dbusErr("org.freedesktop.DBus.Error.ServiceUnknown", "not activatable"), definite},
		{"dbus pointer", dbus.NewError("org.freedesktop.DBus.Error.ServiceUnknown", []interface{}{"gone"}), definite},
		{"dbus value wrapped", fmt.Errorf("call failed: %w", dbusErr("org.freedesktop.DBus.Error.Failed", "x")), definite},
		{"dbus pointer wrapped", fmt.Errorf("call failed: %w", dbus.NewError("org.freedesktop.DBus.Error.Failed", []interface{}{"x"})), definite},
		{"context deadline", context.DeadlineExceeded, unknown},
		{"os deadline", os.ErrDeadlineExceeded, unknown},
		{"net timeout", timeoutNetErr{}, unknown},
		{"wrapped deadline", fmt.Errorf("apply: %w", context.DeadlineExceeded), unknown},
		{"dbus NoReply value", dbusErr("org.freedesktop.DBus.Error.NoReply", "no reply"), unknown},
		{"dbus NoReply pointer", dbus.NewError("org.freedesktop.DBus.Error.NoReply", []interface{}{"no reply"}), unknown},
		{"dbus NoReply wrapped", fmt.Errorf("w: %w", dbusErr("org.freedesktop.DBus.Error.NoReply", "r")), unknown},
		{"dbus Timeout", dbusErr("org.freedesktop.DBus.Error.Timeout", "t"), unknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newSetRunningHarness(t)
			h.ops.results[compBeta] = c.err
			_, errs := h.runChecked()
			if len(errs) != 1 {
				t.Fatalf("want exactly one error, got %d: %v", len(errs), errs)
			}
			msg := errs[0].Error()
			for _, need := range []string{compBeta, c.want,
				"already applied and NOT rolled back: " + compAlpha,
				"not attempted: " + compGamma} {
				if !strings.Contains(msg, need) {
					t.Errorf("error lacks %q:\n%s", need, msg)
				}
			}
			other := unknown
			if c.want == unknown {
				other = definite
			}
			if strings.Contains(msg, other) {
				t.Errorf("error wrongly says %q:\n%s", other, msg)
			}
			h.wantCalls(compAlpha, compBeta) // stopped: gamma never attempted
			h.wantCommitted(map[string]bool{compAlpha: true, compBeta: false, compGamma: false})
		})
	}
}

func TestSetRunningCheckedFirstComponentFailureAppliesNothing(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compAlpha] = errors.New("alpha down")
	_, errs := h.runChecked()
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), compAlpha) {
		t.Fatalf("errs=%v", errs)
	}
	if strings.Contains(errs[0].Error(), "already applied") {
		t.Errorf("nothing was applied before the first failure: %s", errs[0])
	}
	h.wantCalls(compAlpha)
	h.wantCommitted(map[string]bool{compAlpha: false, compBeta: false, compGamma: false})
}

func TestSetRunningCheckedLastComponentFailureStillReported(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compGamma] = errors.New("gamma down")
	_, errs := h.runChecked()
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), compGamma) {
		t.Fatalf("errs=%v", errs)
	}
	if strings.Contains(errs[0].Error(), "not attempted") {
		t.Errorf("last component: nothing left to skip: %s", errs[0])
	}
	h.wantCommitted(map[string]bool{compAlpha: true, compBeta: true, compGamma: false})
}

func TestSetRunningCheckedDialFailureIsAnErrorAndNothingChanges(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.dialErr = errors.New("bus unavailable")
	_, errs := h.runChecked()
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "bus unavailable") ||
		!strings.Contains(errs[0].Error(), "no component was changed") {
		t.Fatalf("errs=%v", errs)
	}
	h.wantCalls()
}

func TestSetRunningCheckedErrorIsAManagementError(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compBeta] = errors.New("beta down")
	_, errs := h.runChecked()
	if _, ok := errs[0].(*mgmterror.OperationFailedApplicationError); !ok {
		t.Fatalf("want a mgmterror application error for the API, got %T", errs[0])
	}
}

// The legacy wrappers are unchanged.
func TestSetRunningLegacyWrappersStillBestEffort(t *testing.T) {
	h := newSetRunningHarness(t)
	h.ops.results[compBeta] = errors.New("beta down")
	dn, _ := encoding.UnmarshalJSON(h.ms, h.input)
	h.cm.ComponentSetRunningWithLog(h.ms, dn, nil, nil)
	h.wantCalls(compAlpha, compBeta, compGamma)
}
