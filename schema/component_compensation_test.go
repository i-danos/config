// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// A02 phase 3, stage 3-A tests: compensation of a failed multi-component
// commit, the dispatch recorder contract, bounded calls, unknown outcomes,
// reconcile with read-back, and the component prerequisites. A scripted
// in-memory OperationsManager and a fake DispatchRecorder are used; no bus,
// no service, no real waiting beyond a few milliseconds.

package schema

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danos/godbus/dbus"
	"github.com/danos/mgmterror"
	"github.com/danos/vci/conf"
	"github.com/danos/yang/data/datanode"
	"github.com/danos/yang/data/encoding"
	"github.com/danos/yang/testutils"
)

// ---- fakes ---------------------------------------------------------------

// event log shared by the fake recorder and the fake component bus, so the
// tests can assert the order BeforeDispatch < call < AfterDispatch.
type eventLog struct {
	mu sync.Mutex
	ev []string
}

func (l *eventLog) add(format string, a ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ev = append(l.ev, fmt.Sprintf(format, a...))
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ev...)
}

func (l *eventLog) index(prefix string) int {
	for i, e := range l.snapshot() {
		if strings.HasPrefix(e, prefix) {
			return i
		}
	}
	return -1
}

func short(c string) string { return strings.TrimPrefix(c, "net.vyatta.test.") }

type fakeRecorder struct {
	log *eventLog

	mu        sync.Mutex
	beforeErr map[string]error // component+"/"+kind -> error (key without kind: any kind)
	recordAge int64            // added to the returned record time (simulates a slow journal)
	mono      func() int64
	before    []string
	after     []string
}

func (r *fakeRecorder) BeforeDispatch(c string, k DispatchKind, seq uint64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.before = append(r.before, short(c)+"/"+string(k))
	r.log.add("before %s/%s", short(c), k)
	if err := r.beforeErr[short(c)+"/"+string(k)]; err != nil {
		return 0, err
	}
	if err := r.beforeErr[short(c)]; err != nil {
		return 0, err
	}
	t := r.mono()
	return t - r.recordAge, nil
}

func (r *fakeRecorder) AfterDispatch(c string, k DispatchKind, seq uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.after = append(r.after, short(c)+"/"+string(k))
	r.log.add("after %s/%s", short(c), k)
}

// compOps is a scriptable component bus: per component and call number it can
// fail, block until released, or answer a read-back.
type compOps struct {
	*testOpsMgr
	log *eventLog

	mu      sync.Mutex
	calls   map[string]int           // component -> number of Set calls
	script  map[string]map[int]error // component -> call# (1-based) -> error
	block   map[string]chan struct{} // component -> channel the call waits on
	started map[string]chan struct{} // component -> closed when a call entered
	payload map[string][]string      // component -> payloads received
	dialErr error
	getter  map[string]string // component -> JSON returned by StoreConfigByModelInto
	getErr  map[string]error
}

func (o *compOps) Dial() error { return o.dialErr }

func (o *compOps) SetConfigForModel(model string, obj interface{}) error {
	o.mu.Lock()
	o.calls[model]++
	n := o.calls[model]
	err := o.script[model][n]
	blk := o.block[model]
	st := o.started[model]
	o.payload[model] = append(o.payload[model], obj.(string))
	o.mu.Unlock()
	o.log.add("call %s#%d", short(model), n)
	if st != nil {
		select {
		case <-st:
		default:
			close(st)
		}
	}
	if blk != nil {
		<-blk
	}
	if err != nil {
		return err
	}
	return o.testOpsMgr.SetConfigForModel(model, obj)
}

func (o *compOps) StoreConfigByModelInto(model string, obj interface{}) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.getErr[model]; err != nil {
		return err
	}
	if g, ok := o.getter[model]; ok {
		*(obj.(*string)) = g
		return nil
	}
	return o.testOpsMgr.StoreConfigByModelInto(model, obj)
}

func (o *compOps) callCount(model string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[model]
}

type inactiveSvc struct {
	*testSvcMgr
	inactive map[string]bool // service name
}

func (s *inactiveSvc) IsActive(name string) (bool, error) {
	return !s.inactive[name], nil
}

type compHarness struct {
	t      *testing.T
	ops    *compOps
	svc    *inactiveSvc
	rec    *fakeRecorder
	cm     *compMgr
	ms     *modelSet
	log    *eventLog
	now    time.Time // fake wall clock for the budget
	pol    CompensationPolicy
	mono   int64
	newDN  datanode.DataNode
	prevDN datanode.DataNode
}

func newCompHarness(t *testing.T) *compHarness {
	return newCompHarnessWithAlpha(t, "")
}

// newCompHarnessWithAlpha is newCompHarness with the body of alpha's YANG
// replaced by alphaYang (a complete "container alphaCont {...}" statement)
// when it is not empty.
func newCompHarnessWithAlpha(t *testing.T, alphaYang string) *compHarness {
	snippet := func(cont string) string {
		if cont == "alphaCont" && alphaYang != "" {
			return alphaYang
		}
		return "container " + cont + " { leaf l { type string; } }"
	}
	schemas := []*testutils.TestSchema{
		testutils.NewTestSchema("vyatta-test-alpha-v1", "alpha").AddSchemaSnippet(snippet("alphaCont")),
		testutils.NewTestSchema("vyatta-test-beta-v1", "beta").AddSchemaSnippet(snippet("betaCont")),
		testutils.NewTestSchema("vyatta-test-gamma-v1", "gamma").AddSchemaSnippet(snippet("gammaCont")),
	}
	dir := createYangDir(t, "a02p3", schemas)
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
	lg := &eventLog{}
	ops := &compOps{
		testOpsMgr: newTestOpsMgr(params), log: lg,
		calls:   map[string]int{},
		script:  map[string]map[int]error{},
		block:   map[string]chan struct{}{},
		started: map[string]chan struct{}{},
		payload: map[string][]string{},
		getter:  map[string]string{},
		getErr:  map[string]error{},
	}
	svc := &inactiveSvc{testSvcMgr: newTestSvcMgr(params), inactive: map[string]bool{}}
	h := &compHarness{t: t, ops: ops, svc: svc, ms: ms, log: lg,
		now: time.Unix(1000, 0), mono: 5000000}
	h.rec = &fakeRecorder{log: lg, beforeErr: map[string]error{},
		mono: func() int64 { return h.mono }}
	h.cm = NewCompMgr(ops, svc, ms, mappings)
	h.pol = CompensationPolicy{
		ForwardCallDeadline: 200 * time.Millisecond,
		CompCallDeadline:    200 * time.Millisecond,
		CompensationBudget:  time.Minute,
		SendWindow:          2 * time.Second,
		Now:                 func() time.Time { return h.now },
		MonoNowUs:           func() int64 { return h.mono },
	}
	var err error
	h.newDN, err = encoding.UnmarshalJSON(ms,
		[]byte(`{"alphaCont":{"l":"NEWa"},"betaCont":{"l":"NEWb"},"gammaCont":{"l":"NEWc"}}`))
	if err != nil {
		t.Fatal(err)
	}
	h.prevDN, err = encoding.UnmarshalJSON(ms,
		[]byte(`{"alphaCont":{"l":"OLDa"},"betaCont":{"l":"OLDb"}}`))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *compHarness) prev() datanode.DataNode { return h.prevDN }

func (h *compHarness) run(forced ...string) (*ApplyReport, []error) {
	return h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, nil, forced,
		h.pol, h.rec, nil)
}

func (h *compHarness) fail(model string, call int, err error) {
	if h.ops.script[model] == nil {
		h.ops.script[model] = map[int]error{}
	}
	h.ops.script[model][call] = err
}

func (h *compHarness) result(rep *ApplyReport, c string) ComponentResult {
	h.t.Helper()
	for _, r := range rep.Results {
		if r.Component == c {
			return r
		}
	}
	h.t.Fatalf("no result for %s in %+v", c, rep.Results)
	return ComponentResult{}
}

func (h *compHarness) wantPayload(model string, n int, contains string) {
	h.t.Helper()
	h.ops.mu.Lock()
	defer h.ops.mu.Unlock()
	p := h.ops.payload[model]
	if len(p) < n {
		h.t.Fatalf("%s: %d payloads, want at least %d", short(model), len(p), n)
	}
	if !strings.Contains(p[n-1], contains) {
		h.t.Fatalf("%s payload #%d = %s, want it to contain %s", short(model), n, p[n-1], contains)
	}
}

func dirtyOf(rep *ApplyReport, c string) (DirtyEntry, bool) {
	for _, d := range rep.Dirty {
		if d.Component == c {
			return d, true
		}
	}
	return DirtyEntry{}, false
}

func has(list []string, c string) bool {
	for _, x := range list {
		if x == c {
			return true
		}
	}
	return false
}

// ---- compensation ---------------------------------------------------------

func TestCompensatedAllSucceedNoCompensation(t *testing.T) {
	h := newCompHarness(t)
	rep, errs := h.run()
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	for _, c := range []string{compAlpha, compBeta, compGamma} {
		if r := h.result(rep, c); r.Forward != OutcomeApplied || r.Compensation != "" {
			t.Errorf("%s: %+v", c, r)
		}
		if h.ops.callCount(c) != 1 {
			t.Errorf("%s called %d times", c, h.ops.callCount(c))
		}
	}
	if len(rep.Dirty) != 0 {
		t.Errorf("dirty on success: %v", rep.Dirty)
	}
}

func TestCompensationOrderAndPayloads(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compGamma, 1, errors.New("gamma: cannot apply"))
	rep, errs := h.run()
	if len(errs) != 2 {
		t.Fatalf("want forward error + compensation report, got %v", errs)
	}
	want := []string{
		"before alpha/forward", "call alpha#1", "after alpha/forward",
		"before beta/forward", "call beta#1", "after beta/forward",
		"before gamma/forward", "call gamma#1", "after gamma/forward",
		// F first, then the applied ones in reverse apply order
		"before gamma/compensation", "call gamma#2", "after gamma/compensation",
		"before beta/compensation", "call beta#2", "after beta/compensation",
		"before alpha/compensation", "call alpha#2", "after alpha/compensation",
	}
	if got := h.log.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("event order:\n got %v\nwant %v", got, want)
	}
	// Compensation payloads come from prevCfg; gamma has no previous config.
	h.wantPayload(compBeta, 2, "OLDb")
	h.wantPayload(compAlpha, 2, "OLDa")
	h.ops.mu.Lock()
	gp := h.ops.payload[compGamma][1]
	h.ops.mu.Unlock()
	if gp != "{}" {
		t.Errorf("gamma compensation payload = %s, want {}", gp)
	}
	for _, c := range []string{compAlpha, compBeta, compGamma} {
		if r := h.result(rep, c); r.Compensation != OutcomeCompensated {
			t.Errorf("%s: %+v", c, r)
		}
	}
	if r := h.result(rep, compGamma); r.Forward != OutcomeFailed {
		t.Errorf("gamma forward = %s", r.Forward)
	}
	if len(rep.Dirty) != 0 {
		t.Errorf("all compensated, dirty = %v", rep.Dirty)
	}
	if !has(rep.Clean, compAlpha) || !has(rep.Clean, compBeta) || !has(rep.Clean, compGamma) {
		t.Errorf("clean = %v", rep.Clean)
	}
	if !strings.Contains(errs[1].Error(), "All applied components were reverted") {
		t.Errorf("report text: %s", errs[1])
	}
}

func TestOneCompensationFailsOthersStillAttempted(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compGamma, 1, errors.New("gamma: boom"))
	h.fail(compBeta, 2, errors.New("beta: compensation refused"))
	rep, errs := h.run()
	if len(errs) != 2 {
		t.Fatalf("errs: %v", errs)
	}
	if h.ops.callCount(compAlpha) != 2 {
		t.Errorf("alpha must still be compensated, calls=%d", h.ops.callCount(compAlpha))
	}
	if r := h.result(rep, compBeta); r.Compensation != OutcomeCompensationFailed || r.CompErr == nil {
		t.Errorf("beta: %+v", r)
	}
	if r := h.result(rep, compAlpha); r.Compensation != OutcomeCompensated {
		t.Errorf("alpha: %+v", r)
	}
	d, ok := dirtyOf(rep, compBeta)
	if !ok || d.Class != DirtyCompensationFailed {
		t.Errorf("beta dirty = %+v ok=%v", d, ok)
	}
	if !strings.Contains(errs[1].Error(), "NOT changed") ||
		!strings.Contains(errs[1].Error(), compBeta) {
		t.Errorf("report text: %s", errs[1])
	}
}

func TestFirstComponentFailsNothingToCompensateButItself(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compAlpha, 1, errors.New("alpha: no"))
	rep, errs := h.run()
	if h.ops.callCount(compBeta) != 0 || h.ops.callCount(compGamma) != 0 {
		t.Fatal("later components must not be touched")
	}
	if r := h.result(rep, compBeta); r.Forward != OutcomeNotAttempted {
		t.Errorf("beta: %+v", r)
	}
	if r := h.result(rep, compAlpha); r.Forward != OutcomeFailed || r.Compensation != OutcomeCompensated {
		t.Errorf("alpha: %+v", r)
	}
	if !strings.Contains(errs[0].Error(), "components not attempted") {
		t.Errorf("forward error: %s", errs[0])
	}
}

func TestSkipCompensationPolicyBootTimeFirstCommit(t *testing.T) {
	h := newCompHarness(t)
	h.pol.SkipCompensation = true
	h.fail(compGamma, 1, errors.New("gamma: boom"))
	rep, errs := h.run()
	if len(errs) != 2 {
		t.Fatalf("errs: %v", errs)
	}
	if h.ops.callCount(compAlpha) != 1 || h.ops.callCount(compBeta) != 1 {
		t.Error("no compensation call may be made")
	}
	for _, c := range []string{compAlpha, compBeta, compGamma} {
		if r := h.result(rep, c); r.Compensation != OutcomeCompensationSkipped || r.Note != skipPolicy {
			t.Errorf("%s: %+v", c, r)
		}
		if d, ok := dirtyOf(rep, c); !ok || d.Class != DirtyBootPartial {
			t.Errorf("%s dirty = %+v", c, d)
		}
	}
	if n := len(h.rec.before); n != 3 {
		t.Errorf("only the 3 forward dispatches are recorded, got %v", h.rec.before)
	}
}

func TestPrevCfgIsLazy(t *testing.T) {
	h := newCompHarness(t)
	called := false
	rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN,
		func() datanode.DataNode { called = true; return h.prevDN },
		nil, nil, h.pol, h.rec, nil)
	if len(errs) != 0 || len(rep.Results) != 3 {
		t.Fatalf("%v %v", errs, rep)
	}
	if called {
		t.Error("prevCfg must not be evaluated on success")
	}
}

func TestProgrammingErrorsCallNoComponent(t *testing.T) {
	h := newCompHarness(t)
	rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, nil, nil, nil, h.pol, h.rec, nil)
	if rep == nil || len(errs) != 1 || len(h.ops.calls) != 0 {
		t.Fatalf("nil prevCfg: %v %v %v", rep, errs, h.ops.calls)
	}
	rep, errs = h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, nil, nil, h.pol, nil, nil)
	if rep == nil || len(errs) != 1 || len(h.ops.calls) != 0 {
		t.Fatalf("nil recorder: %v %v", rep, errs)
	}
}

func TestDialFailureNothingAttempted(t *testing.T) {
	h := newCompHarness(t)
	h.ops.dialErr = errors.New("no bus")
	rep, errs := h.run()
	if rep == nil || len(errs) != 1 || len(h.ops.calls) != 0 || len(h.rec.before) != 0 {
		t.Fatalf("dial failure: %v %v", errs, h.ops.calls)
	}
	for _, r := range rep.Results {
		if r.Forward != OutcomeNotAttempted {
			t.Errorf("%+v", r)
		}
	}
}

// ---- unknown outcome ------------------------------------------------------

func TestUnknownForwardOutcomeIsNeverCompensated(t *testing.T) {
	for name, errv := range map[string]error{
		"NoReply value":   dbus.Error{Name: "org.freedesktop.DBus.Error.NoReply"},
		"NoReply pointer": dbus.NewError("org.freedesktop.DBus.Error.NoReply", nil),
		"Timeout":         dbus.Error{Name: "org.freedesktop.DBus.Error.Timeout"},
		"NoReply wrapped": fmt.Errorf("Set failed: %w",
			dbus.Error{Name: "org.freedesktop.DBus.Error.NoReply"}),
		"NoReply pointer wrapped twice": fmt.Errorf("outer: %w",
			fmt.Errorf("inner: %w", dbus.NewError("org.freedesktop.DBus.Error.NoReply", nil))),
	} {
		t.Run(name, func(t *testing.T) {
			h := newCompHarness(t)
			h.fail(compGamma, 1, errv)
			rep, errs := h.run()
			if len(errs) != 2 || !strings.Contains(errs[0].Error(), "OUTCOME UNKNOWN") {
				t.Fatalf("errs: %v", errs)
			}
			r := h.result(rep, compGamma)
			if r.Forward != OutcomeUnknown {
				t.Fatalf("forward = %s", r.Forward)
			}
			if r.Compensation == OutcomeCompensated {
				t.Fatal("an unknown-outcome component must never be reported compensated")
			}
			if r.Compensation != OutcomeCompensationUnverified {
				t.Errorf("compensation = %s", r.Compensation)
			}
			if h.ops.callCount(compGamma) != 2 {
				t.Error("a compensation attempt must still be made")
			}
			if d, ok := dirtyOf(rep, compGamma); !ok || d.Class != DirtyUnknown {
				t.Errorf("gamma dirty = %+v", d)
			}
			if has(rep.Clean, compGamma) {
				t.Error("gamma must not be clean")
			}
		})
	}
}

func TestInactiveServiceSkipsCompensation(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compGamma, 1, dbus.Error{Name: "org.freedesktop.DBus.Error.NoReply"})
	h.svc.inactive[h.cm.compMappings.Component(compGamma).name] = true
	rep, _ := h.run()
	r := h.result(rep, compGamma)
	if r.Compensation != OutcomeCompensationSkipped || r.Note != skipInactive {
		t.Fatalf("gamma: %+v", r)
	}
	if h.ops.callCount(compGamma) != 1 {
		t.Error("a dead service must not be called again")
	}
	if d, ok := dirtyOf(rep, compGamma); !ok || d.Class != DirtyUnknown {
		t.Errorf("dirty = %+v", d)
	}
}

// ---- recorder contract ----------------------------------------------------

func TestRecordPrecedesEveryCallForAllKinds(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compGamma, 1, errors.New("gamma: no"))
	h.run()
	ev := h.log.snapshot()
	for _, c := range []string{"alpha", "beta", "gamma"} {
		for _, k := range []string{"forward", "compensation"} {
			b := h.log.index(fmt.Sprintf("before %s/%s", c, k))
			a := h.log.index(fmt.Sprintf("after %s/%s", c, k))
			if b < 0 || a < 0 || b >= a {
				t.Fatalf("%s/%s: before=%d after=%d in %v", c, k, b, a, ev)
			}
		}
	}
	// every call is directly preceded (somewhere earlier) by its record
	for i, e := range ev {
		if strings.HasPrefix(e, "call ") {
			if i == 0 || !strings.HasPrefix(ev[i-1], "before ") {
				t.Fatalf("call %q not directly preceded by a record: %v", e, ev)
			}
		}
	}
}

func TestBeforeDispatchErrorForwardNotSentAndEarlierCompensated(t *testing.T) {
	h := newCompHarness(t)
	h.rec.beforeErr["beta/forward"] = errors.New("disk full")
	rep, errs := h.run()
	if h.ops.callCount(compBeta) != 0 {
		t.Fatal("the call must not be made when the record is not durable")
	}
	if h.ops.callCount(compGamma) != 0 {
		t.Fatal("later components must not be touched")
	}
	r := h.result(rep, compBeta)
	if r.Forward != OutcomeNotSent {
		t.Fatalf("beta: %+v", r)
	}
	var ej *ErrJournal
	if !errors.As(r.ForwardErr, &ej) {
		t.Errorf("ForwardErr = %v", r.ForwardErr)
	}
	if !strings.Contains(errs[0].Error(), "cannot record the dispatch for "+compBeta) ||
		!strings.Contains(errs[0].Error(), "nothing was sent to it") {
		t.Errorf("message: %s", errs[0])
	}
	// alpha, applied before, is compensated; beta (never sent) is clean
	if r := h.result(rep, compAlpha); r.Compensation != OutcomeCompensated {
		t.Errorf("alpha: %+v", r)
	}
	if !has(rep.Clean, compBeta) || len(rep.Dirty) != 0 {
		t.Errorf("clean=%v dirty=%v", rep.Clean, rep.Dirty)
	}
}

func TestBeforeDispatchErrorOnCompensationSkipsAndMarksUnfenced(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compGamma, 1, errors.New("gamma: no"))
	h.rec.beforeErr["beta/compensation"] = errors.New("journal gone")
	rep, _ := h.run()
	if h.ops.callCount(compBeta) != 1 {
		t.Fatal("beta compensation must not be sent")
	}
	r := h.result(rep, compBeta)
	if r.Compensation != OutcomeCompensationSkipped || r.Note != skipJournal {
		t.Fatalf("beta: %+v", r)
	}
	if d, ok := dirtyOf(rep, compBeta); !ok || d.Class != DirtyUnfenced {
		t.Errorf("beta dirty = %+v", d)
	}
	// the other compensations still go ahead
	if r := h.result(rep, compAlpha); r.Compensation != OutcomeCompensated {
		t.Errorf("alpha: %+v", r)
	}
}

func TestSendWindowOverrunReRecordsThenFails(t *testing.T) {
	h := newCompHarness(t)
	h.rec.recordAge = int64(3 * time.Second / time.Microsecond) // always older than the window
	rep, errs := h.run()
	if h.rec.before == nil || len(h.rec.before) != h.pol.withDefaults().MaxRecordAttempts {
		t.Fatalf("records = %v, want %d attempts", h.rec.before, maxRecordAttempts)
	}
	if h.ops.callCount(compAlpha) != 0 {
		t.Fatal("no call after a stale record")
	}
	if r := h.result(rep, compAlpha); r.Forward != OutcomeNotSent || !errors.Is(r.ForwardErr, ErrSendWindow) {
		t.Errorf("alpha: %+v", r)
	}
	if len(errs) == 0 {
		t.Error("expected an error")
	}
}

func TestSendWindowReRecordSucceedsOnSecondAttempt(t *testing.T) {
	h := newCompHarness(t)
	n := 0
	h.rec.mono = func() int64 {
		// first record is "old" (the persist took longer than the window)
		n++
		return h.mono
	}
	h.rec.recordAge = int64(3 * time.Second / time.Microsecond)
	// make only the first attempt stale
	orig := h.rec
	_ = orig
	staleOnce := &staleOnceRecorder{fakeRecorder: h.rec, staleFor: 1, age: h.rec.recordAge}
	h.rec.recordAge = 0
	rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, nil, nil, h.pol, staleOnce, nil)
	if len(errs) != 0 || h.result(rep, compAlpha).Forward != OutcomeApplied {
		t.Fatalf("%v %+v", errs, rep.Results)
	}
	if staleOnce.records["alpha/forward"] != 2 {
		t.Errorf("alpha was recorded %d times, want 2", staleOnce.records["alpha/forward"])
	}
}

type staleOnceRecorder struct {
	*fakeRecorder
	staleFor int
	age      int64
	records  map[string]int
}

func (s *staleOnceRecorder) BeforeDispatch(c string, k DispatchKind, seq uint64) (int64, error) {
	if s.records == nil {
		s.records = map[string]int{}
	}
	key := short(c) + "/" + string(k)
	s.records[key]++
	t, err := s.fakeRecorder.BeforeDispatch(c, k, seq)
	if err == nil && s.records[key] <= s.staleFor {
		t -= s.age
	}
	return t, err
}

// ---- bounded calls --------------------------------------------------------

func TestCallBoundedReturnsAtDeadlineAndSettlesLater(t *testing.T) {
	release := make(chan struct{})
	start := time.Now()
	err, pc := callBounded(30*time.Millisecond, func() error {
		<-release
		return errors.New("late")
	})
	if time.Since(start) > 2*time.Second {
		t.Fatal("did not return at the deadline")
	}
	if pc == nil || !isOutcomeUnknown(err) {
		t.Fatalf("err=%v pc=%v", err, pc)
	}
	var de *deadlineError
	if !errors.As(err, &de) || !de.Timeout() {
		t.Fatalf("not a timeout error: %v", err)
	}
	select {
	case <-pc.done:
		t.Fatal("settled too early")
	default:
	}
	close(release)
	select {
	case <-pc.done:
	case <-time.After(2 * time.Second):
		t.Fatal("pending call never settled")
	}
	if pc.err == nil || pc.err.Error() != "late" {
		t.Errorf("late result = %v", pc.err)
	}
}

func TestCallBoundedOnTimeHasNoPending(t *testing.T) {
	err, pc := callBounded(time.Second, func() error { return errors.New("x") })
	if pc != nil || err == nil || err.Error() != "x" {
		t.Fatalf("%v %v", err, pc)
	}
}

func TestForwardDeadlineIsUnknownAndNoSecondCallWhileUnsettled(t *testing.T) {
	h := newCompHarness(t)
	h.pol.ForwardCallDeadline = 30 * time.Millisecond
	h.pol.CompCallDeadline = 30 * time.Millisecond
	release := make(chan struct{})
	h.ops.block[compGamma] = release
	rep, errs := h.run()
	if len(errs) != 2 || !strings.Contains(errs[0].Error(), "OUTCOME UNKNOWN") {
		t.Fatalf("errs: %v", errs)
	}
	r := h.result(rep, compGamma)
	if r.Forward != OutcomeUnknown {
		t.Fatalf("gamma: %+v", r)
	}
	// the compensation of gamma must not overlap the abandoned call: it is
	// skipped as unsettled, and gamma is dirty "unsettled"
	if r.Compensation != OutcomeCompensationSkipped || r.Note != skipUnsettled {
		t.Errorf("gamma compensation: %+v", r)
	}
	if h.ops.callCount(compGamma) != 1 {
		t.Error("only the abandoned call may exist")
	}
	if d, ok := dirtyOf(rep, compGamma); !ok {
		t.Errorf("gamma not dirty")
	} else if d.Class != DirtyUnknown {
		t.Errorf("gamma class = %s (forward unknown wins)", d.Class)
	}
	// beta and alpha were compensated meanwhile
	if h.result(rep, compBeta).Compensation != OutcomeCompensated {
		t.Errorf("beta: %+v", h.result(rep, compBeta))
	}
	close(release)
	// after it settled, a new call to gamma may go ahead
	deadline := time.Now().Add(2 * time.Second)
	for h.cm.pending.get(compGamma) != nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if h.cm.pending.get(compGamma) != nil {
		t.Fatal("pending mark never cleared")
	}
	rep2, errs2 := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compGamma}, h.pol, h.rec, nil)
	if len(errs2) != 0 || h.result(rep2, compGamma).Forward != OutcomeApplied {
		t.Fatalf("reconcile after settle: %v %+v", errs2, rep2.Results)
	}
}

func TestCompensationBudgetExhaustionSkipsRemaining(t *testing.T) {
	h := newCompHarness(t)
	h.pol.CompensationBudget = 10 * time.Second
	h.fail(compGamma, 1, errors.New("gamma: no"))
	// every compensation call "takes" 6 s of the fake clock
	orig := h.ops
	h.cm = NewCompMgr(&advancingOps{compOps: orig, h: h, step: 6 * time.Second},
		h.svc, h.ms, h.cm.compMappings)
	rep, _ := h.run()
	if h.result(rep, compGamma).Compensation != OutcomeCompensated {
		t.Errorf("gamma: %+v", h.result(rep, compGamma))
	}
	if h.result(rep, compBeta).Compensation != OutcomeCompensated {
		t.Errorf("beta: %+v", h.result(rep, compBeta))
	}
	r := h.result(rep, compAlpha)
	if r.Compensation != OutcomeCompensationSkipped || r.Note != skipBudget {
		t.Fatalf("alpha must be skipped by the budget: %+v", r)
	}
	if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyCompensationFailed {
		t.Errorf("alpha dirty = %+v", d)
	}
}

// advancingOps moves the fake clock forward on every compensation Set.
type advancingOps struct {
	*compOps
	h    *compHarness
	step time.Duration
}

func (a *advancingOps) SetConfigForModel(model string, obj interface{}) error {
	err := a.compOps.SetConfigForModel(model, obj)
	if a.compOps.callCount(model) >= 2 {
		a.h.now = a.h.now.Add(a.step)
	}
	return err
}

func TestCompensationCallDeadlineIsMinOfCallAndRemainingBudget(t *testing.T) {
	h := newCompHarness(t)
	h.pol.CompCallDeadline = time.Hour
	h.pol.CompensationBudget = 40 * time.Millisecond
	h.pol.ForwardCallDeadline = time.Second
	h.fail(compBeta, 1, errors.New("beta: no"))
	release := make(chan struct{})
	defer close(release)
	// the second call to alpha (its compensation) blocks
	h.ops.block[compAlpha] = nil
	blocker := &blockOnSecond{compOps: h.ops, model: compAlpha, release: release}
	h.cm = NewCompMgr(blocker, h.svc, h.ms, h.cm.compMappings)
	h.pol.Now = time.Now // real clock for this test
	start := time.Now()
	rep, _ := h.run()
	if time.Since(start) > 2*time.Second {
		t.Fatal("compensation exceeded the budget-bounded deadline")
	}
	r := h.result(rep, compAlpha)
	if r.Compensation != OutcomeCompensationFailed || r.Note != "compensation call timed out" {
		t.Fatalf("alpha: %+v", r)
	}
	if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyUnknown {
		t.Errorf("alpha dirty = %+v", d)
	}
}

type blockOnSecond struct {
	*compOps
	model   string
	release chan struct{}
}

func (b *blockOnSecond) SetConfigForModel(model string, obj interface{}) error {
	err := b.compOps.SetConfigForModel(model, obj)
	if model == b.model && b.compOps.callCount(model) == 2 {
		<-b.release
	}
	return err
}

// ---- unsettled forward skip (r2) --------------------------------------------

// leaveCallPending starts a reconcile whose call to model blocks until the
// returned channel is closed and whose caller stops waiting, so the pending
// table holds an unsettled call for that component.
func leaveCallPending(h *compHarness, model string) chan struct{} {
	release := make(chan struct{})
	h.ops.block[model] = release
	pol := h.pol
	pol.ForwardCallDeadline = 30 * time.Millisecond
	h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{model}, pol, h.rec, nil)
	return release
}

func TestForwardUnsettledSkipIsDirtyUnsettledNeverClean(t *testing.T) {
	for _, first := range []string{compAlpha, compBeta} {
		t.Run(short(first), func(t *testing.T) {
			h := newCompHarness(t)
			release := leaveCallPending(h, first)
			defer close(release)
			callsBefore := h.ops.callCount(first)
			h.pol.ForwardCallDeadline = 30 * time.Millisecond
			rep, errs := h.run()
			if len(errs) != 2 {
				t.Fatalf("errs: %v", errs)
			}
			if h.ops.callCount(first) != callsBefore {
				t.Fatal("no second call may reach a component with an unsettled call")
			}
			r := h.result(rep, first)
			if r.Forward != OutcomeNotSent || r.Note != skipUnsettled || !errors.Is(r.ForwardErr, ErrUnsettled) {
				t.Fatalf("%s: %+v", first, r)
			}
			d, ok := dirtyOf(rep, first)
			if !ok || d.Class != DirtyUnsettled {
				t.Fatalf("dirty = %+v ok=%v, want unsettled", d, ok)
			}
			if has(rep.Clean, first) {
				t.Errorf("%s must not be clean: %v", first, rep.Clean)
			}
			for _, e := range errs {
				if strings.Contains(e.Error(), "nothing was sent") {
					t.Errorf("must not claim nothing was sent: %s", e)
				}
				if strings.Contains(e.Error(), "All applied components were reverted") {
					t.Errorf("must not claim a full revert: %s", e)
				}
			}
			if !strings.Contains(errs[0].Error(), "not settled") &&
				!strings.Contains(errs[0].Error(), "state is unknown") {
				t.Errorf("forward error should say the state is unknown: %s", errs[0])
			}
			// components after it are not attempted; those before it (if any)
			// are compensated
			if first == compBeta {
				if h.result(rep, compAlpha).Compensation != OutcomeCompensated {
					t.Errorf("alpha: %+v", h.result(rep, compAlpha))
				}
			}
			if h.result(rep, compGamma).Forward != OutcomeNotAttempted {
				t.Errorf("gamma: %+v", h.result(rep, compGamma))
			}
		})
	}
}

// The journal / send-window not_sent stays clean for a forward dispatch (the
// call was never made and no earlier call is outstanding): this is the case
// TestNotSentIsCleanForwardButDirtyForReconcile pins; the unsettled case above
// is the opposite one.

// ---- pending table (r2) -------------------------------------------------------

func TestStartCallRegisteredPendingIsClearedByTheCompletionGoroutine(t *testing.T) {
	var tbl pendingTable
	for i := 0; i < 500; i++ {
		pc := &pendingCall{done: make(chan struct{})}
		tbl.set("c", pc)
		startCall(pc, func() error { return nil }, func() { tbl.clear("c", pc) })
		err, timedOut := waitCall(pc, time.Second)
		if timedOut || err != nil {
			t.Fatalf("iteration %d: %v %v", i, err, timedOut)
		}
		deadline := time.Now().Add(2 * time.Second)
		for tbl.get("c") != nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if tbl.get("c") != nil {
			t.Fatalf("iteration %d: a finished call stayed in the pending table", i)
		}
	}
}

// The race the review found: a call finishing right around the caller's
// deadline must never leave a finished call pending. Every iteration races the
// completion against a tiny deadline, then checks that no entry survives and
// that the next dispatch is not refused as unsettled.
func TestDispatchFinishingAroundTheDeadlineNeverLeavesAFinishedCallPending(t *testing.T) {
	h := newCompHarness(t)
	h.pol.ForwardCallDeadline = time.Millisecond
	slow := &sleepingOps{compOps: h.ops, d: 900 * time.Microsecond}
	h.cm = NewCompMgr(slow, h.svc, h.ms, h.cm.compMappings)
	for i := 0; i < 300; i++ {
		rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
		_ = rep
		deadline := time.Now().Add(2 * time.Second)
		for h.cm.pending.get(compAlpha) != nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if pc := h.cm.pending.get(compAlpha); pc != nil {
			select {
			case <-pc.done:
				t.Fatalf("iteration %d: finished call still pending", i)
			default:
				t.Fatalf("iteration %d: call never finished", i)
			}
		}
	}
	// and a normal-deadline dispatch afterwards works
	h.pol.ForwardCallDeadline = time.Second
	rep, errs := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	if len(errs) != 0 || h.result(rep, compAlpha).Forward != OutcomeApplied {
		t.Fatalf("%v %+v", errs, rep.Results)
	}
}

type sleepingOps struct {
	*compOps
	d time.Duration
}

func (s *sleepingOps) SetConfigForModel(model string, obj interface{}) error {
	time.Sleep(s.d)
	return s.compOps.SetConfigForModel(model, obj)
}

// ---- CompensateReport -----------------------------------------------------

func TestCompensateReportRevertsOnlyAppliedInReverseOrder(t *testing.T) {
	h := newCompHarness(t)
	rep, errs := h.run()
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	h.log.mu.Lock()
	h.log.ev = nil
	h.log.mu.Unlock()
	out, cerrs := h.cm.CompensateReport(h.ms, h.prev, rep, h.pol, h.rec, nil)
	if len(cerrs) != 0 {
		t.Fatalf("errs: %v", cerrs)
	}
	want := []string{
		"before gamma/compensation", "call gamma#2", "after gamma/compensation",
		"before beta/compensation", "call beta#2", "after beta/compensation",
		"before alpha/compensation", "call alpha#2", "after alpha/compensation",
	}
	if got := h.log.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	// the input report is not modified
	for _, r := range rep.Results {
		if r.Compensation != "" {
			t.Errorf("input report modified: %+v", r)
		}
	}
	for _, c := range []string{compAlpha, compBeta, compGamma} {
		if h.result(out, c).Compensation != OutcomeCompensated || !has(out.Clean, c) {
			t.Errorf("%s: %+v clean=%v", c, h.result(out, c), out.Clean)
		}
	}
}

func TestCompensateReportSkipsNonAppliedAndRepairSteps(t *testing.T) {
	h := newCompHarness(t)
	applied := &ApplyReport{Results: []ComponentResult{
		{Component: compAlpha, Forward: OutcomeApplied},
		{Component: compBeta, Forward: OutcomeFailed},
		{Component: compGamma, Forward: OutcomeNotAttempted},
	}}
	repair := &ApplyReport{Results: []ComponentResult{
		{Component: compAlpha, Forward: OutcomeApplied, Repair: true},
		{Component: compBeta, Forward: OutcomeApplied},
	}}
	out, _ := h.cm.CompensateReport(h.ms, h.prev, applied, h.pol, h.rec, nil)
	if h.ops.callCount(compAlpha) != 1 || h.ops.callCount(compBeta) != 0 || h.ops.callCount(compGamma) != 0 {
		t.Errorf("calls = %v", h.ops.calls)
	}
	if h.result(out, compBeta).Compensation != "" {
		t.Errorf("failed component must not be compensated here: %+v", h.result(out, compBeta))
	}
	h2 := newCompHarness(t)
	out2, _ := h2.cm.CompensateReport(h2.ms, h2.prev, repair, h2.pol, h2.rec, nil)
	if h2.ops.callCount(compAlpha) != 0 || h2.ops.callCount(compBeta) != 1 {
		t.Errorf("repair steps must not be compensated: %v", h2.ops.calls)
	}
	if h2.result(out2, compAlpha).Compensation != "" {
		t.Errorf("repair: %+v", h2.result(out2, compAlpha))
	}
}

func TestCompensateReportNilReportRecorderAndPrev(t *testing.T) {
	h := newCompHarness(t)
	out, errs := h.cm.CompensateReport(h.ms, h.prev, nil, h.pol, h.rec, nil)
	if out == nil || len(errs) != 0 {
		t.Errorf("nil applied: %v %v", out, errs)
	}
	out, errs = h.cm.CompensateReport(h.ms, h.prev, &ApplyReport{}, h.pol, nil, nil)
	if out == nil || len(errs) != 1 {
		t.Errorf("nil recorder: %v %v", out, errs)
	}
	out, errs = h.cm.CompensateReport(h.ms, nil, &ApplyReport{}, h.pol, h.rec, nil)
	if out == nil || len(errs) != 1 {
		t.Errorf("nil prev: %v %v", out, errs)
	}
}

func TestCompensateReportBeforeDispatchErrorNoCallDirtyUnfenced(t *testing.T) {
	h := newCompHarness(t)
	rep, _ := h.run()
	h.rec.beforeErr["beta"] = errors.New("ENOSPC")
	out, errs := h.cm.CompensateReport(h.ms, h.prev, rep, h.pol, h.rec, nil)
	if h.ops.callCount(compBeta) != 1 {
		t.Fatal("no compensation call may reach beta")
	}
	if len(errs) != 1 {
		t.Fatalf("errs: %v", errs)
	}
	if d, ok := dirtyOf(out, compBeta); !ok || d.Class != DirtyUnfenced {
		t.Errorf("beta dirty = %+v", d)
	}
	if !strings.Contains(errs[0].Error(), "compensation_skipped") {
		t.Errorf("text: %s", errs[0])
	}
}

// ---- forced components ----------------------------------------------------

func TestForcedRepairStepIsAppliedNeverCompensatedDroppedKeysNoted(t *testing.T) {
	h := newCompHarness(t)
	changed := map[string]bool{h.cm.compMappings.nsMapKeyFor(t, compGamma): true}
	h.fail(compGamma, 1, errors.New("gamma: no"))
	rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, &changed,
		[]string{compAlpha, compAlpha, "net.vyatta.test.nope"}, h.pol, h.rec, nil)
	if len(errs) != 2 {
		t.Fatalf("errs: %v", errs)
	}
	ra := h.result(rep, compAlpha)
	if !ra.Repair || ra.Forward != OutcomeApplied || ra.Compensation != "" {
		t.Fatalf("alpha: %+v", ra)
	}
	if h.ops.callCount(compAlpha) != 1 {
		t.Errorf("alpha must be called once (repair, not compensated), got %d", h.ops.callCount(compAlpha))
	}
	if len(rep.Notes) != 1 || !strings.Contains(rep.Notes[0], "net.vyatta.test.nope") {
		t.Errorf("notes = %v", rep.Notes)
	}
	if h.ops.callCount(compBeta) != 0 {
		t.Error("beta is neither changed nor forced")
	}
}

// A failing repair step is never sent prevCfg (its target already equals the
// previous state) and, having proved nothing, is neither Clean nor newly Dirty:
// an existing dirty entry of configd stays.
func TestForcedRepairStepFailureIsNotCompensatedAndNotClean(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantDirty DirtyClass
	}{
		{"failed", errors.New("alpha: no"), ""},
		{"unknown", context.DeadlineExceeded, DirtyUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCompHarness(t)
			changed := map[string]bool{h.cm.compMappings.nsMapKeyFor(t, compGamma): true}
			h.fail(compAlpha, 1, tc.err)
			rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, &changed,
				[]string{compAlpha}, h.pol, h.rec, nil)
			if len(errs) == 0 {
				t.Fatal("the commit must fail")
			}
			if n := h.ops.callCount(compAlpha); n != 1 {
				t.Errorf("the failing repair step must be called exactly once, got %d", n)
			}
			if h.ops.callCount(compGamma) != 0 {
				t.Errorf("the changed component must not be called after the repair failed")
			}
			ra := h.result(rep, compAlpha)
			if !ra.Repair || ra.Compensation != "" {
				t.Errorf("alpha: %+v", ra)
			}
			if has(rep.Clean, compAlpha) {
				t.Errorf("a failed repair must not be Clean: %v", rep.Clean)
			}
			d, dirty := dirtyOf(rep, compAlpha)
			if tc.wantDirty == "" && dirty {
				t.Errorf("a failed repair must not add a dirty entry: %+v", d)
			}
			if tc.wantDirty != "" && (!dirty || d.Class != tc.wantDirty) {
				t.Errorf("alpha dirty = %+v, want class %s", d, tc.wantDirty)
			}
		})
	}
}

// Opportunistic repair steps: applied in order with the running config, never
// compensated, and their failure does not fail the forward phase.
func TestOpportunisticRepairFailureDoesNotFailTheCommit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantDirty DirtyClass
	}{
		{"failed", errors.New("alpha: no"), ""},
		{"unknown", context.DeadlineExceeded, DirtyUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCompHarness(t)
			changed := map[string]bool{h.cm.compMappings.nsMapKeyFor(t, compGamma): true}
			h.fail(compAlpha, 1, tc.err)
			pol := h.pol
			pol.Opportunistic = []string{compAlpha}
			rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, &changed,
				nil, pol, h.rec, nil)
			if len(errs) != 0 {
				t.Fatalf("an opportunistic failure must not fail the commit: %v", errs)
			}
			if h.ops.callCount(compAlpha) != 1 || h.ops.callCount(compGamma) != 1 {
				t.Errorf("alpha %d, gamma %d calls; want 1 and 1",
					h.ops.callCount(compAlpha), h.ops.callCount(compGamma))
			}
			ra := h.result(rep, compAlpha)
			if !ra.Repair || !ra.Soft || ra.Compensation != "" || ra.Forward == OutcomeApplied {
				t.Errorf("alpha: %+v", ra)
			}
			if has(rep.Clean, compAlpha) {
				t.Errorf("a failed opportunistic repair must not be Clean")
			}
			d, dirty := dirtyOf(rep, compAlpha)
			if tc.wantDirty == "" && dirty || tc.wantDirty != "" && (!dirty || d.Class != tc.wantDirty) {
				t.Errorf("alpha dirty = %+v (%v), want class %q", d, dirty, tc.wantDirty)
			}
		})
	}
}

// An opportunistic repair whose prerequisite (also opportunistic) did not
// succeed is skipped as "blocked by <p>"; a changed component is never gated.
func TestOpportunisticRepairBlockedByFailedOpportunisticPrerequisite(t *testing.T) {
	h := newCompHarness(t)
	changed := map[string]bool{h.cm.compMappings.nsMapKeyFor(t, compGamma): true}
	h.fail(compAlpha, 1, errors.New("alpha: no"))
	pol := h.pol
	pol.Opportunistic = []string{compAlpha, compBeta}
	rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, &changed,
		nil, pol, h.rec, nil)
	if len(errs) != 0 {
		t.Fatalf("no error expected: %v", errs)
	}
	if h.ops.callCount(compBeta) != 0 {
		t.Errorf("beta must not be called: it is blocked by alpha")
	}
	if h.ops.callCount(compAlpha) != 1 || h.ops.callCount(compGamma) != 1 {
		t.Errorf("alpha %d, gamma %d calls; want 1 and 1 (changed gamma is never gated)",
			h.ops.callCount(compAlpha), h.ops.callCount(compGamma))
	}
	rb := h.result(rep, compBeta)
	if !rb.Soft || rb.Forward != OutcomeNotAttempted || rb.Note != "blocked by "+compAlpha || rb.Compensation != "" {
		t.Errorf("beta: %+v", rb)
	}
	if has(rep.Clean, compBeta) {
		t.Errorf("a blocked component must not be Clean")
	}
	if _, dirty := dirtyOf(rep, compBeta); dirty {
		t.Errorf("a blocked component adds no dirty entry (the existing one stays)")
	}
}

func TestOpportunisticRepairAppliedIsCleanAndNotCompensatedWhenChangedFails(t *testing.T) {
	h := newCompHarness(t)
	changed := map[string]bool{h.cm.compMappings.nsMapKeyFor(t, compGamma): true}
	h.fail(compGamma, 1, errors.New("gamma: no"))
	pol := h.pol
	pol.Opportunistic = []string{compAlpha, compGamma, "net.vyatta.test.nope"}
	rep, errs := h.cm.ComponentSetRunningCompensated(h.ms, h.newDN, h.prev, &changed,
		nil, pol, h.rec, nil)
	if len(errs) == 0 {
		t.Fatal("the changed component failed: the commit must fail")
	}
	if n := h.ops.callCount(compAlpha); n != 1 {
		t.Errorf("an applied opportunistic repair must not be compensated, calls=%d", n)
	}
	ra := h.result(rep, compAlpha)
	if !ra.Soft || ra.Forward != OutcomeApplied || ra.Compensation != "" || !has(rep.Clean, compAlpha) {
		t.Errorf("alpha: %+v clean=%v", ra, rep.Clean)
	}
	if rg := h.result(rep, compGamma); rg.Soft {
		t.Errorf("a changed component is never opportunistic: %+v", rg)
	}
}

// nsMapKeyFor returns a namespace belonging to the component (test helper).
func (cm *ComponentMappings) nsMapKeyFor(t *testing.T, model string) string {
	for ns, m := range cm.nsMap {
		if m == model {
			return ns
		}
	}
	t.Fatalf("no namespace for %s", model)
	return ""
}

// ---- reconcile and read-back ----------------------------------------------

func TestReconcileVerifiedWhenReadBackMatches(t *testing.T) {
	h := newCompHarness(t)
	h.ops.getter[compAlpha] = `{"vyatta-test-alpha-v1:alphaCont":{"l":"OLDa"}}`
	rep, errs := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	r := h.result(rep, compAlpha)
	if r.Verified != VerifyMatched || !has(rep.Clean, compAlpha) {
		t.Fatalf("alpha: %+v clean=%v payload=%v", r, rep.Clean, h.ops.payload[compAlpha])
	}
	if got := h.rec.before; len(got) != 1 || got[0] != "alpha/reconcile" {
		t.Errorf("recorder: %v", got)
	}
}

func TestReconcileMismatchRetriesThenUnverified(t *testing.T) {
	h := newCompHarness(t)
	h.ops.getter[compAlpha] = `{"vyatta-test-alpha-v1:alphaCont":{"l":"SOMETHING ELSE"}}`
	rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	r := h.result(rep, compAlpha)
	if r.Verified != VerifyMismatch {
		t.Fatalf("alpha: %+v", r)
	}
	if h.ops.callCount(compAlpha) != verifyAttempts {
		t.Errorf("calls = %d, want %d", h.ops.callCount(compAlpha), verifyAttempts)
	}
	if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyUnverified {
		t.Errorf("dirty = %+v", d)
	}
}

func TestReconcileMismatchThenMatchOnRetry(t *testing.T) {
	h := newCompHarness(t)
	h.ops.getter[compAlpha] = `{"vyatta-test-alpha-v1:alphaCont":{"l":"stale"}}`
	flip := &flipGetter{compOps: h.ops, model: compAlpha,
		after: 2, good: `{"vyatta-test-alpha-v1:alphaCont":{"l":"OLDa"}}`}
	h.cm = NewCompMgr(flip, h.svc, h.ms, h.cm.compMappings)
	rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	if r := h.result(rep, compAlpha); r.Verified != VerifyMatched {
		t.Fatalf("alpha: %+v", r)
	}
	if h.ops.callCount(compAlpha) != 2 {
		t.Errorf("calls = %d, want 2", h.ops.callCount(compAlpha))
	}
}

type flipGetter struct {
	*compOps
	model string
	after int
	good  string
}

func (f *flipGetter) StoreConfigByModelInto(model string, obj interface{}) error {
	if model == f.model && f.compOps.callCount(model) >= f.after {
		*(obj.(*string)) = f.good
		return nil
	}
	return f.compOps.StoreConfigByModelInto(model, obj)
}

func TestReconcileComponentWithoutGetterIsCleanButNotVerifiable(t *testing.T) {
	h := newCompHarness(t)
	h.ops.getErr[compAlpha] = mgmterror.NewOperationNotSupportedApplicationError()
	rep, errs := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	r := h.result(rep, compAlpha)
	if r.Verified != VerifyNotVerifiable || !has(rep.Clean, compAlpha) {
		t.Fatalf("alpha: %+v clean=%v", r, rep.Clean)
	}
}

func TestReconcileGetterErrorIsUnverified(t *testing.T) {
	h := newCompHarness(t)
	h.ops.getErr[compAlpha] = errors.New("component broke")
	rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	if r := h.result(rep, compAlpha); r.Verified != VerifyError {
		t.Fatalf("alpha: %+v", r)
	}
	if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyUnverified {
		t.Errorf("dirty = %+v", d)
	}
}

func TestReconcileSkipsDependantsOfFailedPrerequisite(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compAlpha, 1, errors.New("alpha: no")) // alpha is before beta is before gamma
	for _, c := range []string{compAlpha, compBeta, compGamma} {
		h.ops.getErr[c] = mgmterror.NewOperationNotSupportedApplicationError()
	}
	rep, errs := h.cm.ComponentReconcile(h.ms, h.prev,
		nil, []string{compGamma, compBeta, compAlpha}, h.pol, h.rec, nil)
	if len(errs) != 1 {
		t.Fatalf("errs: %v", errs)
	}
	if h.ops.callCount(compBeta) != 0 || h.ops.callCount(compGamma) != 0 {
		t.Errorf("dependants must not be called: %v", h.ops.calls)
	}
	if r := h.result(rep, compBeta); r.Note != "blocked by "+compAlpha {
		t.Errorf("beta: %+v", r)
	}
	// gamma's transitive prerequisites are alpha and beta; the first one that
	// did not reconcile (alpha, the root cause) is named.
	if r := h.result(rep, compGamma); r.Note != "blocked by "+compAlpha {
		t.Errorf("gamma: %+v", r)
	}
	// order of results follows the dependency order, not the argument order
	if rep.Results[0].Component != compAlpha {
		t.Errorf("results order: %+v", rep.Results)
	}
}

func TestReconcileBeforeDispatchErrorNotSentNotReconciled(t *testing.T) {
	h := newCompHarness(t)
	h.rec.beforeErr["alpha/reconcile"] = errors.New("journal gone")
	rep, errs := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	if h.ops.callCount(compAlpha) != 0 {
		t.Fatal("not sent expected")
	}
	if r := h.result(rep, compAlpha); r.Forward != OutcomeNotSent {
		t.Fatalf("%+v", r)
	}
	if len(errs) != 1 {
		t.Errorf("errs: %v", errs)
	}
	// A reconcile that could not be recorded leaves the component dirty
	// (unfenced); unlike a forward not_sent it is NOT clean.
	if has(rep.Clean, compAlpha) {
		t.Errorf("a reconcile that was not sent must not be clean: clean=%v", rep.Clean)
	}
	if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyUnfenced {
		t.Errorf("dirty = %+v ok=%v, want unfenced", d, ok)
	}
}

// The same not_sent outcome means different things per phase: a forward
// dispatch that was not sent changed nothing (clean), a reconcile that was not
// sent leaves a dirty component dirty.
func TestNotSentIsCleanForwardButDirtyForReconcile(t *testing.T) {
	h := newCompHarness(t)
	h.rec.beforeErr["alpha/forward"] = errors.New("no journal")
	rep, _ := h.run()
	if r := h.result(rep, compAlpha); r.Forward != OutcomeNotSent || r.Phase != KindForward {
		t.Fatalf("%+v", r)
	}
	if !has(rep.Clean, compAlpha) {
		t.Errorf("forward not_sent must be clean: %v", rep.Clean)
	}
	if _, ok := dirtyOf(rep, compAlpha); ok {
		t.Error("forward not_sent must not be dirty")
	}
}

func TestReconcileFailureClassesAreDirtyNeverClean(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compAlpha, 1, errors.New("alpha: still broken"))
	h.fail(compBeta, 1, dbus.Error{Name: "org.freedesktop.DBus.Error.NoReply"})
	for _, c := range []string{compAlpha, compBeta} {
		h.ops.getErr[c] = mgmterror.NewOperationNotSupportedApplicationError()
	}
	// beta is not a prerequisite-blocked case here: reconcile alpha and gamma
	// separately from beta by asking for them in one call; alpha fails, beta
	// (depends on alpha) is blocked, gamma depends on both.
	rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha, compBeta, compGamma}, h.pol, h.rec, nil)
	if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyCompensationFailed {
		t.Errorf("alpha: %+v ok=%v", d, ok)
	}
	if has(rep.Clean, compAlpha) {
		t.Error("a failed reconcile must not be clean")
	}
	// timeout class, on its own
	h2 := newCompHarness(t)
	h2.fail(compAlpha, 1, dbus.Error{Name: "org.freedesktop.DBus.Error.NoReply"})
	rep2, _ := h2.cm.ComponentReconcile(h2.ms, h2.prev, nil, []string{compAlpha}, h2.pol, h2.rec, nil)
	if d, ok := dirtyOf(rep2, compAlpha); !ok || d.Class != DirtyUnknown || has(rep2.Clean, compAlpha) {
		t.Errorf("timeout reconcile: dirty=%+v ok=%v clean=%v", d, ok, rep2.Clean)
	}
}

func TestReconcileUnsettledEarlierCallIsDirtyUnsettled(t *testing.T) {
	h := newCompHarness(t)
	h.pol.ForwardCallDeadline = 30 * time.Millisecond
	release := make(chan struct{})
	h.ops.block[compAlpha] = release
	// first call times out and stays pending
	h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	// second call finds the first one unsettled
	rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
	close(release)
	r := h.result(rep, compAlpha)
	if r.Forward != OutcomeNotSent || r.Note != skipUnsettled {
		t.Fatalf("alpha: %+v", r)
	}
	if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyUnsettled || has(rep.Clean, compAlpha) {
		t.Errorf("dirty=%+v ok=%v clean=%v", d, ok, rep.Clean)
	}
}

// Contract for configd (3-B): every requested component that is not Clean in
// a reconcile report is dirty in it, including those that were not touched.
func TestReconcileBlockedComponentsStayDirtyAndAreNotClean(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compAlpha, 1, errors.New("alpha: no"))
	rep, _ := h.cm.ComponentReconcile(h.ms, h.prev,
		nil, []string{compAlpha, compBeta, compGamma}, h.pol, h.rec, nil)
	for _, c := range []string{compAlpha, compBeta, compGamma} {
		if has(rep.Clean, c) {
			t.Errorf("%s must not be clean", c)
		}
	}
	for _, c := range []string{compBeta, compGamma} {
		d, ok := dirtyOf(rep, c)
		if !ok || d.Class != DirtyBlocked || !strings.Contains(d.Reason, "blocked by") {
			t.Errorf("%s dirty = %+v ok=%v", c, d, ok)
		}
	}
	// requested set = requested results: every requested component is
	// accounted for either as clean or as dirty
	if len(rep.Clean)+len(rep.Dirty) != 3 {
		t.Errorf("clean=%v dirty=%v", rep.Clean, rep.Dirty)
	}
}

func TestReconcileDialFailureLeavesEverythingDirtyBlocked(t *testing.T) {
	h := newCompHarness(t)
	h.ops.dialErr = errors.New("no bus")
	rep, errs := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha, compBeta}, h.pol, h.rec, nil)
	if len(errs) != 1 || len(h.ops.calls) != 0 {
		t.Fatalf("%v %v", errs, h.ops.calls)
	}
	if len(rep.Clean) != 0 || len(rep.Dirty) != 2 {
		t.Errorf("clean=%v dirty=%v", rep.Clean, rep.Dirty)
	}
	for _, d := range rep.Dirty {
		if d.Class != DirtyBlocked {
			t.Errorf("%+v", d)
		}
	}
}

// ---- CompensateReport with unreachable components -------------------------

func TestCompensateReportDialFailureKeepsAppliedComponentsDirty(t *testing.T) {
	h := newCompHarness(t)
	rep, errs := h.run()
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	h.ops.dialErr = errors.New("bus gone")
	callsBefore := len(h.rec.before)
	out, cerrs := h.cm.CompensateReport(h.ms, h.prev, rep, h.pol, h.rec, nil)
	if len(cerrs) != 1 {
		t.Fatalf("errs: %v", cerrs)
	}
	if len(h.rec.before) != callsBefore {
		t.Error("no dispatch may be recorded when the components cannot be reached")
	}
	for _, c := range []string{compAlpha, compBeta, compGamma} {
		d, ok := dirtyOf(out, c)
		if !ok || d.Class != DirtyCompensationFailed {
			t.Errorf("%s dirty = %+v ok=%v", c, d, ok)
		}
		if has(out.Clean, c) {
			t.Errorf("%s must not be clean", c)
		}
		if r := h.result(out, c); r.Compensation != OutcomeCompensationSkipped || r.Note != skipDial {
			t.Errorf("%s: %+v", c, r)
		}
	}
	if !strings.Contains(cerrs[0].Error(), "bus gone") ||
		!strings.Contains(cerrs[0].Error(), "NOT changed") {
		t.Errorf("text: %s", cerrs[0])
	}
	// repair steps and failed components are not turned into dirty by this path
	rep2 := &ApplyReport{Results: []ComponentResult{
		{Component: compAlpha, Phase: KindForward, Forward: OutcomeApplied, Repair: true},
		{Component: compBeta, Phase: KindForward, Forward: OutcomeFailed},
	}}
	out2, _ := h.cm.CompensateReport(h.ms, h.prev, rep2, h.pol, h.rec, nil)
	if _, ok := dirtyOf(out2, compAlpha); ok {
		t.Error("a repair step is not dirty")
	}
}

func TestReconcileUnknownRequestedComponentIsNoted(t *testing.T) {
	h := newCompHarness(t)
	rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha, "net.vyatta.test.ghost", compAlpha},
		h.pol, h.rec, nil)
	if len(rep.Results) != 1 || len(rep.Notes) != 1 {
		t.Fatalf("results=%+v notes=%v", rep.Results, rep.Notes)
	}
}

// The read-back compares after schema normalisation: an unqualified answer
// from the component equals the module-qualified payload that was sent.
func TestReconcileReadBackSemanticallyEquivalentSpellingsMatch(t *testing.T) {
	for name, got := range map[string]string{
		"unqualified top-level name": `{"alphaCont":{"l":"OLDa"}}`,
		"qualified":                  `{"vyatta-test-alpha-v1:alphaCont":{"l":"OLDa"}}`,
		"extra whitespace":           "{ \"vyatta-test-alpha-v1:alphaCont\" :\n { \"l\" : \"OLDa\" } }",
	} {
		t.Run(name, func(t *testing.T) {
			h := newCompHarness(t)
			h.ops.getter[compAlpha] = got
			rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
			if r := h.result(rep, compAlpha); r.Verified != VerifyMatched {
				t.Fatalf("%s: %+v (sent %v)", name, r, h.ops.payload[compAlpha])
			}
		})
	}
}

func TestReconcileReadBackDifferentValueOrExtraContainerMismatches(t *testing.T) {
	for name, got := range map[string]string{
		"different value": `{"alphaCont":{"l":"OTHER"}}`,
		"empty":           `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := newCompHarness(t)
			h.ops.getter[compAlpha] = got
			rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
			if r := h.result(rep, compAlpha); r.Verified != VerifyMismatch {
				t.Fatalf("%s: %+v", name, r)
			}
		})
	}
}

func TestReconcileReadBackUnparsableAnswerIsVerificationError(t *testing.T) {
	for name, got := range map[string]string{
		"not json":          `this is not json`,
		"unknown container": `{"noSuchContainer":{"l":"x"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := newCompHarness(t)
			h.ops.getter[compAlpha] = got
			rep, _ := h.cm.ComponentReconcile(h.ms, h.prev, nil, []string{compAlpha}, h.pol, h.rec, nil)
			r := h.result(rep, compAlpha)
			if r.Verified != VerifyError || r.ForwardErr == nil ||
				!strings.Contains(r.ForwardErr.Error(), "read back") {
				t.Fatalf("%s: %+v", name, r)
			}
			if d, ok := dirtyOf(rep, compAlpha); !ok || d.Class != DirtyUnverified {
				t.Errorf("dirty = %+v ok=%v", d, ok)
			}
			if has(rep.Clean, compAlpha) {
				t.Error("must not be clean")
			}
		})
	}
}

// ---- prerequisites --------------------------------------------------------

func TestPrerequisitesTransitiveClosureInOrder(t *testing.T) {
	h := newCompHarness(t)
	m := h.cm.compMappings
	if got := m.Prerequisites(compAlpha); len(got) != 0 {
		t.Errorf("alpha: %v", got)
	}
	if got := m.Prerequisites(compBeta); !reflect.DeepEqual(got, []string{compAlpha}) {
		t.Errorf("beta: %v", got)
	}
	if got := m.Prerequisites(compGamma); !reflect.DeepEqual(got, []string{compAlpha, compBeta}) {
		t.Errorf("gamma: %v", got)
	}
	if got := m.Prerequisites("net.vyatta.test.unknown"); got != nil {
		t.Errorf("unknown: %v", got)
	}
}

func TestPrerequisitesThroughNonComponentVertex(t *testing.T) {
	// alpha before X (not a component); gamma After X: gamma depends on alpha
	// through the unit X, which is not a VCI component.
	snippet := func(cont string) string {
		return "container " + cont + " { leaf l { type string; } }"
	}
	schemas := []*testutils.TestSchema{
		testutils.NewTestSchema("vyatta-test-alpha-v1", "alpha").AddSchemaSnippet(snippet("alphaCont")),
		testutils.NewTestSchema("vyatta-test-gamma-v1", "gamma").AddSchemaSnippet(snippet("gammaCont")),
	}
	dir := createYangDir(t, "a02p3b", schemas)
	defer os.RemoveAll(dir)
	comps := []string{
		conf.CreateTestDotComponentFile("alpha").SetBefore("x.target").AddBaseModel().String(),
		conf.CreateTestDotComponentFile("gamma").SetAfter("x.target").AddBaseModel().String(),
	}
	ms, _ := getModelSet(t, dir)
	mp, err := CreateComponentNSMappings(ms, testModelSet, getComponentConfigs(t, comps...))
	if err != nil {
		t.Fatal(err)
	}
	if got := mp.Prerequisites(compGamma); !reflect.DeepEqual(got, []string{compAlpha}) {
		t.Errorf("gamma: %v", got)
	}
	if got := mp.Prerequisites(compAlpha); len(got) != 0 {
		t.Errorf("alpha: %v", got)
	}
}

// ---- unchanged entry points ----------------------------------------------

func TestLegacyEntryPointsUnchangedByThePendingTable(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compBeta, 1, errors.New("beta: no"))
	if _, errs := h.cm.ComponentSetRunningChecked(h.ms, h.newDN, nil, nil); len(errs) != 1 ||
		!strings.Contains(errs[0].Error(), "NOT rolled back") ||
		strings.Contains(errs[0].Error(), restartPhraseForTest) {
		t.Fatalf("checked entry point changed: %v", errs)
	}
	if len(h.rec.before) != 0 {
		t.Error("the checked entry point must not use the recorder")
	}
}

// The exact phrase of design section 8 (straight quotes and the comma included).
const restartPhraseForTest = "also attempted automatically when configd restarts, after the component is fenced"

// A report that leaves dirty components says that startup also reconciles.
func TestCompensationReportWithDirtyComponentsNamesTheRestartReconcile(t *testing.T) {
	h := newCompHarness(t)
	h.pol.SkipCompensation = true // applied components stay dirty (boot_partial)
	h.fail(compGamma, 1, errors.New("gamma: no"))
	rep, errs := h.run()
	if len(errs) != 2 || len(rep.Dirty) == 0 {
		t.Fatalf("setup: errs=%v dirty=%v", errs, rep.Dirty)
	}
	txt := errs[1].Error()
	for _, need := range []string{restartPhraseForTest, "Running configuration was NOT changed",
		"re-applied by 'reconcile' or the next commit"} {
		if !strings.Contains(txt, need) {
			t.Errorf("report lacks %q: %s", need, txt)
		}
	}
	if strings.Contains(txt, "NOT rolled back") {
		t.Errorf("legacy wording in the new report: %s", txt)
	}
}

// The all-compensated sentence does not carry it.
func TestAllCompensatedReportDoesNotNameTheRestartReconcile(t *testing.T) {
	h := newCompHarness(t)
	h.fail(compGamma, 1, errors.New("gamma: no"))
	_, errs := h.run()
	if len(errs) != 2 || !strings.Contains(errs[1].Error(), "All applied components were reverted") {
		t.Fatalf("setup: %v", errs)
	}
	if strings.Contains(errs[1].Error(), restartPhraseForTest) {
		t.Errorf("the phrase leaked into the all-compensated report: %s", errs[1])
	}
}
